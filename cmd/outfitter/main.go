// Outfitter's reviewed-local CLI is intentionally not a network service.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/assessment"
	"github.com/EduCloud-Ecosystem/outfitter/internal/broker"
	"github.com/EduCloud-Ecosystem/outfitter/internal/consumer"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider/docker"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider/jetstream2"
)

var errReportedPreflight = errors.New("preflight resource checks failed")
var errReportedOwned = errors.New("owned-data reply already emitted")

func main() {
	if e := execute(); e != nil {
		if !errors.Is(e, errReportedPreflight) && !errors.Is(e, errReportedOwned) {
			json.NewEncoder(os.Stdout).Encode(map[string]string{"error": e.Error()})
		}
		os.Exit(2)
	}
}
func execute() error {
	if isOwnedCommand(os.Args[1:]) {
		return executeOwnedCommand(os.Args[1:], os.Stdin, os.Stdout)
	}
	root := flag.NewFlagSet("outfitter", flag.ContinueOnError)
	root.SetOutput(io.Discard)
	state := root.String("state-dir", "", "private local ledger directory")
	if e := root.Parse(os.Args[1:]); e != nil {
		return e
	}
	args := root.Args()
	if len(args) < 1 {
		return errors.New("usage: outfitter --state-dir PRIVATE_PATH COMMAND")
	}
	command := args[0]
	// Refuse the assessment-only authority before request/config reads or any
	// provider construction, including the early Jetstream2 command branches.
	if *state != "" {
		if _, e := os.Lstat(filepath.Join(*state, "ledger.json")); e == nil {
			version, e := ledger.ReadOnlyVersion(*state)
			if e != nil {
				return e
			}
			if version == 6 {
				s, e := ledger.Open(*state, false)
				if e != nil {
					return e
				}
				b := broker.New(s, nil)
				switch command {
				case "reap":
					if len(args) != 1 {
						return ledger.ErrOwnedDataOnly
					}
					result, e := b.OwnedReap()
					if e != nil {
						return e
					}
					return json.NewEncoder(os.Stdout).Encode(result)
				case "watch":
					if len(args) != 3 || args[1] != "--request-id" {
						return ledger.ErrOwnedDataOnly
					}
					return b.Watch(context.Background(), args[2])
				default:
					return ledger.ErrOwnedDataOnly
				}
			}
		}
	}
	if command == "assessment-receipts" {
		if *state != "" || len(args) != 1 {
			return errors.New("INVALID_ASSESSMENT_ARGUMENTS")
		}
		return assessment.Serve(os.Stdin, os.Stdout)
	}
	if command == "jetstream2-reap" {
		return jetstreamReap(*state, args[1:])
	}
	if command == "jetstream2-preflight" {
		return jetstreamPreflight(args[1:])
	}
	if command == "consumer-preflight" {
		return consumerPreflight(args[1:])
	}
	if *state == "" {
		return errors.New("private --state-dir is required")
	}
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(io.Discard)
	project := f.String("project-id", "", "opaque project identifier")
	consumerName := f.String("consumer", "", "supported synthetic consumer")
	job := f.String("job-id", "", "opaque consumer job identifier")
	budget := f.Int64("budget-cpu-seconds", 0, "finite project budget")
	request := f.String("request", "", "request JSON file")
	id := f.String("request-id", "", "request identifier")
	output := f.String("output", "", "new private checkpoint directory")
	checkpoint := f.String("checkpoint", "", "validated private checkpoint directory")
	checkpointSHA := f.String("checkpoint-sha256", "", "separately retained checkpoint digest")
	source := f.String("source-state-dir", "", "current local source authority")
	if e := f.Parse(args[1:]); e != nil {
		return e
	}
	if len(f.Args()) > 0 {
		return errors.New("unexpected arguments")
	}
	if command == "restore" {
		if *checkpoint == "" || *checkpointSHA == "" {
			return errors.New("restore requires --checkpoint and --checkpoint-sha256")
		}
		result, e := ledger.Restore(*state, *checkpoint, *checkpointSHA)
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(result)
	}
	s, e := ledger.Open(*state, command == "project-init")
	if e != nil {
		return e
	}
	b := broker.New(s, docker.New())
	b.StartWatchdog = func(id string) error {
		exe, e := os.Executable()
		if e != nil {
			return e
		}
		cmd := exec.Command(exe, "--state-dir", s.Dir, "watch", "--request-id", id)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		null, e := os.OpenFile(os.DevNull, os.O_RDWR, 0600)
		if e != nil {
			return e
		}
		defer null.Close()
		cmd.Stdin = null
		cmd.Stdout = null
		cmd.Stderr = null
		if e = cmd.Start(); e != nil {
			return e
		}
		defer cmd.Process.Release()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if _, e = os.Stat(filepath.Join(s.Dir, "watch-"+id+".ready")); e == nil {
				return nil
			}
			time.Sleep(20 * time.Millisecond)
		}
		return errors.New("watchdog readiness timed out")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var result any
	switch command {
	case "checkpoint":
		if *output == "" {
			return errors.New("checkpoint requires --output NEW_PRIVATE_DIRECTORY")
		}
		result, e = s.Checkpoint(*output)
	case "recovery-status":
		result, e = s.RecoveryStatus()
	case "activate":
		if *source == "" || *checkpoint == "" || *checkpointSHA == "" {
			return errors.New("activate requires --source-state-dir, --checkpoint and --checkpoint-sha256")
		}
		result, e = s.Activate(*source, *checkpoint, *checkpointSHA)
	case "project-init":
		result, e = s.InitProject(*project, *budget)
	case "project-status":
		result, e = s.Usage(*project)
	case "status":
		result, e = s.Receipt(*id)
	case "consumer-run":
		var req consumer.Request
		req, e = readConsumerRequest(*request)
		if e == nil {
			result, e = consumer.Run(ctx, b, req)
		}
	case "consumer-status":
		result, e = consumer.Status(s, *consumerName, *project, *job)
	case "run":
		file, er := os.Open(*request)
		if er != nil {
			return errors.New("request file unavailable")
		}
		defer file.Close()
		dec := json.NewDecoder(io.LimitReader(file, 65537))
		dec.DisallowUnknownFields()
		var w provider.Request
		if er = dec.Decode(&w); er != nil {
			return fmt.Errorf("invalid request JSON: %w", er)
		}
		if dec.Decode(new(any)) != io.EOF {
			return errors.New("trailing or oversized request JSON")
		}
		result, e = b.Run(ctx, w)
	case "cancel":
		result, e = b.Cancel(*id, "operator requested cancellation")
	case "reconcile":
		result, e = b.Reconcile(*id)
	case "reap":
		result, e = b.Reap()
	case "watch":
		return b.Watch(ctx, *id)
	default:
		return errors.New("unknown command")
	}
	if e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func jetstreamPreflight(args []string) error {
	flags := flag.NewFlagSet("jetstream2-preflight", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	path := flags.String("config", "", "nonsecret pinned provider configuration")
	if e := flags.Parse(args); e != nil {
		return e
	}
	if *path == "" || len(flags.Args()) != 0 {
		return errors.New("jetstream2-preflight requires --config FILE")
	}
	f, e := os.Open(*path)
	if e != nil {
		return errors.New("provider configuration unavailable")
	}
	defer f.Close()
	var config jetstream2.Config
	decoder := json.NewDecoder(io.LimitReader(f, 65537))
	decoder.DisallowUnknownFields()
	if e = decoder.Decode(&config); e != nil {
		return errors.New("invalid provider configuration")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("trailing or oversized provider configuration")
	}
	client, e := jetstream2.New(config, nil, nil)
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	credentials := jetstream2.Credentials{ID: os.Getenv("OS_APPLICATION_CREDENTIAL_ID"), Secret: os.Getenv("OS_APPLICATION_CREDENTIAL_SECRET")}
	if e = client.Authenticate(ctx, credentials); e != nil {
		return e
	}
	result, e := client.Preflight(ctx)
	if e != nil {
		return e
	}
	return emitPreflight(os.Stdout, result)
}

func emitPreflight(w io.Writer, result jetstream2.Preflight) error {
	if e := json.NewEncoder(w).Encode(result); e != nil {
		return e
	}
	if !result.ResourceChecksPassed {
		return errReportedPreflight
	}
	return nil
}

// Cleanup-only entry point. It cannot configure a permit or create a worker.
func jetstreamReap(state string, args []string) error {
	f := flag.NewFlagSet("jetstream2-reap", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	path := f.String("config", "", "pinned provider configuration")
	if e := f.Parse(args); e != nil {
		return e
	}
	if state == "" || *path == "" || len(f.Args()) != 0 {
		return errors.New("jetstream2-reap requires --state-dir and --config")
	}
	file, e := os.Open(*path)
	if e != nil {
		return e
	}
	defer file.Close()
	var cfg jetstream2.Config
	dec := json.NewDecoder(io.LimitReader(file, 65537))
	dec.DisallowUnknownFields()
	if e = dec.Decode(&cfg); e != nil {
		return errors.New("invalid provider configuration")
	}
	if dec.Decode(new(any)) != io.EOF {
		return errors.New("trailing configuration")
	}
	store, e := ledger.Open(state, false)
	if e != nil {
		return e
	}
	journal, e := jetstream2.OpenJournal(store, cfg)
	if e != nil {
		return e
	}
	client, e := jetstream2.New(cfg, nil, journal)
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if e = client.Authenticate(ctx, jetstream2.Credentials{ID: os.Getenv("OS_APPLICATION_CREDENTIAL_ID"), Secret: os.Getenv("OS_APPLICATION_CREDENTIAL_SECRET")}); e != nil {
		return e
	}
	results, e := client.Reap(ctx)
	if e != nil {
		return e
	}
	if e = json.NewEncoder(os.Stdout).Encode(results); e != nil {
		return e
	}
	for _, r := range results {
		if r.Error != "" {
			return errors.New("native cleanup remains unresolved; inspect sweep results")
		}
	}
	return nil
}

func readConsumerRequest(path string) (consumer.Request, error) {
	file, e := os.Open(path)
	if e != nil {
		return consumer.Request{}, errors.New("consumer request file unavailable")
	}
	defer file.Close()
	return consumer.Decode(file)
}
func consumerPreflight(args []string) error {
	f := flag.NewFlagSet("consumer-preflight", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	path := f.String("request", "", "synthetic consumer request JSON")
	if e := f.Parse(args); e != nil {
		return e
	}
	if *path == "" || len(f.Args()) != 0 {
		return errors.New("consumer-preflight requires --request FILE")
	}
	req, e := readConsumerRequest(*path)
	if e != nil {
		return e
	}
	result, e := consumer.Check(req)
	if e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
