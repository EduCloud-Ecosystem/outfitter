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

	"github.com/EduCloud-Ecosystem/outfitter/internal/broker"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider/docker"
)

func main() {
	if e := execute(); e != nil {
		json.NewEncoder(os.Stdout).Encode(map[string]string{"error": e.Error()})
		os.Exit(2)
	}
}
func execute() error {
	root := flag.NewFlagSet("outfitter", flag.ContinueOnError)
	root.SetOutput(io.Discard)
	state := root.String("state-dir", "", "private local ledger directory")
	if e := root.Parse(os.Args[1:]); e != nil {
		return e
	}
	args := root.Args()
	if *state == "" || len(args) < 1 {
		return errors.New("usage: outfitter --state-dir PRIVATE_PATH COMMAND")
	}
	command := args[0]
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(io.Discard)
	project := f.String("project-id", "", "opaque project identifier")
	budget := f.Int64("budget-cpu-seconds", 0, "finite project budget")
	request := f.String("request", "", "request JSON file")
	id := f.String("request-id", "", "request identifier")
	if e := f.Parse(args[1:]); e != nil {
		return e
	}
	if len(f.Args()) > 0 {
		return errors.New("unexpected arguments")
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
	case "project-init":
		result, e = s.InitProject(*project, *budget)
	case "project-status":
		result, e = s.Usage(*project)
	case "status":
		var r provider.Record
		r, e = s.Record(*id)
		result = r.Receipt
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
