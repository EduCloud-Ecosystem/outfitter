package jetstream2

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
)

// The helper environment contains only public fixture inputs and fixture keys.
// It never reads OS_APPLICATION_CREDENTIAL_* or discovers a live endpoint.
func TestOfflineRescueProcessHelper(t *testing.T) {
	role := os.Getenv("OUTFITTER_RESCUE_TEST_ROLE")
	if role == "" {
		return
	}
	var cfg Config
	var p RescuePolicy
	if e := json.Unmarshal([]byte(os.Getenv("OUTFITTER_RESCUE_TEST_CONFIG")), &cfg); e != nil {
		t.Fatal(e)
	}
	if e := json.Unmarshal([]byte(os.Getenv("OUTFITTER_RESCUE_TEST_POLICY")), &p); e != nil {
		t.Fatal(e)
	}
	cert, e := base64.StdEncoding.DecodeString(os.Getenv("OUTFITTER_RESCUE_TEST_CERT"))
	if e != nil {
		t.Fatal(e)
	}
	parsed, e := x509.ParseCertificate(cert)
	if e != nil {
		t.Fatal(e)
	}
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}
	defer transport.CloseIdleConnections()
	crash := func(point string) error {
		if point == os.Getenv("OUTFITTER_RESCUE_TEST_CRASH") {
			_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
			panic("SIGKILL returned")
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if role == "creator" {
		s, e := ledger.Open(os.Getenv("OUTFITTER_RESCUE_TEST_A"), false)
		if e != nil {
			t.Fatal(e)
		}
		j, e := OpenJournal(s, cfg)
		if e != nil {
			t.Fatal(e)
		}
		var owner string
		if e = s.View(func(d ledger.Data) error { owner = d.Owner; return nil }); e != nil {
			t.Fatal(e)
		}
		sender := &RescueSender{Key: fixtureKey("A")}
		sender.Exchange = func(ctx context.Context, raw []byte) ([]byte, error) {
			var scope RescueScope
			if e := rescueVerify(raw, "scope", p.SourceKey, &scope); e != nil {
				return nil, e
			}
			output := filepath.Join(os.Getenv("OUTFITTER_RESCUE_TEST_OUTPUT_DIR"), scope.Phase+"-ack.json")
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOfflineRescueProcessHelper$")
			env := rescueEnvSet(os.Environ(), "OUTFITTER_RESCUE_TEST_ROLE", "accept")
			env = rescueEnvSet(env, "OUTFITTER_RESCUE_TEST_OUTPUT", output)
			env = rescueEnvSet(env, "OUTFITTER_RESCUE_TEST_A", "") // B gets no A directory
			env = rescueEnvSet(env, "OUTFITTER_RESCUE_TEST_CRASH", os.Getenv("OUTFITTER_RESCUE_TEST_B_CRASH"))
			child.Env = env
			child.Stdin = bytes.NewReader(raw)
			if out, e := child.CombinedOutput(); e != nil {
				return nil, fmt.Errorf("fixture B unavailable: %v %s", e, out)
			}
			return os.ReadFile(output)
		}
		c, e := NewOfflineRescueCreator(cfg, transport, j, sender)
		if e != nil {
			t.Fatal(e)
		}
		c.rescueFault = crash
		if e = c.Authenticate(ctx, Credentials{"fixture", "fixture"}); e != nil {
			t.Fatal(e)
		}
		_, e = c.CreateWorker(ctx, WorkerRequest{"request", owner, 4, 100})
		if e != nil {
			t.Fatal(e)
		}
		return
	}
	b, e := NewOfflineRescueObserver(os.Getenv("OUTFITTER_RESCUE_TEST_B"), false, p, cfg, fixtureKey("B"), transport)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	b.store.fault = crash
	var output []byte
	switch role {
	case "accept":
		raw, err := io.ReadAll(io.LimitReader(os.Stdin, rescueMaxBytes+1))
		if err != nil {
			t.Fatal(err)
		}
		output, e = b.Accept(ctx, raw)
	case "sweep":
		if e = b.Authenticate(ctx, Credentials{"fixture", "fixture"}); e != nil {
			t.Fatal(e)
		}
		output, e = b.Sweep(ctx)
	default:
		t.Fatal("unknown fixture role")
	}
	if len(output) > 0 {
		if writeErr := os.WriteFile(os.Getenv("OUTFITTER_RESCUE_TEST_OUTPUT"), output, 0600); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	if e != nil {
		t.Fatal(e)
	}
}
func rescueEnvSet(env []string, key, value string) []string {
	out := []string{}
	for _, item := range env {
		if !strings.HasPrefix(item, key+"=") {
			out = append(out, item)
		}
	}
	return append(out, key+"="+value)
}
func (r *rescueTestRig) processEnv(t *testing.T) []string {
	t.Helper()
	response, e := r.c.http.Get(r.f.api.url)
	if e != nil {
		t.Fatal(e)
	}
	cert := response.TLS.PeerCertificates[0].Raw
	response.Body.Close()
	cfg, _ := json.Marshal(r.c.config)
	p, _ := json.Marshal(r.j.policy.Rescue)
	return []string{"GORACE=atexit_sleep_ms=0", "OUTFITTER_RESCUE_TEST_CONFIG=" + string(cfg), "OUTFITTER_RESCUE_TEST_POLICY=" + string(p), "OUTFITTER_RESCUE_TEST_CERT=" + base64.StdEncoding.EncodeToString(cert), "OUTFITTER_RESCUE_TEST_A=" + r.j.Store.Dir, "OUTFITTER_RESCUE_TEST_B=" + r.b.store.dir, "OUTFITTER_RESCUE_TEST_OUTPUT_DIR=" + t.TempDir()}
}

func TestOfflineRescueProcessCrashBoundaries(t *testing.T) {
	type fault struct {
		name, a, b string
		posts      int
	}
	faults := []fault{}
	for _, phase := range []string{"port", "server"} {
		posts := 0
		if phase == "server" {
			posts = 1
		}
		for _, stage := range []string{"intent-before", "intent-after", "scope-saved", "exchange-returned", "ack-before", "ack-after", "post-before", "post-after"} {
			want := posts
			if stage == "post-after" {
				want++
			}
			faults = append(faults, fault{"A-" + phase + "-" + stage, phase + "-" + stage, "", want})
		}
		for _, file := range []string{phase + ".scope", phase + ".ack"} {
			for _, stage := range []string{"before-write", "after-write", "before-file-sync", "after-file-sync", "before-dir-sync", "after-dir-sync"} {
				point := stage + ":" + file
				faults = append(faults, fault{"B-" + point, "", point, posts})
			}
		}
	}
	for _, fault := range faults {
		t.Run(fault.name, func(t *testing.T) {
			t.Parallel()
			r := setupRescue(t)
			env := r.processEnv(t)
			creator := exec.Command(os.Args[0], "-test.run=^TestOfflineRescueProcessHelper$")
			creator.Env = rescueEnvSet(rescueEnvSet(rescueEnvSet(env, "OUTFITTER_RESCUE_TEST_ROLE", "creator"), "OUTFITTER_RESCUE_TEST_CRASH", fault.a), "OUTFITTER_RESCUE_TEST_B_CRASH", fault.b)
			if out, e := creator.CombinedOutput(); e == nil {
				t.Fatalf("crash not exercised: %s", out)
			}
			posts, _ := r.counts()
			if posts != fault.posts {
				t.Fatalf("creation boundary: got %d want %d", posts, fault.posts)
			}
			record, e := r.j.Lookup(context.Background(), "request")
			if e != nil {
				t.Fatal(e)
			}
			assertHeld(t, r.j, 100)
			// Repeating A must not resume a partially dispatched reservation.
			if _, e = r.c.CreateWorker(context.Background(), WorkerRequest{"request", r.w.Owner, 4, 100}); e != nil {
				t.Fatal(e)
			}
			again, _ := r.counts()
			if again != posts {
				t.Fatal("creator retried after crash")
			}
			if _, e = r.c.TeardownWorker(context.Background(), record); e == nil {
				t.Fatal("A cleanup takeover")
			}
			// Remove A entirely before the separate B process runs. B has only its
			// own files, fixture trust keys and explicit loopback provider transport.
			if e = os.Rename(r.j.Store.Dir, r.j.Store.Dir+"-unavailable"); e != nil {
				t.Fatal(e)
			}
			if _, e = os.Stat(filepath.Join(r.b.store.dir, "port.scope")); os.IsNotExist(e) {
				if posts != 0 {
					t.Fatal("effect without remote scope")
				}
				return
			} else if e != nil {
				t.Fatal(e)
			}
			if delay := time.Until(record.Deadline.Add(rescueClockTolerance + 20*time.Millisecond)); delay > 0 {
				time.Sleep(delay)
			}
			output := filepath.Join(t.TempDir(), "receipt.json")
			state, e := r.b.store.load()
			if e != nil {
				t.Fatal(e)
			}
			unknownServer := state.server != nil && posts == 1
			sweep := exec.Command(os.Args[0], "-test.run=^TestOfflineRescueProcessHelper$")
			sweep.Env = rescueEnvSet(rescueEnvSet(rescueEnvSet(env, "OUTFITTER_RESCUE_TEST_ROLE", "sweep"), "OUTFITTER_RESCUE_TEST_A", ""), "OUTFITTER_RESCUE_TEST_OUTPUT", output)
			if out, e := sweep.CombinedOutput(); (e != nil) != unknownServer {
				t.Fatalf("B restart outcome mismatch: %v %s", e, out)
			}
			raw, e := os.ReadFile(output)
			if e != nil {
				t.Fatal(e)
			}
			receipt := r.receipt(t, raw)
			if unknownServer && receipt.Outcome != "UNKNOWN" {
				t.Fatal("unknown server intent falsely resolved")
			}
			_, deletes := r.counts()
			wantDeletes := posts
			if unknownServer {
				wantDeletes = 0
			}
			if deletes != wantDeletes {
				t.Fatal("owned effects not recovered", posts, deletes)
			}
			before, e := os.ReadFile(filepath.Join(r.j.Store.Dir+"-unavailable", "ledger.json"))
			if e != nil {
				t.Fatal(e)
			}
			// Independent second sweep is observational and idempotent.
			sweep2 := exec.Command(os.Args[0], "-test.run=^TestOfflineRescueProcessHelper$")
			sweep2.Env = sweep.Env
			if out, e := sweep2.CombinedOutput(); (e != nil) != unknownServer {
				t.Fatalf("second B sweep: %v %s", e, out)
			}
			after, e := os.ReadFile(filepath.Join(r.j.Store.Dir+"-unavailable", "ledger.json"))
			if e != nil || !bytes.Equal(before, after) {
				t.Fatal("B changed unavailable accounting")
			}
			_, deletes2 := r.counts()
			if deletes2 != deletes {
				t.Fatal("duplicate cleanup")
			}
		})
	}
}

func TestOfflineRescueHeldAndCorruptA(t *testing.T) {
	r := setupRescue(t)
	record := r.create(t)
	point := filepath.Join(t.TempDir(), "checkpoint")
	snap, e := r.j.Store.Checkpoint(point)
	if e != nil {
		t.Fatal(e)
	}
	dest := filepath.Join(t.TempDir(), "held")
	if _, e = ledger.Restore(dest, point, snap.CheckpointSHA256); e != nil {
		t.Fatal(e)
	}
	s, e := ledger.Open(dest, false)
	if e != nil {
		t.Fatal(e)
	}
	j, e := OpenJournal(s, r.c.config)
	if e != nil {
		t.Fatal(e)
	}
	r.c.journal = j
	if _, e = r.c.TeardownWorker(context.Background(), record); e == nil {
		t.Fatal("held A took cleanup")
	}
	if _, e = r.c.ReconcileExpired(context.Background(), record.Request.RequestID); e == nil {
		t.Fatal("held A reaper took cleanup")
	}
	if e = os.WriteFile(filepath.Join(r.j.Store.Dir, "ledger.json"), []byte("CORRUPT"), 0600); e != nil {
		t.Fatal(e)
	}
	r.future(record)
	if _, e = r.b.Sweep(context.Background()); e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(filepath.Join(r.j.Store.Dir, "ledger.json"))
	if e != nil || string(before) != "CORRUPT" {
		t.Fatal("B repaired ledger")
	}
	assertHeld(t, j, 100)
}
