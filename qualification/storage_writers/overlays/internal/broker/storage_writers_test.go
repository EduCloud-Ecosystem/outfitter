package broker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
	"github.com/EduCloud-Ecosystem/outfitter/internal/qualification"
)

func bwCheck(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}
func bwRead(t *testing.T, p string) []byte {
	t.Helper()
	b, e := os.ReadFile(p)
	bwCheck(t, e)
	return b
}
func bwEmit(t *testing.T, id, property string, observations map[string]any) {
	t.Helper()
	b, e := json.Marshal(map[string]any{"id": id, "production_property": property, "observations": observations})
	bwCheck(t, e)
	t.Log("WITNESS " + string(b))
}
func bwInventory(t *testing.T, root string) (int, int64) {
	t.Helper()
	entries := 0
	var size int64
	bwCheck(t, filepath.WalkDir(root, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		entries++
		if !d.IsDir() {
			st, e := d.Info()
			if e != nil {
				return e
			}
			size += st.Size()
		}
		return nil
	}))
	return entries, size
}
func bwAwait(t *testing.T, desc string, fn func() bool) {
	t.Helper()
	end := time.Now().Add(10 * time.Second)
	for time.Now().Before(end) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out: " + desc)
}
func bwExists(p string) bool { _, e := os.Stat(p); return e == nil }
func TestStorageWriterSnapshotEntryBoundary(t *testing.T) {
	b, r, f := setup(t, 30)
	initial := bwRead(t, filepath.Join(r.InputDir, "data.csv"))
	for i := 0; i < 1022; i++ {
		bwCheck(t, os.WriteFile(filepath.Join(r.InputDir, fmt.Sprintf("x%04d", i)), nil, 0600))
	}
	out, e := b.Run(context.Background(), r)
	bwCheck(t, e)
	if out.Status != "failed" || !strings.Contains(out.Error, "too many input files") || f.created != 0 {
		t.Fatal(out, f.created)
	}
	entries, size := bwInventory(t, filepath.Join(b.Store.Dir, "snapshots"))
	if entries != 1026 || size != int64(len(initial)) {
		t.Fatal(entries, size)
	}
	if bwExists(filepath.Join(b.Store.Dir, "snapshots", r.RequestID, "recipe")) {
		t.Fatal("1025th walked entry created")
	}
	bwEmit(t, "snapshot-entry-partial", "FAIL", map[string]any{"walked_entries_before_refusal": 1024, "retained_entries_including_snapshot_parent_and_request_dir": entries, "retained_bytes": size, "provider_creates": f.created, "charged_cpu_seconds": out.Compute.Charged})
}
func TestStorageWriterSnapshotGrowth(t *testing.T) {
	b, r, f := setup(t, 30)
	path, pathErr := filepath.EvalSymlinks(filepath.Join(r.InputDir, "data.csv"))
	bwCheck(t, pathErr)
	grew := false
	expected := make([]byte, (16<<20)+1)
	expectedHash := sha256.Sum256(expected)
	qualification.Hook = func(stage, p string) error {
		if stage == "snapshot-after-stat" && p == path {
			grew = true
			return os.WriteFile(p, expected, 0600)
		}
		return nil
	}
	t.Cleanup(func() { qualification.Hook = nil })
	out, e := b.Run(context.Background(), r)
	qualification.Hook = nil
	bwCheck(t, e)
	if !grew || out.Status != "failed" || !strings.Contains(out.Error, "input grew") || f.created != 0 {
		t.Fatal(out, grew, f.created)
	}
	actual := bwRead(t, filepath.Join(b.Store.Dir, "snapshots", r.RequestID, "input", "data.csv"))
	if len(actual) != len(expected) || sha256.Sum256(actual) != expectedHash {
		t.Fatal("partial bytes differ from pre-copy expectation")
	}
	entries, size := bwInventory(t, filepath.Join(b.Store.Dir, "snapshots"))
	if entries != 4 || size != int64(len(expected)) {
		t.Fatal(entries, size)
	}
	bwEmit(t, "snapshot-growth-retains-probe-byte", "FAIL", map[string]any{"injected": "source grows after production stat, before open/copy", "retained_bytes": size, "declared_copy_limit": 16 << 20, "retained_entries": entries, "provider_creates": f.created, "charged_cpu_seconds": out.Compute.Charged})
}
func TestStorageWriterSnapshotCancellationAndCheckpoint(t *testing.T) {
	b, r, f := setup(t, 30)
	expectedInput := bwRead(t, filepath.Join(r.InputDir, "data.csv"))
	expectedRecipe := bwRead(t, filepath.Join(r.RecipeDir, "recipe.py"))
	entered := make(chan struct{})
	release := make(chan struct{})
	once := false
	qualification.Hook = func(stage, p string) error {
		if stage == "snapshot-after-stat" && !once {
			once = true
			close(entered)
			<-release
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	b.StartWatchdog = func(string) error { return errors.New("fixture stops before provider effects") }
	go func() { _, e := b.Run(ctx, r); done <- e }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("copy boundary missing")
	}
	cancel()
	if ctx.Err() == nil {
		t.Fatal("observer has not cancelled")
	}
	select {
	case <-done:
		t.Fatal("writer prematurely completed")
	default:
	}
	checkpoint := filepath.Join(t.TempDir(), "checkpoint")
	report, e := b.Store.Checkpoint(checkpoint)
	bwCheck(t, e)
	if len(report.Unresolved) != 1 {
		t.Fatal("checkpoint lost reservation")
	}
	if bwExists(filepath.Join(b.Store.Dir, "snapshots", r.RequestID, "input", "data.csv")) {
		t.Fatal("copy happened before barrier release")
	}
	close(release)
	select {
	case e := <-done:
		bwCheck(t, e)
	case <-time.After(10 * time.Second):
		t.Fatal("copy did not terminate")
	}
	qualification.Hook = nil
	if !bytes.Equal(expectedInput, bwRead(t, filepath.Join(b.Store.Dir, "snapshots", r.RequestID, "input", "data.csv"))) || !bytes.Equal(expectedRecipe, bwRead(t, filepath.Join(b.Store.Dir, "snapshots", r.RequestID, "recipe", "recipe.py"))) {
		t.Fatal("copy after cancellation differs")
	}
	if f.created != 0 {
		t.Fatal("unexpected fake provider creation")
	}
	bwEmit(t, "snapshot-cancellation-is-not-writer-completion", "FAIL", map[string]any{"checkpoint_completed_while_copy_blocked": true, "copy_wrote_after_context_cancel": true, "checkpoint_unresolved_requests": len(report.Unresolved), "copy_terminal_observed_separately": true, "provider_creates": f.created})
}

// Child helpers execute actual Watch with a fake provider. The test supervisor
// owns and Waits every child. A separate requester process exits; this does not
// simulate a host crash, Docker, or the CLI launcher's ready-file polling.
func TestStorageWriterProcessHelper(t *testing.T) {
	kind := os.Getenv("OUTFITTER_STORAGE_HELPER")
	if kind == "" {
		return
	}
	ctl := os.Getenv("OUTFITTER_STORAGE_CONTROL")
	if ctl == "" {
		t.Fatal("missing disposable control dir")
	}
	if kind == "requester" {
		bwCheck(t, os.WriteFile(filepath.Join(ctl, "started"), []byte("started"), 0600))
		bwAwait(t, "requester stop", func() bool { return bwExists(filepath.Join(ctl, "stop")) })
		return
	}
	if kind != "watch" {
		t.Fatal("unknown helper")
	}
	s, e := ledger.Open(os.Getenv("OUTFITTER_STORAGE_STATE"), false)
	bwCheck(t, e)
	b := New(s, &fake{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if bwExists(filepath.Join(ctl, "stop")) {
					cancel()
					_ = os.WriteFile(filepath.Join(ctl, "cancel-observed"), []byte("cancelled"), 0600)
					return
				}
			}
		}
	}()
	ready := false
	lockObserved := false
	qualification.Hook = func(stage, path string) error {
		if stage == "watch-before-ready" || stage == "watch-after-ready" {
			bwCheck(t, os.WriteFile(filepath.Join(ctl, stage), []byte("entered"), 0600))
			bwAwait(t, "release "+stage, func() bool { return bwExists(filepath.Join(ctl, "release-"+stage)) })
			if stage == "watch-after-ready" {
				ready = true
			}
		}
		if stage == "ledger-before-lock" && ready && !lockObserved {
			lockObserved = true
			bwCheck(t, os.WriteFile(filepath.Join(ctl, "ledger-before-lock"), []byte("entered"), 0600))
		}
		return nil
	}
	e = b.Watch(ctx, "run")
	cancel()
	<-monitorDone
	if e != nil && !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}

type bwChild struct {
	cmd      *exec.Cmd
	ctl      string
	done     chan error
	finished bool
}

func bwStart(t *testing.T, kind, state string) *bwChild {
	t.Helper()
	ctl := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestStorageWriterProcessHelper$", "-test.timeout=20s")
	cmd.Env = append(os.Environ(), "OUTFITTER_STORAGE_HELPER="+kind, "OUTFITTER_STORAGE_CONTROL="+ctl, "OUTFITTER_STORAGE_STATE="+state)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	log, e := os.Create(filepath.Join(ctl, "process.log"))
	bwCheck(t, e)
	cmd.Stdout = log
	cmd.Stderr = log
	bwCheck(t, cmd.Start())
	c := &bwChild{cmd: cmd, ctl: ctl, done: make(chan error, 1)}
	go func() { e := cmd.Wait(); log.Close(); c.done <- e }()
	t.Cleanup(func() {
		if !c.finished {
			_ = cmd.Process.Kill()
			select {
			case <-c.done:
			case <-time.After(5 * time.Second):
				t.Error("child termination unconfirmed")
			}
		}
	})
	return c
}
func (c *bwChild) signal(t *testing.T, name string) {
	t.Helper()
	bwCheck(t, os.WriteFile(filepath.Join(c.ctl, name), []byte("signal"), 0600))
}
func (c *bwChild) await(t *testing.T, name string) {
	t.Helper()
	bwAwait(t, name, func() bool { return bwExists(filepath.Join(c.ctl, name)) })
}
func (c *bwChild) wait(t *testing.T) {
	t.Helper()
	select {
	case e := <-c.done:
		c.finished = true
		if e != nil {
			t.Fatalf("child: %v\n%s", e, bwRead(t, filepath.Join(c.ctl, "process.log")))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("child still running")
	}
}
func bwLock(t *testing.T, root, name string) func() {
	t.Helper()
	f, e := os.OpenFile(filepath.Join(root, name), os.O_CREATE|os.O_RDWR, 0600)
	bwCheck(t, e)
	bwCheck(t, syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))
	return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }
}
func TestStorageWriterWatchReadinessAndBlockedLock(t *testing.T) {
	b, r, _ := setup(t, 30)
	_, _, e := b.Reserve(r)
	bwCheck(t, e)
	ready := filepath.Join(b.Store.Dir, "watch-run.ready")
	bwCheck(t, os.WriteFile(ready, []byte("stale owner"), 0600))
	c := bwStart(t, "watch", b.Store.Dir)
	c.await(t, "watch-before-ready")
	unlockExecution := bwLock(t, b.Store.Dir, "execution.lock")
	defer unlockExecution()
	unlockLedger := bwLock(t, b.Store.Dir, "ledger.lock")
	c.signal(t, "release-watch-before-ready")
	c.await(t, "watch-after-ready")
	if string(bwRead(t, ready)) != "ready\n" {
		t.Fatal("readiness did not overwrite stale identity")
	}
	c.signal(t, "release-watch-after-ready")
	c.await(t, "ledger-before-lock")
	c.signal(t, "stop")
	// The helper confirms context cancellation while production flock is held.
	c.await(t, "cancel-observed")
	select {
	case e := <-c.done:
		c.finished = true
		t.Fatal("Watch escaped held ledger lock", e)
	default:
	}
	if !bwExists(ready) {
		t.Fatal("readiness removed before blocked owner stopped")
	}
	unlockLedger()
	c.wait(t)
	if bwExists(ready) {
		t.Fatal("ready not removed after terminal Watch")
	}
	u, e := b.Store.Usage("project")
	bwCheck(t, e)
	if u.Unknown != 30 {
		t.Fatal(u)
	}
	bwEmit(t, "watch-ready-outside-locks-and-cancel-blocked", "FAIL", map[string]any{"stale_ready_overwritten": true, "ready_written_under_exclusive_execution_and_ledger_locks": true, "cancelled_watch_waited_for_ledger_unlock": true, "process_wait_confirmed": true, "unknown_cpu_seconds": u.Unknown})
}
func TestStorageWriterDuplicateWatchOwners(t *testing.T) {
	b, r, _ := setup(t, 30)
	_, _, e := b.Reserve(r)
	bwCheck(t, e)
	a := bwStart(t, "watch", b.Store.Dir)
	c := bwStart(t, "watch", b.Store.Dir)
	for _, child := range []*bwChild{a, c} {
		child.await(t, "watch-before-ready")
		child.signal(t, "release-watch-before-ready")
		child.await(t, "watch-after-ready")
	}
	ready := filepath.Join(b.Store.Dir, "watch-run.ready")
	if !bwExists(ready) {
		t.Fatal("no ready")
	}
	a.signal(t, "stop")
	a.signal(t, "release-watch-after-ready")
	a.wait(t)
	if bwExists(ready) {
		t.Fatal("expected first owner's removal of shared readiness")
	}
	select {
	case e := <-c.done:
		c.finished = true
		t.Fatal("second owner unexpectedly terminal", e)
	default:
	}
	c.signal(t, "stop")
	c.signal(t, "release-watch-after-ready")
	c.wait(t)
	bwEmit(t, "duplicate-watch-shared-ready", "FAIL", map[string]any{"simultaneous_watch_processes": 2, "one_exit_removed_other_live_owner_ready": true, "both_processes_waited": true})
}
func TestStorageWriterRequesterExitRetainsWatcher(t *testing.T) {
	b, r, _ := setup(t, 30)
	_, _, e := b.Reserve(r)
	bwCheck(t, e)
	requester := bwStart(t, "requester", b.Store.Dir)
	requester.await(t, "started")
	_, e = b.change(r.RequestID, func(x *provider.Record) {
		x.PID = requester.cmd.Process.Pid
		x.CreateAttempted = true
		x.CreateFinished = true
		x.CreateAmbiguous = true
		x.Status = "unknown"
	})
	bwCheck(t, e)
	c := bwStart(t, "watch", b.Store.Dir)
	c.await(t, "watch-before-ready")
	c.signal(t, "release-watch-before-ready")
	c.await(t, "watch-after-ready")
	c.signal(t, "release-watch-after-ready")
	requester.signal(t, "stop")
	requester.wait(t)
	if Alive(requester.cmd.Process.Pid) {
		t.Fatal("requester still alive")
	}
	observed := map[[32]byte]bool{}
	bwAwait(t, "repeated production ledger generations", func() bool {
		data, e := os.ReadFile(filepath.Join(b.Store.Dir, "ledger.json"))
		if e != nil {
			return false
		}
		var d ledger.Data
		if json.Unmarshal(data, &d) != nil || !d.Runs["run"].CancelRequested {
			return false
		}
		observed[sha256.Sum256(data)] = true
		return len(observed) >= 3
	})
	c.signal(t, "stop")
	c.wait(t)
	u, e := b.Store.Usage("project")
	bwCheck(t, e)
	if u.Unknown != 30 || u.Reserved != 30 || u.Charged != 0 || u.Remaining != 0 {
		t.Fatal(u)
	}
	bwEmit(t, "requester-exit-does-not-stop-watch-writes", "FAIL", map[string]any{"requester_exit_waited": true, "watch_ledger_generations_after_requester_exit": len(observed), "unknown_cpu_seconds": u.Unknown, "watch_exit_waited_separately": true, "provider": "fake", "actual_cli_launcher": "NOT_RUN"})
}
func TestStorageWriterSettledCutoverBeforeWatchReady(t *testing.T) {
	b, r, _ := setup(t, 30)
	out, e := b.Run(context.Background(), r)
	bwCheck(t, e)
	if !out.CleanupVerified || out.UnresolvedLiability {
		t.Fatal(out)
	}
	c := bwStart(t, "watch", b.Store.Dir)
	c.await(t, "watch-before-ready")
	cp := filepath.Join(t.TempDir(), "checkpoint")
	report, e := b.Store.Checkpoint(cp)
	bwCheck(t, e)
	dest := filepath.Join(t.TempDir(), "restored")
	_, e = ledger.Restore(dest, cp, report.CheckpointSHA256)
	bwCheck(t, e)
	dst, e := ledger.Open(dest, false)
	bwCheck(t, e)
	_, e = dst.Activate(b.Store.Dir, cp, report.CheckpointSHA256)
	bwCheck(t, e)
	c.signal(t, "release-watch-before-ready")
	c.await(t, "watch-after-ready")
	if !bwExists(filepath.Join(b.Store.Dir, "watch-run.ready")) {
		t.Fatal("delayed ready missing")
	}
	status, e := b.Store.RecoveryStatus()
	bwCheck(t, e)
	if status.Admission != "fenced" {
		t.Fatal(status)
	}
	c.signal(t, "stop")
	c.signal(t, "release-watch-after-ready")
	c.wait(t)
	bwEmit(t, "settled-cutover-does-not-fence-watch-ready", "FAIL", map[string]any{"charged_cpu_seconds": out.Compute.Charged, "source_admission": status.Admission, "watch_ready_written_after_cutover": true, "process_wait_confirmed_separately": true})
}
