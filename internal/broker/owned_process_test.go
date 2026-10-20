package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/assessment"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

// This constructor, argv, injected clock/policy, and descriptor launcher exist
// only in the Go test executable. Every operation calls the real broker method.
func testOwnedBoundary(s *ledger.Store, operation string) (*ownedBoundary, error) {
	root, e := os.OpenRoot(os.Getenv("OUTFITTER_OWNED_TEST_ROOT"))
	if e != nil {
		return nil, e
	}
	state, e := os.OpenRoot(s.Dir)
	if e != nil {
		root.Close()
		return nil, e
	}
	f, e := root.Open(".")
	if e != nil {
		return nil, e
	}
	id, e := openedIdentity(f, "directory")
	f.Close()
	if e != nil {
		return nil, e
	}
	x := &ownedBoundary{root: root, stateRoot: state, id: strings.Repeat("b", 32), rootIdentity: id}
	x.clock = func() (ownedClock, error) {
		var n ownedClock
		raw, e := os.ReadFile(os.Getenv("OUTFITTER_OWNED_TEST_CLOCK"))
		if e != nil {
			return n, e
		}
		e = json.Unmarshal(raw, &n)
		return n, e
	}
	x.policy = func(project, owner string) (ownedPolicy, error) {
		if !owneddata.ID(project) || owner != strings.Repeat("7", 32) {
			return ownedPolicy{}, owneddata.ErrDenied
		}
		cap := time.Date(2026, 10, 19, 13, 0, 0, 0, time.UTC)
		return ownedPolicy{MaxUploadBytes: 1024, ProjectEnd: cap, AuthorizationExpiry: cap.Add(time.Hour), PolicyExpiry: cap.Add(2 * time.Hour)}, nil
	}
	pipe := func(fd int) *os.File {
		if syscall.SetNonblock(fd, true) != nil {
			return nil
		}
		syscall.CloseOnExec(fd)
		return os.NewFile(uintptr(fd), "private-test-pipe")
	}
	switch operation {
	case "prepare":
		x.ownerRead, x.watchRead, x.watchWrite = pipe(3), pipe(4), pipe(5)
	case "ingest", "seal":
		x.ownerRead, x.watchRead = pipe(3), pipe(4)
	case "watch":
		x.ownerRead, x.watchWrite, x.readyWrite = pipe(3), pipe(4), pipe(5)
	}
	point := os.Getenv("OUTFITTER_OWNED_TEST_FAULT")
	x.fault = func(got string) error {
		if got == os.Getenv("OUTFITTER_OWNED_TEST_CRASH") {
			os.Exit(86)
		}
		transition := os.Getenv("OUTFITTER_OWNED_TEST_TRANSITION")
		parts := strings.SplitN(transition, ":", 2)
		if len(parts) == 2 && got == "before-journal:"+parts[1] {
			switch parts[0] {
			case "expiry":
				n, e := x.clock()
				if e != nil {
					return e
				}
				n.Wall = n.Wall.Add(2 * time.Hour)
				n.TickMS += 7200000
				raw, _ := json.Marshal(n)
				if e = os.WriteFile(os.Getenv("OUTFITTER_OWNED_TEST_CLOCK"), raw, 0600); e != nil {
					return e
				}
			case "owner":
				_ = x.ownerRead.Close()
			case "watch":
				if operation == "watch" {
					_ = x.watchWrite.Close()
				} else {
					_ = x.watchRead.Close()
				}
			}
		}
		for _, p := range strings.Split(point, ",") {
			if got == p {
				return errors.New("synthetic-raw-error-must-not-escape")
			}
		}
		return nil
	}
	x.spawnWatch = func(scope owneddata.Scope, owner, watch *os.File) (io.ReadCloser, error) {
		read, write, e := os.Pipe()
		if e != nil {
			return nil, e
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestOwnedPeer$", "--", "watch", scope.ScopeID)
		cmd.Env = os.Environ()
		cmd.ExtraFiles = []*os.File{owner, watch, write}
		null, e := os.OpenFile(os.DevNull, os.O_RDWR, 0600)
		if e != nil {
			return nil, e
		}
		cmd.Stdin, cmd.Stdout, cmd.Stderr = null, null, null
		e = cmd.Start()
		write.Close()
		null.Close()
		if e != nil {
			read.Close()
			return nil, e
		}
		if path := os.Getenv("OUTFITTER_OWNED_TEST_WATCH_PID"); path != "" {
			_ = os.WriteFile(path, []byte(strconv.Itoa(cmd.Process.Pid)), 0600)
		}
		_ = cmd.Process.Release()
		return read, nil
	}
	return x, nil
}

func TestOwnedPeer(t *testing.T) {
	if os.Getenv("OUTFITTER_OWNED_TEST_HELPER") != "1" {
		return
	}
	args := os.Args
	for len(args) > 0 && args[0] != "--" {
		args = args[1:]
	}
	if len(args) < 2 {
		os.Exit(2)
	}
	args = args[1:]
	operation := args[0]
	emit := func(value any, e error) {
		code := 0
		if e != nil {
			code = 2
			value = owneddata.ErrorReply{Schema: owneddata.ErrorSchema, Code: ownedError(e).Error()}
		}
		raw, err := owneddata.CanonicalReply(value)
		if err != nil {
			os.Exit(3)
		}
		_, _ = os.Stdout.Write(raw)
		os.Exit(code)
	}
	s, e := ledger.Open(os.Getenv("OUTFITTER_OWNED_TEST_STATE"), false)
	if e != nil {
		emit(nil, e)
	}
	x, e := testOwnedBoundary(s, operation)
	if e != nil {
		emit(nil, e)
	}
	b := New(s, nil)
	b.owned = x
	if operation == "watch" {
		if len(args) != 2 {
			os.Exit(2)
		}
		e = b.Watch(context.Background(), args[1])
		if path := os.Getenv("OUTFITTER_OWNED_TEST_WATCH_EXIT"); path != "" {
			code := "0"
			if e != nil {
				code = "2"
			}
			_ = os.WriteFile(path, []byte(code), 0600)
		}
		if e != nil {
			os.Exit(2)
		}
		os.Exit(0)
	}
	if operation == "reap" {
		_, e = b.OwnedReap()
		if e != nil {
			emit(nil, e)
		}
		os.Exit(0)
	}
	var c owneddata.Control
	var body io.Reader
	if operation == "ingest" {
		c, body, e = assessment.ReadOwnedHeader(operation, os.Stdin)
	} else {
		c, e = assessment.ReadOwnedControl(operation, os.Stdin)
	}
	if e != nil {
		emit(nil, e)
	}
	status, e := b.OwnedData(context.Background(), c, body)
	emit(status, e)
}

var ownedSeedOnce sync.Once
var ownedSeedBinary string
var ownedSeedError error

func TestMain(m *testing.M) {
	code := m.Run()
	if ownedSeedBinary != "" {
		_ = os.RemoveAll(filepath.Dir(ownedSeedBinary))
	}
	os.Exit(code)
}

type ownedProcessFixture struct {
	t                                            *testing.T
	state, root, clock, pid, exit                string
	ownerRead, ownerWrite, watchRead, watchWrite *os.File
	before                                       []byte
	prepare                                      map[string]any
}

func newOwnedProcessFixture(t *testing.T) *ownedProcessFixture {
	t.Helper()
	ownedSeedOnce.Do(func() {
		dir, e := os.MkdirTemp("", "outfitter-owned-seed-")
		if e != nil {
			ownedSeedError = e
			return
		}
		ownedSeedBinary = filepath.Join(dir, "ledger.test")
		cmd := exec.Command("go", "test", "-p", "1", "-c", "-o", ownedSeedBinary, "../ledger")
		cmd.Env = append(os.Environ(), "GOMAXPROCS=2")
		if raw, e := cmd.CombinedOutput(); e != nil {
			ownedSeedError = fmt.Errorf("seed compile: %w: %s", e, raw)
		}
	})
	if ownedSeedError != nil {
		t.Fatal(ownedSeedError)
	}
	dir, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	f := &ownedProcessFixture{t: t, state: filepath.Join(dir, "state"), root: filepath.Join(dir, "content"), clock: filepath.Join(dir, "clock.json"), pid: filepath.Join(dir, "watch.pid"), exit: filepath.Join(dir, "watch.exit")}
	if e = os.Mkdir(f.root, 0700); e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command(ownedSeedBinary, "-test.run=^TestOwnedFixtureSeedHelper$")
	cmd.Env = append(os.Environ(), "OUTFITTER_OWNED_TEST_SEED="+f.state)
	if raw, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("seed: %v %s", e, raw)
	}
	f.setClock(ownedClock{Wall: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC), TickMS: 1000, Epoch: strings.Repeat("a", 32)})
	f.ownerRead, f.ownerWrite, e = os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	f.watchRead, f.watchWrite, e = os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile("testdata/owned-prepare.json")
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(raw, &f.prepare); e != nil {
		t.Fatal(e)
	}
	f.before = f.accounting()
	t.Cleanup(func() {
		if f.ownerWrite != nil {
			f.ownerWrite.Close()
		}
		if f.watchWrite != nil {
			f.watchWrite.Close()
		}
		if _, e := os.Stat(f.pid); e == nil {
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				if _, e := os.Stat(f.exit); e == nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if _, e := os.Stat(f.exit); e != nil {
				raw, _ := os.ReadFile(f.pid)
				pid, _ := strconv.Atoi(string(raw))
				if pid > 0 {
					_ = syscall.Kill(pid, syscall.SIGKILL)
				}
				t.Error("test watcher did not exit after owner close")
			}
		}
		f.ownerRead.Close()
		f.watchRead.Close()
		if !bytes.Equal(f.before, f.accounting()) {
			t.Error("owned lifecycle changed existing accounting")
		}
	})
	return f
}
func (f *ownedProcessFixture) setClock(n ownedClock) {
	f.t.Helper()
	raw, _ := json.Marshal(n)
	tmp := f.clock + ".next"
	if e := os.WriteFile(tmp, raw, 0600); e != nil {
		f.t.Fatal(e)
	}
	if e := os.Rename(tmp, f.clock); e != nil {
		f.t.Fatal(e)
	}
}
func (f *ownedProcessFixture) accounting() []byte {
	f.t.Helper()
	raw, e := os.ReadFile(filepath.Join(f.state, "ledger.json"))
	if e != nil {
		f.t.Fatal(e)
	}
	var d map[string]any
	if json.Unmarshal(raw, &d) != nil {
		f.t.Fatal("invalid journal")
	}
	delete(d, "owned_data_scopes")
	raw, _ = json.Marshal(d)
	return raw
}
func (f *ownedProcessFixture) scope() owneddata.Scope {
	f.t.Helper()
	s, e := ledger.Open(f.state, false)
	if e != nil {
		f.t.Fatal(e)
	}
	var result owneddata.Scope
	e = s.View(func(d ledger.Data) error { result = (*d.OwnedDataScopes)[strings.Repeat("4", 32)]; return nil })
	if e != nil {
		f.t.Fatal(e)
	}
	return result
}
func (f *ownedProcessFixture) control(op string, status owneddata.Status) map[string]any {
	c := map[string]any{"schema": owneddata.ControlSchema, "operation": op, "scope_id": strings.Repeat("4", 32), "waypoint_authority_id": strings.Repeat("6", 32), "expected_broker_authority_id": strings.Repeat("5", 32)}
	if op == "ingest" || op == "seal" {
		c["expected_generation"] = status.Generation
		c["writer_epoch"] = status.WriterEpoch
		c["admitting_owner_epoch"] = status.AdmittingOwnerEpoch
	}
	if op == "cancel" {
		c["reason"] = "cancelled"
	}
	return c
}
func (f *ownedProcessFixture) command(op, fault string) *exec.Cmd {
	cmd := exec.Command(os.Args[0], "-test.run=^TestOwnedPeer$", "--", op)
	cmd.Env = append(os.Environ(), "OUTFITTER_OWNED_TEST_HELPER=1", "OUTFITTER_OWNED_TEST_STATE="+f.state, "OUTFITTER_OWNED_TEST_ROOT="+f.root, "OUTFITTER_OWNED_TEST_CLOCK="+f.clock, "OUTFITTER_OWNED_TEST_WATCH_PID="+f.pid, "OUTFITTER_OWNED_TEST_WATCH_EXIT="+f.exit, "OUTFITTER_OWNED_TEST_FAULT="+fault)
	switch op {
	case "prepare":
		cmd.ExtraFiles = []*os.File{f.ownerRead, f.watchRead, f.watchWrite}
	case "ingest", "seal":
		cmd.ExtraFiles = []*os.File{f.ownerRead, f.watchRead}
	}
	return cmd
}
func (f *ownedProcessFixture) invoke(op string, c map[string]any, body []byte, fault string) (owneddata.Status, owneddata.ErrorReply) {
	f.t.Helper()
	raw, _ := json.Marshal(c)
	raw = append(raw, '\n')
	raw = append(raw, body...)
	cmd := f.command(op, fault)
	cmd.Stdin = bytes.NewReader(raw)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if e := cmd.Start(); e != nil {
		f.t.Fatal(e)
	}
	if op == "prepare" && f.watchWrite != nil {
		f.watchWrite.Close()
		f.watchWrite = nil
	}
	e := cmd.Wait()
	if stderr.Len() != 0 {
		f.t.Fatalf("peer stderr: %s", stderr.Bytes())
	}
	var status owneddata.Status
	var failure owneddata.ErrorReply
	if e == nil {
		if owneddata.ClosedJSON(bytes.TrimSuffix(stdout.Bytes(), []byte{'\n'}), &status, owneddata.MaxReplyBytes) != nil || status.Validate() != nil {
			f.t.Fatalf("invalid status %s", stdout.Bytes())
		}
		canonical, _ := owneddata.CanonicalReply(status)
		if !bytes.Equal(canonical, stdout.Bytes()) {
			f.t.Fatal("noncanonical status")
		}
	} else {
		if cmd.ProcessState.ExitCode() != 2 || owneddata.ClosedJSON(bytes.TrimSuffix(stdout.Bytes(), []byte{'\n'}), &failure, owneddata.MaxReplyBytes) != nil {
			f.t.Fatalf("invalid failure %v %s", e, stdout.Bytes())
		}
	}
	return status, failure
}
func (f *ownedProcessFixture) ready() owneddata.Status {
	f.t.Helper()
	s, e := f.invoke("prepare", f.prepare, nil, "")
	if e.Code != "" {
		f.t.Fatalf("prepare: %s", e.Code)
	}
	if s.Generation != 4 || s.TestPermission != "synthetic_ingest_only" || s.Liveness != "observed_current" {
		f.t.Fatal(s)
	}
	return s
}
func (f *ownedProcessFixture) waitScope(predicate func(owneddata.Scope) bool) owneddata.Scope {
	f.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s := f.scope()
		if predicate(s) {
			return s
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.t.Fatal("scope transition timed out")
	return owneddata.Scope{}
}

func TestOwnedSeparateProcessesCompleteSealCancel(t *testing.T) {
	f := newOwnedProcessFixture(t)
	ready := f.ready()
	status, failure := f.invoke("status", f.control("status", ready), nil, "")
	if failure.Code != "" || status.TestPermission != "none" {
		t.Fatal(status, failure)
	}
	body := []byte("RAW_OWNED_PAYLOAD_SENTINEL")
	ingest := f.control("ingest", ready)
	ingest["content_length"] = len(body)
	complete, failure := f.invoke("ingest", ingest, body, "")
	if failure.Code != "" {
		t.Fatal(failure)
	}
	if !f.scope().UploadComplete {
		t.Fatal("upload not complete")
	}
	sealed, failure := f.invoke("seal", f.control("seal", complete), nil, "")
	if failure.Code != "" || sealed.State != "sealed" {
		t.Fatal(sealed, failure)
	}
	raw, e := os.ReadFile(filepath.Join(f.root, ready.ScopeID, "payload"))
	if e != nil || !bytes.Equal(raw, body) {
		t.Fatal("content mismatch", e)
	}
	_ = filepath.Walk(f.state, func(path string, info os.FileInfo, e error) error {
		if e == nil && !info.IsDir() {
			raw, _ := os.ReadFile(path)
			if bytes.Contains(raw, body) || bytes.Contains(raw, []byte("synthetic-raw-error")) {
				t.Error("raw content/error retained in metadata")
			}
		}
		return nil
	})
	cancelled, failure := f.invoke("cancel", f.control("cancel", sealed), nil, "")
	if failure.Code != "" || cancelled.State != "cleanup_unresolved" || cancelled.CleanupTrusted {
		t.Fatal(cancelled, failure)
	}
	if _, e := os.Stat(filepath.Join(f.root, ready.ScopeID)); !os.IsNotExist(e) {
		t.Fatal("owned content remains", e)
	}
	if _, e := os.Stat(filepath.Join(f.state, leaseName(ready.ScopeID, "writer"))); e != nil {
		t.Fatal("writer tombstone removed", e)
	}
}

func TestOwnedSeparateProcessesShortOverlongAndClaimACKLoss(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   string
		length int
		fault  string
	}{{"short", "ab", 3, ""}, {"overlong", "abcd", 3, ""}, {"claim-ack", "abc", 3, "after-journal:writer-claim"}, {"content-ack", "abc", 3, "after-journal:content-identity"}, {"completion-ack", "abc", 3, "after-journal:upload-complete"}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOwnedProcessFixture(t)
			ready := f.ready()
			c := f.control("ingest", ready)
			c["content_length"] = tc.length
			_, failure := f.invoke("ingest", c, []byte(tc.body), tc.fault)
			if failure.Code == "" {
				t.Fatal("failed stream accepted")
			}
			s := f.waitScope(func(s owneddata.Scope) bool { return s.DenyLatched })
			if !s.WriterStarted {
				t.Fatal("writer claim lost")
			}
			_, failure = f.invoke("ingest", c, []byte(tc.body), "")
			if failure.Code == "" {
				t.Fatal("writer replay accepted")
			}
			if tc.name == "content-ack" {
				info, e := os.Stat(filepath.Join(f.root, s.ScopeID, "payload"))
				if e != nil || info.Size() != 0 {
					t.Fatal("identity ACK uncertainty read data or removed uncertain orphan", e)
				}
			}
		})
	}
}

func TestOwnedSeparateProcessesOwnerAndWatcherLoss(t *testing.T) {
	for _, loss := range []string{"owner", "watcher"} {
		t.Run(loss, func(t *testing.T) {
			f := newOwnedProcessFixture(t)
			ready := f.ready()
			if loss == "owner" {
				f.ownerWrite.Close()
				f.ownerWrite = nil
			} else {
				raw, e := os.ReadFile(f.pid)
				if e != nil {
					t.Fatal(e)
				}
				pid, _ := strconv.Atoi(string(raw))
				if e = syscall.Kill(pid, syscall.SIGKILL); e != nil {
					t.Fatal(e)
				}
				_ = os.WriteFile(f.exit, []byte("killed-by-test"), 0600)
			}
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				if loss == "owner" || livePipe(f.watchRead, f.scope().FilesystemIdentity.WatchRead) == "lost" {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			c := f.control("ingest", ready)
			c["content_length"] = 3
			_, failure := f.invoke("ingest", c, []byte("abc"), "")
			if failure.Code == "" {
				t.Fatal("dead lifeline accepted")
			}
			if f.scope().WriterStarted {
				t.Fatal("dead lifeline consumed writer claim")
			}
		})
	}
}

func TestOwnedSeparateProcessesBlockedWriterCancelAndRace(t *testing.T) {
	f := newOwnedProcessFixture(t)
	ready := f.ready()
	c := f.control("ingest", ready)
	c["content_length"] = 3
	cmd := f.command("ingest", "")
	input, e := cmd.StdinPipe()
	if e != nil {
		t.Fatal(e)
	}
	var output, stderr bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &stderr
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		input.Close()
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	header, _ := json.Marshal(c)
	_, _ = input.Write(append(header, '\n'))
	f.waitScope(func(s owneddata.Scope) bool { return s.WriterStarted && s.FilesystemIdentity.Content != nil })
	_, failure := f.invoke("ingest", c, []byte("abc"), "")
	if failure.Code == "" {
		t.Fatal("second writer accepted")
	}
	denied, failure := f.invoke("cancel", f.control("cancel", ready), nil, "")
	if failure.Code != "" || !denied.DenyLatched {
		t.Fatal(denied, failure)
	}
	for _, o := range denied.Observations {
		if o.OwnedRemoval == "observed_absent" {
			t.Fatal("blocked writer reported drained")
		}
	}
	if _, e = os.Stat(filepath.Join(f.root, ready.ScopeID, "payload")); e != nil {
		t.Fatal("payload removed while writer held", e)
	}
	_, _ = input.Write([]byte("abc"))
	input.Close()
	if e = cmd.Wait(); e == nil {
		t.Fatal("cancelled writer succeeded")
	}
	if stderr.Len() != 0 {
		t.Fatal(stderr.String())
	}
	f.waitScope(func(s owneddata.Scope) bool {
		for _, o := range s.Observations {
			if o.OwnedRemoval == "observed_absent" {
				return true
			}
		}
		return false
	})
}

func TestOwnedSeparateProcessesRejectBeforeBody(t *testing.T) {
	for _, change := range []string{"broker", "waypoint", "owner", "writer", "generation", "ceiling", "scope", "boolean", "extra"} {
		t.Run(change, func(t *testing.T) {
			f := newOwnedProcessFixture(t)
			ready := f.ready()
			c := f.control("ingest", ready)
			c["content_length"] = 3
			switch change {
			case "broker":
				c["expected_broker_authority_id"] = strings.Repeat("d", 32)
			case "waypoint":
				c["waypoint_authority_id"] = strings.Repeat("d", 32)
			case "owner":
				c["admitting_owner_epoch"] = strings.Repeat("d", 32)
			case "writer":
				c["writer_epoch"] = strings.Repeat("d", 32)
			case "generation":
				c["expected_generation"] = 999
			case "ceiling":
				c["content_length"] = 1025
			case "scope":
				c["scope_id"] = strings.Repeat("d", 32)
			case "boolean":
				c["content_length"] = true
			case "extra":
				c["path"] = "MUST_NOT_BE_READ"
			}
			cmd := f.command("ingest", "")
			input, e := cmd.StdinPipe()
			if e != nil {
				t.Fatal(e)
			}
			defer input.Close()
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			if e = cmd.Start(); e != nil {
				t.Fatal(e)
			}
			header, _ := json.Marshal(c)
			_, _ = input.Write(append(header, '\n')) // Keep stdin open, without any body or EOF.
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			select {
			case e = <-done:
				if e == nil || cmd.ProcessState.ExitCode() != 2 {
					t.Fatal("invalid header/runtime binding accepted")
				}
			case <-time.After(time.Second):
				_ = cmd.Process.Kill()
				<-done
				t.Fatal("refusal waited for a body byte or EOF")
			}
			if stderr.Len() != 0 || bytes.Contains(stdout.Bytes(), []byte("MUST_NOT_BE_READ")) {
				t.Fatal("raw diagnostic escaped")
			}
			if f.scope().WriterStarted {
				t.Fatal("invalid command claimed writer")
			}
		})
	}
}

func TestOwnedSeparateProcessesUnknownProjectAndWrongPair(t *testing.T) {
	for _, change := range []string{"unknown-project", "wrong-watch-pair"} {
		t.Run(change, func(t *testing.T) {
			f := newOwnedProcessFixture(t)
			if change == "unknown-project" {
				f.prepare["project_id"] = strings.Repeat("d", 32)
				f.prepare["manifest"].(map[string]any)["project_id"] = strings.Repeat("d", 32)
			} else {
				f.watchRead.Close()
				r, w, e := os.Pipe()
				if e != nil {
					t.Fatal(e)
				}
				defer w.Close()
				f.watchRead = r
			}
			_, failure := f.invoke("prepare", f.prepare, nil, "")
			if failure.Code == "" {
				t.Fatal("invalid prepare accepted")
			}
			entries, e := os.ReadDir(f.root)
			if e != nil || len(entries) != 0 {
				t.Fatal("invalid prepare created scope", e)
			}
			if f.scope().WriterStarted {
				t.Fatal("invalid prepare permitted writer")
			}
			if _, e := os.Stat(f.pid); !os.IsNotExist(e) {
				t.Fatal("invalid prepare spawned watcher")
			}
		})
	}
}

func TestOwnedSeparateProcessesAlivePipeSubstitution(t *testing.T) {
	for _, endpoint := range []string{"owner", "watch"} {
		t.Run(endpoint, func(t *testing.T) {
			f := newOwnedProcessFixture(t)
			ready := f.ready()
			r, w, e := os.Pipe()
			if e != nil {
				t.Fatal(e)
			}
			defer w.Close()
			if endpoint == "owner" {
				f.ownerRead.Close()
				f.ownerRead = r
			} else {
				f.watchRead.Close()
				f.watchRead = r
			}
			c := f.control("ingest", ready)
			c["content_length"] = 3
			_, failure := f.invoke("ingest", c, []byte("abc"), "")
			if failure.Code == "" || f.scope().WriterStarted {
				t.Fatal("unrelated live pipe authorized writer")
			}
		})
	}
}

func TestOwnedSeparateProcessesClockFences(t *testing.T) {
	for _, change := range []string{"wall-backward", "tick-backward", "epoch", "wall-expiry", "tick-expiry", "later-command"} {
		t.Run(change, func(t *testing.T) {
			f := newOwnedProcessFixture(t)
			ready := f.ready()
			original := f.scope()
			n := ownedClock{Wall: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC), TickMS: 1000, Epoch: strings.Repeat("a", 32)}
			switch change {
			case "wall-backward":
				n.Wall = n.Wall.Add(-time.Microsecond)
			case "tick-backward":
				n.TickMS--
			case "epoch":
				n.Epoch = strings.Repeat("c", 32)
			case "wall-expiry":
				n.Wall = n.Wall.Add(time.Hour)
			case "tick-expiry":
				n.TickMS = original.DeadlineTickMS
			case "later-command":
				n.Wall = n.Wall.Add(30 * time.Minute)
				n.TickMS += 1800000
			}
			f.setClock(n)
			c := f.control("ingest", ready)
			c["content_length"] = 3
			status, failure := f.invoke("ingest", c, []byte("abc"), "")
			if change == "later-command" {
				if failure.Code != "" {
					t.Fatal(failure)
				}
				if status.State != "accepting" {
					t.Fatal(status)
				}
			} else if failure.Code == "" {
				t.Fatal("invalid clock authorized content")
			}
			if f.scope().DeadlineTickMS != original.DeadlineTickMS || f.scope().ExpiresAt != original.ExpiresAt {
				t.Fatal("deadline rebased")
			}
		})
	}
}

func TestOwnedSeparateProcessesForeignObjectsHeld(t *testing.T) {
	for _, change := range []string{"root", "scope", "marker", "content", "writer-lease", "watch-lease", "symlink", "fifo", "foreign-entry"} {
		t.Run(change, func(t *testing.T) {
			f := newOwnedProcessFixture(t)
			ready := f.ready()
			c := f.control("ingest", ready)
			c["content_length"] = 3
			complete, failure := f.invoke("ingest", c, []byte("abc"), "")
			if failure.Code != "" {
				t.Fatal(failure)
			}
			scope := filepath.Join(f.root, ready.ScopeID)
			payload := filepath.Join(scope, "payload")
			foreign := ""
			switch change {
			case "root":
				if e := os.Rename(f.root, f.root+"-old"); e != nil {
					t.Fatal(e)
				}
				if e := os.Mkdir(f.root, 0700); e != nil {
					t.Fatal(e)
				}
				foreign = filepath.Join(f.root, "foreign")
			case "scope":
				if e := os.Rename(scope, scope+"-old"); e != nil {
					t.Fatal(e)
				}
				if e := os.Mkdir(scope, 0700); e != nil {
					t.Fatal(e)
				}
				foreign = filepath.Join(scope, "foreign")
			case "marker":
				foreign = filepath.Join(scope, "scope-owner.json")
			case "content":
				if e := os.Rename(payload, filepath.Join(filepath.Dir(f.root), "original-payload")); e != nil {
					t.Fatal(e)
				}
				foreign = payload
			case "writer-lease", "watch-lease":
				kind := strings.TrimSuffix(change, "-lease")
				path := filepath.Join(f.state, leaseName(ready.ScopeID, kind))
				if e := os.Rename(path, path+"-old"); e != nil {
					t.Fatal(e)
				}
				foreign = path
			case "symlink":
				if e := os.Remove(payload); e != nil {
					t.Fatal(e)
				}
				foreign = filepath.Join(filepath.Dir(f.root), "foreign-target")
				if e := os.Symlink(foreign, payload); e != nil {
					t.Fatal(e)
				}
			case "fifo":
				if e := os.Remove(payload); e != nil {
					t.Fatal(e)
				}
				if e := syscall.Mkfifo(payload, 0600); e != nil {
					t.Fatal(e)
				}
			case "foreign-entry":
				foreign = filepath.Join(scope, "foreign")
			}
			if foreign != "" {
				if e := os.WriteFile(foreign, []byte("FOREIGN_SENTINEL"), 0600); e != nil {
					t.Fatal(e)
				}
			}
			status, failure := f.invoke("cancel", f.control("cancel", complete), nil, "")
			if failure.Code != "" {
				t.Fatal(failure)
			}
			for _, o := range status.Observations {
				if o.OwnedRemoval == "observed_absent" {
					t.Fatal("foreign/replaced object reported removed")
				}
			}
			if foreign != "" {
				raw, e := os.ReadFile(foreign)
				if e != nil || string(raw) != "FOREIGN_SENTINEL" {
					t.Fatal("foreign data modified", e)
				}
			}
		})
	}
}

func TestOwnedSeparateProcessesCommitTimeFences(t *testing.T) {
	for _, point := range []string{"writer-claim", "upload-complete", "seal"} {
		for _, event := range []string{"expiry", "owner", "watch"} {
			t.Run(point+"/"+event, func(t *testing.T) {
				f := newOwnedProcessFixture(t)
				ready := f.ready()
				op := "ingest"
				body := []byte("abc")
				c := f.control(op, ready)
				c["content_length"] = 3
				if point == "seal" {
					complete, failure := f.invoke("ingest", c, body, "")
					if failure.Code != "" {
						t.Fatal(failure)
					}
					op = "seal"
					body = nil
					c = f.control(op, complete)
				}
				t.Setenv("OUTFITTER_OWNED_TEST_TRANSITION", event+":"+point)
				_, failure := f.invoke(op, c, body, "")
				if failure.Code == "" {
					t.Error("transition succeeded after commit-time expiry or lifeline loss")
				}
				s := f.scope()
				if !s.DenyLatched {
					t.Error("transition failed to latch denial")
				}
				if point == "writer-claim" && s.WriterStarted {
					t.Error("stale sampled liveness consumed writer claim")
				}
				if point == "upload-complete" && s.UploadComplete {
					t.Error("stale sampled liveness completed upload")
				}
				if point == "seal" && s.State == "sealed" {
					t.Error("stale sampled liveness sealed upload")
				}
			})
		}
	}
}

func TestOwnedSeparateProcessesCloseUncertainty(t *testing.T) {
	for _, tc := range []struct{ name, body, fault string }{
		{"main", "abc", "content-close:main"},
		{"main-uncertainty-before-commit", "abc", "content-close:main,before-journal:close-uncertain"},
		{"main-uncertainty-after-commit", "abc", "content-close:main,after-journal:close-uncertain"},
		{"deferred", "ab", "content-close:deferred"},
		{"uncertainty-before-commit", "ab", "content-close:deferred,before-journal:close-uncertain"},
		{"uncertainty-after-commit", "ab", "content-close:deferred,after-journal:close-uncertain"},
		{"confirmation-before-commit", "abc", "before-journal:close-confirmed"},
		{"confirmation-after-commit", "abc", "after-journal:close-confirmed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOwnedProcessFixture(t)
			ready := f.ready()
			c := f.control("ingest", ready)
			c["content_length"] = 3
			_, failure := f.invoke("ingest", c, []byte(tc.body), tc.fault)
			if failure.Code == "" {
				t.Error("uncertain close/publication accepted upload")
			}
			status, failure := f.invoke("reconcile", f.control("reconcile", ready), nil, "")
			if failure.Code != "" {
				t.Fatal(failure)
			}
			for _, o := range status.Observations {
				if o.WritersDrained == "observed" || o.ScopeAccessRevocation == "observed" || o.OwnedRemoval == "observed_absent" {
					t.Error("lease freedom promoted unconfirmed close into drain/access/removal evidence")
				}
			}
			if _, e := os.Stat(filepath.Join(f.root, ready.ScopeID, "payload")); e != nil {
				t.Error("unconfirmed content was removed", e)
			}
		})
	}
}

func TestOwnedSeparateProcessesWatchReadyCommitFences(t *testing.T) {
	for _, event := range []string{"expiry", "owner", "watch"} {
		t.Run(event, func(t *testing.T) {
			f := newOwnedProcessFixture(t)
			t.Setenv("OUTFITTER_OWNED_TEST_TRANSITION", event+":watch-ready")
			_, failure := f.invoke("prepare", f.prepare, nil, "")
			if failure.Code == "" {
				t.Fatal("watch readiness acknowledged stale lifetime")
			}
			s := f.scope()
			if !s.DenyLatched || s.State == "accepting" || s.WriterStarted {
				t.Fatal("watch readiness reopened expired/lost scope")
			}
		})
	}
}

func TestOwnedSeparateProcessesUnconfirmedWriterCrashHeld(t *testing.T) {
	for _, point := range []string{"after-journal:writer-claim", "after-content-create", "after-journal:content-identity"} {
		t.Run(point, func(t *testing.T) {
			f := newOwnedProcessFixture(t)
			ready := f.ready()
			c := f.control("ingest", ready)
			c["content_length"] = 3
			cmd := f.command("ingest", "")
			cmd.Env = append(cmd.Env, "OUTFITTER_OWNED_TEST_CRASH="+point)
			raw, _ := json.Marshal(c)
			cmd.Stdin = bytes.NewReader(append(append(raw, '\n'), []byte("abc")...))
			if out, e := cmd.CombinedOutput(); e == nil || cmd.ProcessState.ExitCode() != 86 || len(out) != 0 {
				t.Fatalf("crash seam failed: %v %s", e, out)
			}
			s := f.waitScope(func(s owneddata.Scope) bool { return s.DenyLatched && len(s.Observations) > 0 })
			if !s.WriterStarted || s.ContentCloseConfirmed {
				t.Fatal("crash generated false close proof")
			}
			for _, o := range s.Observations {
				if o.WritersDrained == "observed" || o.ScopeAccessRevocation == "observed" || o.OwnedRemoval == "observed_absent" {
					t.Fatal("crash/lease freedom supplied close proof")
				}
			}
			if point != "after-journal:writer-claim" {
				if _, e := os.Stat(filepath.Join(f.root, ready.ScopeID, "payload")); e != nil {
					t.Fatal("uncertain orphan removed", e)
				}
			}
		})
	}
}

func TestOwnedSeparateProcessesSaturatedObservationsStillRemove(t *testing.T) {
	f := newOwnedProcessFixture(t)
	ready := f.ready()
	c := f.control("ingest", ready)
	c["content_length"] = 3
	cmd := f.command("ingest", "")
	input, e := cmd.StdinPipe()
	if e != nil {
		t.Fatal(e)
	}
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		input.Close()
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	raw, _ := json.Marshal(c)
	_, _ = input.Write(append(raw, '\n'))
	f.waitScope(func(s owneddata.Scope) bool { return s.WriterStarted && s.FilesystemIdentity.Content != nil })
	for j := 0; j < 10 && !f.scope().ObservationsSaturated; j++ {
		_, failure := f.invoke("cancel", f.control("cancel", ready), nil, "")
		if failure.Code != "" {
			t.Fatal(failure)
		}
	}
	s := f.scope()
	if len(s.Observations) != 8 || !s.ObservationsSaturated {
		t.Fatal("observation bound not reached")
	}
	before, _ := json.Marshal(s.Observations)
	_, _ = input.Write([]byte("abc"))
	input.Close()
	if cmd.Wait() == nil {
		t.Fatal("cancelled stream succeeded")
	}
	_, failure := f.invoke("reconcile", f.control("reconcile", ready), nil, "")
	if failure.Code != "" {
		t.Fatal(failure)
	}
	s = f.scope()
	after, _ := json.Marshal(s.Observations)
	if !bytes.Equal(before, after) || s.State != "cleanup_unresolved" {
		t.Fatal("saturation rewrote observations or claimed closure")
	}
	if _, e = os.Stat(filepath.Join(f.root, ready.ScopeID)); !os.IsNotExist(e) {
		t.Fatal("saturation prevented safe owned removal", e)
	}
}

func TestOwnedSeparateProcessesRemovalAndObservationFailures(t *testing.T) {
	for _, point := range []string{"before-owned-removal", "after-owned-removal", "before-journal:observation", "after-journal:observation"} {
		t.Run(point, func(t *testing.T) {
			f := newOwnedProcessFixture(t)
			ready := f.ready()
			c := f.control("ingest", ready)
			c["content_length"] = 3
			complete, failure := f.invoke("ingest", c, []byte("abc"), "")
			if failure.Code != "" {
				t.Fatal(failure)
			}
			_, _ = f.invoke("cancel", f.control("cancel", complete), nil, point)
			before := f.scope().Observations
			status, failure := f.invoke("reconcile", f.control("reconcile", complete), nil, "")
			if failure.Code != "" || status.State != "cleanup_unresolved" || status.CleanupTrusted {
				t.Fatal(status, failure)
			}
			for j, o := range before {
				if j >= len(status.Observations) || o != status.Observations[j] {
					t.Fatal("reconciliation rewrote earlier observations")
				}
			}
		})
	}
}

func TestOwnedAuthorityRefusesOrdinaryReconciliationBeforeProvider(t *testing.T) {
	f := newOwnedProcessFixture(t)
	s, e := ledger.Open(f.state, false)
	if e != nil {
		t.Fatal(e)
	}
	p := &fake{exists: true}
	b := New(s, p)
	if _, e = b.Reconcile("prior-unresolved"); e == nil {
		t.Fatal("ordinary reconciliation accepted v6")
	}
	if !p.exists {
		t.Fatal("ordinary reconciliation called provider teardown before refusing v6")
	}
	if _, e = b.Cancel("prior-unresolved", "ordinary cancel"); e == nil {
		t.Fatal("ordinary cancel accepted v6")
	}
	if _, _, e = b.Reserve(provider.Request{}); e == nil {
		t.Fatal("ordinary reserve accepted v6")
	}
	// A nil provider makes any attempted provider method an immediate failure.
	// These direct API controls cover more than the CLI's early dispatcher.
	b = New(s, nil)
	if _, e = b.Reconcile("prior-unresolved"); e == nil {
		t.Fatal("nil-provider reconciliation admitted")
	}
	if _, e = b.Cancel("prior-unresolved", "cancel"); e == nil {
		t.Fatal("nil-provider cancellation admitted")
	}
	if _, e = b.Run(context.Background(), provider.Request{}); e == nil {
		t.Fatal("nil-provider run admitted")
	}
	if _, _, e = b.RunOnce(context.Background(), provider.Request{}); e == nil {
		t.Fatal("nil-provider run-once admitted")
	}
	if receipts, e := b.Reap(); e != nil || len(receipts) != 0 {
		t.Fatal("owned-only reap touched legacy compute", e)
	}
	if e = b.Watch(context.Background(), "prior-unresolved"); e == nil {
		t.Fatal("owned-only watch accepted legacy compute")
	}
}
