package broker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/assessment"
	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

// The driver reuses this already-built test executable. It never builds or
// installs a helper, invokes Python, or provides production activation.
type executionSessionDriver struct {
	config                                                                    executionSessionConfig
	binding                                                                   x.ScopeBinding
	store                                                                     *ledger.Store
	primaryIn, primaryOut, safetyIn, safetyOut, upload, ownerWrite, watchRead *os.File
	owned                                                                     []*os.File
	closed                                                                    map[*os.File]bool
	closeErr                                                                  error
	sequence                                                                  map[byte]uint64
	cmd                                                                       *exec.Cmd
	cancel                                                                    context.CancelFunc
	wait                                                                      chan error
	stderr                                                                    chan error
	stderrDone                                                                chan struct{}
	mu                                                                        sync.Mutex
	primaryFiller                                                             *os.File
}

type executionSessionDriverOptions struct {
	prequeuePrepareTrigger bool
	retainReplyFiller      bool
	cleanupOrder           bool
}

func (d *executionSessionDriver) closeFile(f *os.File) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if f == nil || d.closed[f] {
		return nil
	}
	d.closed[f] = true
	e := f.Close()
	if e != nil {
		d.closeErr = e
	}
	return e
}

// NewFile observes an already-nonblocking atomic duplicate, so Go's spawn Fd
// conversion does not switch its shared open-file description to blocking.
func executionSessionSpawnDuplicate(source *os.File) (*os.File, error) {
	raw, e := source.SyscallConn()
	if e != nil {
		return nil, e
	}
	var duplicate *os.File
	var nativeErr error
	e = raw.Control(func(fd uintptr) {
		flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFL, 0)
		if errno != 0 || flags&syscall.O_NONBLOCK == 0 {
			nativeErr = executionSessionProtocolError
			return
		}
		n, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_DUPFD_CLOEXEC, 0)
		if errno != 0 {
			nativeErr = errno
			return
		}
		duplicate = os.NewFile(n, "private-session-spawn")
		if duplicate == nil {
			nativeErr = executionSessionProtocolError
		}
	})
	if e != nil {
		return duplicate, e
	}
	return duplicate, nativeErr
}

func newExecutionSessionDriver(t *testing.T, options ...executionSessionDriverOptions) *executionSessionDriver {
	t.Helper()
	d := &executionSessionDriver{closed: map[*os.File]bool{}, sequence: map[byte]uint64{}, wait: make(chan error, 1), stderr: make(chan error, 1)}
	// Register TempDir first so endpoint/process cleanup runs before its removal.
	base := t.TempDir()
	// Register partial cleanup before constructing any resources.
	t.Cleanup(func() {
		if d.cancel != nil {
			d.cancel()
		}
		for _, f := range d.owned {
			_ = d.closeFile(f)
		}
		if d.cmd != nil && d.cmd.Process != nil {
			select {
			case <-d.wait:
			case <-time.After(3 * time.Second):
				t.Error("original session process did not reap")
			}
		}
		if d.closeErr != nil {
			t.Error("checked driver endpoint close failed")
		}
		if d.stderrDone != nil {
			select {
			case <-d.stderrDone:
			case <-time.After(time.Second):
				t.Error("bounded diagnostic reader did not drain")
			}
		}
	})
	state, data := filepath.Join(base, "state"), filepath.Join(base, "data")
	for _, path := range []string{state, data} {
		if e := os.Mkdir(path, 0700); e != nil {
			t.Fatal(e)
		}
	}
	var e error
	state, e = filepath.EvalSymlinks(state)
	if e != nil {
		t.Fatal(e)
	}
	data, e = filepath.EvalSymlinks(data)
	if e != nil {
		t.Fatal(e)
	}
	idAt := func(path string) x.FilesystemIdentity {
		st, e := os.Stat(path)
		if e != nil {
			t.Fatal(e)
		}
		id, e := ledger.FileIdentity(st)
		if e != nil {
			t.Fatal(e)
		}
		return id
	}
	for _, name := range []string{"initialized", "ledger.lock", "execution.lock"} {
		if e := os.WriteFile(filepath.Join(state, name), nil, 0600); e != nil {
			t.Fatal(e)
		}
	}
	identity := x.AuthorityIdentity{WaypointAuthorityID: strings.Repeat("4", 32), Directory: idAt(state), JournalLease: idAt(filepath.Join(state, "ledger.lock")), ExecutionLease: idAt(filepath.Join(state, "execution.lock"))}
	project := strings.Repeat("2", 32)
	seed := ledger.Data{Version: 7, Owner: strings.Repeat("f", 32), Authority: &ledger.Authority{ID: strings.Repeat("5", 32), StatePath: state, Admission: "active"}, Projects: map[string]ledger.Project{project: {ID: project, Budget: 60}}, Runs: map[string]provider.Record{}, AssessmentExecution: &x.Authority{Schema: x.AuthoritySchema, Identity: identity, Scopes: map[string]x.ExecutionScope{}, Attempts: map[string]x.AssessmentAttempt{}}}
	raw, e := json.Marshal(seed)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(state, "ledger.json"), raw, 0600); e != nil {
		t.Fatal(e)
	}
	d.store, e = ledger.Open(state, false)
	if e != nil {
		t.Fatal(e)
	}
	var m assessment.ManifestV2
	if e = x.DecodeClosed(executionSessionManifestSource, &m, 65536); e != nil {
		t.Fatal(e)
	}
	now := time.Now().UTC()
	m.ProjectID, m.ExpiresAt = project, now.Add(time.Hour).Truncate(time.Second).Format(owneddata.SecondUTC)
	expiry := now.Add(time.Hour).Truncate(time.Second)
	d.config = executionSessionConfig{StateRoot: state, DataRoot: data, WaypointAuthorityID: identity.WaypointAuthorityID, ScopeID: strings.Repeat("3", 32), OwnerEpoch: strings.Repeat("8", 32), ScopeExpiresAt: executionSessionExpiry(now, expiry).Format(owneddata.SecondUTC), AuthorizationExpiry: expiry.Format(owneddata.SecondUTC), ProjectExpiry: expiry.Format(owneddata.SecondUTC), PolicyExpiry: expiry.Format(owneddata.SecondUTC), Manifest: m}
	_, d.binding = executionSessionBindings(d.config)
	configPath := filepath.Join(base, "private-bootstrap.json")
	pipe := func() (*os.File, *os.File) {
		r, w, e := os.Pipe()
		if r != nil {
			d.owned = append(d.owned, r)
		}
		if w != nil {
			d.owned = append(d.owned, w)
		}
		if e != nil {
			t.Fatal(e)
		}
		return r, w
	}
	var child [9]*os.File
	child[0], d.primaryIn = pipe()
	d.primaryOut, child[1] = pipe()
	errRead, errWrite := pipe()
	child[2] = errWrite
	child[3], d.ownerWrite = pipe()
	child[4], child[5] = pipe()
	d.watchRead = child[4]
	child[6], d.upload = pipe()
	child[7], d.safetyIn = pipe()
	d.safetyOut, child[8] = pipe()
	var inherited [9]*os.File
	for i, f := range child {
		inherited[i], e = executionSessionSpawnDuplicate(f)
		if inherited[i] != nil {
			d.owned = append(d.owned, inherited[i])
		}
		if e != nil {
			t.Fatal("checked spawn duplicate", e)
		}
	}
	var option executionSessionDriverOptions
	if len(options) > 1 {
		t.Fatal("fixed driver options")
	}
	if len(options) == 1 {
		option = options[0]
	}
	if option.retainReplyFiller {
		d.primaryFiller, e = executionSessionSpawnDuplicate(child[1])
		if d.primaryFiller != nil {
			d.owned = append(d.owned, d.primaryFiller)
		}
		if e != nil {
			t.Fatal(e)
		}
	}
	// The original admission bound starts BEFORE spawn, not in the child.
	if e = d.config.bindSessionDeadline(time.Now().UTC()); e != nil {
		t.Fatal(e)
	}
	configRaw, e := json.Marshal(d.config)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(configPath, configRaw, 0600); e != nil {
		t.Fatal(e)
	}
	if option.prequeuePrepareTrigger {
		frame, e := assessment.Canonical(d.prepareControl())
		if e != nil {
			t.Fatal(e)
		}
		frame = append(frame, '\n')
		var prefix [4]byte
		binary.BigEndian.PutUint32(prefix[:], uint32(len(frame)))
		packet := append(prefix[:], frame...)
		packet = append(packet, 1)
		if e = d.primaryIn.SetWriteDeadline(time.Now().Add(time.Second)); e != nil {
			t.Fatal(e)
		}
		if e = executionSessionWrite(d.primaryIn, packet); e != nil {
			t.Fatal("bounded prelaunch queued packet", e)
		}
	}
	deadline, e := owneddata.Time(d.config.SessionDeadline, true)
	if e != nil {
		t.Fatal(e)
	}
	// Five extra seconds permit only original-process kill/reap and checked
	// teardown; child policy/context and every trigger remain capped earlier.
	ctx, cancel := context.WithDeadline(context.Background(), deadline.Add(5*time.Second))
	d.cancel = cancel
	d.cmd = exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExecutionPrivateSessionOwnerHelper$")
	d.cmd.Env = []string{"OUTFITTER_PRIVATE_EXECUTION_SESSION=1", "OUTFITTER_PRIVATE_EXECUTION_SESSION_CONFIG=" + configPath, "GOMAXPROCS=2"}
	if option.cleanupOrder {
		d.cmd.Env = append(d.cmd.Env, "OUTFITTER_PRIVATE_EXECUTION_SESSION_CAUSAL=cleanup-order")
	}
	d.cmd.Stdin, d.cmd.Stdout, d.cmd.Stderr = inherited[0], inherited[1], inherited[2]
	d.cmd.ExtraFiles = append([]*os.File(nil), inherited[3:]...)
	d.cmd.WaitDelay = 250 * time.Millisecond
	if e = d.cmd.Start(); e != nil {
		t.Fatal("original session spawn", e)
	}
	go func() { d.wait <- d.cmd.Wait() }()
	for _, f := range inherited {
		if e = d.closeFile(f); e != nil {
			t.Fatal("inherited duplicate close", e)
		}
	}
	for i, f := range child {
		if i != 4 {
			if e = d.closeFile(f); e != nil {
				t.Fatal("parent child-end close", e)
			}
		}
	}
	d.stderrDone = make(chan struct{})
	go func() {
		defer close(d.stderrDone)
		// Never retain raw child diagnostics. Exceeding the tiny closed channel
		// bound is a failure; it cannot grow memory or contaminate protocol bytes.
		n, e := io.Copy(io.Discard, io.LimitReader(errRead, 65537))
		if n > 65536 {
			e = executionSessionProtocolError
		}
		d.stderr <- e
	}()
	return d
}

func (d *executionSessionDriver) endpoints(lane byte) (*os.File, *os.File) {
	if lane == 'P' {
		return d.primaryIn, d.primaryOut
	}
	return d.safetyIn, d.safetyOut
}

func (d *executionSessionDriver) stage(t *testing.T, lane byte, c x.Control) {
	t.Helper()
	in, out := d.endpoints(lane)
	deadline := time.Now().Add(executionSessionStageTime)
	if e := in.SetWriteDeadline(deadline); e != nil {
		t.Fatal(e)
	}
	if e := out.SetReadDeadline(deadline); e != nil {
		t.Fatal(e)
	}
	frame, e := assessment.Canonical(c)
	if e != nil {
		t.Fatal(e)
	}
	frame = append(frame, '\n')
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(frame)))
	if e = executionSessionWrite(in, prefix[:]); e != nil {
		t.Fatal("stage prefix", e)
	}
	if e = executionSessionWrite(in, frame); e != nil {
		t.Fatal("stage frame", e)
	}
	var ack [executionSessionACKBytes]byte
	if _, e = io.ReadFull(out, ack[:]); e != nil {
		t.Fatal("inert stage ACK", e)
	}
	d.sequence[lane]++
	digest := sha256.Sum256(frame)
	if string(ack[:8]) != "EXACK001" || ack[8] != lane || binary.BigEndian.Uint64(ack[9:17]) != d.sequence[lane] || binary.BigEndian.Uint32(ack[17:21]) != uint32(len(frame)) || !bytes.Equal(ack[21:], digest[:]) {
		t.Fatal("stage ACK correlation")
	}
}

func (d *executionSessionDriver) trigger(t *testing.T, lane byte, execute bool) []byte {
	t.Helper()
	in, _ := d.endpoints(lane)
	if e := executionSessionWrite(in, []byte{1}); e != nil {
		t.Fatal("single trigger", e)
	}
	return d.readReply(t, lane, execute)
}

func (d *executionSessionDriver) readReply(t *testing.T, lane byte, execute bool) []byte {
	t.Helper()
	limit := executionSessionReplyTime
	if execute {
		limit = executionSessionExecuteTime
	}
	deadline, e := owneddata.Time(d.config.SessionDeadline, true)
	if e != nil {
		t.Fatal("private session deadline")
	}
	return d.readReplyUntil(t, lane, executionSessionMinDeadline(time.Now().Add(limit), deadline))
}

func (d *executionSessionDriver) readReplyUntil(t *testing.T, lane byte, deadline time.Time) []byte {
	t.Helper()
	_, out := d.endpoints(lane)
	if e := out.SetReadDeadline(deadline); e != nil {
		t.Fatal(e)
	}
	var raw []byte
	for len(raw) < x.MaxReplyBytes {
		var one [1]byte
		if _, e := io.ReadFull(out, one[:]); e != nil {
			t.Fatal("bounded public reply", e)
		}
		raw = append(raw, one[0])
		if one[0] == '\n' {
			break
		}
	}
	if len(raw) == 0 || raw[len(raw)-1] != '\n' {
		t.Fatal("reply bound")
	}
	for _, poison := range []string{"PRIVATE_INPUT_POISON", "PRIVATE_OUTPUT_POISON", "PRIVATE_STDERR_POISON", d.config.StateRoot, d.config.DataRoot} {
		if bytes.Contains(raw, []byte(poison)) {
			t.Fatal("private content in public reply")
		}
	}
	return raw
}

func (d *executionSessionDriver) finish(t *testing.T) {
	t.Helper()
	for _, f := range []*os.File{d.primaryIn, d.safetyIn} {
		if e := d.closeFile(f); e != nil {
			t.Fatal(e)
		}
	}
	select {
	case e := <-d.wait:
		d.cmd = nil
		if e != nil {
			t.Fatal("original helper exit", executionProcessErrorClass(e))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("original helper did not exit after checked teardown")
	}
	var one [1]byte
	for _, f := range []*os.File{d.primaryOut, d.safetyOut} {
		if e := f.SetReadDeadline(time.Now().Add(time.Second)); e != nil {
			t.Fatal(e)
		}
		if n, e := f.Read(one[:]); n != 0 || !errors.Is(e, io.EOF) {
			t.Fatal("extra protocol or test harness stdout")
		}
	}
	select {
	case e := <-d.stderr:
		if e != nil {
			t.Fatal("bounded stderr drain")
		}
	case <-time.After(time.Second):
		t.Fatal("stderr not drained")
	}
}

func (d *executionSessionDriver) status(t *testing.T, lane byte, c x.Control) x.Status {
	t.Helper()
	d.stage(t, lane, c)
	raw := d.trigger(t, lane, false)
	s, e := x.DecodeStatus(raw, c)
	if c.Operation == "reconcile" && c.Slot == nil {
		d.reconcileDiagnostic(t, raw, s, e)
	}
	if e != nil {
		t.Fatal("closed correlated status", e)
	}
	return s
}

func (d *executionSessionDriver) reconcileDiagnostic(t *testing.T, raw []byte, status x.Status, decodeErr error) {
	t.Helper()
	category, code := "unknown", "unknown"
	if decodeErr == nil {
		category, code = "status", status.Code
	} else if len(raw) > 0 {
		var failure x.Error
		if x.DecodeClosed(raw[:len(raw)-1], &failure, x.MaxReplyBytes) == nil {
			canonical, e := x.EncodeReply(failure)
			if e == nil && bytes.Equal(raw, canonical) {
				category, code = "error", failure.Code
			}
		}
	}
	// Record only validated closed category/code and current booleans/counters.
	// This later snapshot is not an atomic explanation of an earlier reply.
	t.Logf("scope reconcile reply: category=%s code=%s status_decoded=%t", category, code, decodeErr == nil)
	var scope x.ExecutionScope
	present, attempts, serialClear := false, 0, 0
	readErr := d.store.View(func(data ledger.Data) error {
		if data.AssessmentExecution == nil {
			return x.ErrHeld
		}
		scope, present = data.AssessmentExecution.Scopes[d.config.ScopeID]
		for _, a := range data.AssessmentExecution.Attempts {
			if a.Binding.ScopeID == d.config.ScopeID {
				attempts++
				if a.SerialClear() {
					serialClear++
				}
			}
		}
		return nil
	})
	t.Logf("scope reconcile later snapshot: read=%s present=%t generation=%d normal=%d safety=%d deny=%t prepare_closed=%t writer_closed=%t seal_closed=%t watch_closed=%t cleanup_claimed=%t cleanup_closed=%t lease_uncertain=%t content_closed=%t content_uncertain=%t observations=%d attempts=%d serial_clear=%d", executionProcessErrorClass(readErr), present, scope.Generation, scope.NormalTransitionsUsed, scope.SafetyTransitionsUsed, scope.DenyLatched, scope.PrepareOwnershipCloseConfirmed, scope.WriterOwnershipCloseConfirmed, scope.SealOwnershipCloseConfirmed, scope.WatchOwnershipCloseConfirmed, scope.CleanupOwnerEpoch != nil, scope.CleanupOwnershipCloseConfirmed, scope.LeaseCloseUncertain, scope.ContentCloseConfirmed, scope.ContentCloseUncertain, len(scope.Observations), attempts, serialClear)
}

func (d *executionSessionDriver) scope(t *testing.T) x.ExecutionScope {
	t.Helper()
	var scope x.ExecutionScope
	e := d.store.View(func(data ledger.Data) error { scope = data.AssessmentExecution.Scopes[d.config.ScopeID]; return nil })
	if e != nil {
		t.Fatal(e)
	}
	return scope
}

func (d *executionSessionDriver) prepareControl() x.Control {
	ceiling := int64(1024)
	return x.Control{Schema: x.ControlSchema, Operation: "prepare", Binding: d.binding, Manifest: &d.config.Manifest, AdmittingOwnerEpoch: &d.config.OwnerEpoch, ExpiresAt: &d.config.ScopeExpiresAt, DeclaredUploadCeilingBytes: &ceiling}
}

func TestExecutionSessionActualOwnerLifecycleAndInertStage(t *testing.T) {
	d := newExecutionSessionDriver(t)
	before, e := os.ReadFile(filepath.Join(d.config.StateRoot, "ledger.json"))
	if e != nil {
		t.Fatal(e)
	}
	prepare := d.prepareControl()
	d.stage(t, 'P', prepare)
	after, e := os.ReadFile(filepath.Join(d.config.StateRoot, "ledger.json"))
	if e != nil {
		t.Fatal(e)
	}
	entries, e := os.ReadDir(d.config.DataRoot)
	if e != nil || len(entries) != 0 || !bytes.Equal(before, after) {
		t.Fatal("inert stage performed core/data effect")
	}
	raw := d.trigger(t, 'P', false)
	status, e := x.DecodeStatus(raw, prepare)
	if e != nil {
		t.Fatal("actual prepare", e)
	}
	length := int64(len(executionSessionInputSource))
	generation := status.Generation
	writer := status.WriterEpoch
	ingest := x.Control{Schema: x.ControlSchema, Operation: "ingest", Binding: d.binding, ExpectedGeneration: &generation, AdmittingOwnerEpoch: &d.config.OwnerEpoch, WriterEpoch: &writer, ContentLength: &length}
	d.stage(t, 'P', ingest)
	deadline, e := owneddata.Time(d.config.SessionDeadline, true)
	if e != nil {
		t.Fatal("private session deadline")
	}
	bodyDeadline := executionSessionMinDeadline(time.Now().Add(executionSessionReplyTime), deadline)
	if e = executionSessionWrite(d.primaryIn, []byte{1}); e != nil {
		t.Fatal("ingest trigger", e)
	}
	if e = d.upload.SetWriteDeadline(bodyDeadline); e != nil {
		t.Fatal(e)
	}
	if e = executionSessionWrite(d.upload, executionSessionInputSource); e != nil {
		t.Fatal(e)
	}
	if e = d.closeFile(d.upload); e != nil {
		t.Fatal(e)
	}
	raw = d.readReplyUntil(t, 'P', bodyDeadline)
	status, e = x.DecodeStatus(raw, ingest)
	if e != nil {
		t.Fatal("closed correlated ingest status", e)
	}
	generation = status.Generation
	seal := ingest
	seal.Operation = "seal"
	seal.ContentLength = nil
	status = d.status(t, 'P', seal)
	generation = status.Generation
	// Healthy reconciliation must retain the original live watcher rather
	// than join it. This spends one of the two fixed scope opportunities.
	healthy := d.status(t, 'S', x.Control{Schema: x.ControlSchema, Operation: "reconcile", Binding: d.binding})
	if healthy.Code != "OK" {
		t.Fatal("healthy reconcile did not preserve live watcher semantics")
	}
	for i, slot := range d.config.Manifest.Reservations {
		nonce := strings.Repeat([]string{"e", "f"}[i], 32)
		c := x.Control{Schema: x.ControlSchema, Operation: "execute", Binding: d.binding, ExpectedGeneration: &generation, AdmittingOwnerEpoch: &d.config.OwnerEpoch, Slot: &slot, HandoffNonce: &nonce}
		d.stage(t, 'P', c)
		raw = d.trigger(t, 'P', true)
		var attempt x.AssessmentAttempt
		scope := d.scope(t)
		e = d.store.View(func(data ledger.Data) error { attempt = data.AssessmentExecution.Attempts[slot.AttemptID]; return nil })
		if e != nil {
			t.Fatal(e)
		}
		r, e := x.DecodeResult(raw, c, scope, &attempt)
		if e != nil || r.Code != "OK" || r.Disposition != "new_attempt" || r.Attempt == nil || r.Attempt.Outcome != "succeeded" || !attempt.SerialClear() || r.Trust != x.Untrusted() || r.Compute.ReservedCPUSeconds != 30 || r.Compute.ChargedCPUSeconds != 30 || r.AccountingUnresolved {
			t.Fatal("actual original-owner phase result", executionProcessErrorClass(e))
		}
		// The existing public Result carries the next current scope generation.
		// Private state is only an independent assertion, never a frame repair.
		generation = r.ScopeGeneration
		if scope.Generation != generation {
			t.Fatal("public scope-generation correlation changed")
		}
	}
	if e = d.closeFile(d.primaryIn); e != nil {
		t.Fatal(e)
	}
	reason := "cancelled"
	cancel := x.Control{Schema: x.ControlSchema, Operation: "cancel", Binding: d.binding, Reason: &reason}
	d.stage(t, 'S', cancel)
	raw = d.trigger(t, 'S', false)
	if _, e = x.DecodeStatus(raw, cancel); e != nil {
		var held x.Error
		if x.DecodeClosed(raw[:len(raw)-1], &held, x.MaxReplyBytes) != nil || held.Schema != x.ErrorSchema || held.Code != x.ErrHeld.Error() {
			t.Fatal("cancel outcome was not definitive HELD/status")
		}
	}
	// Watch EOF is a transport observation only. Core cleanup confirmation is
	// checked independently below, after the helper joins the exact original
	// callback and still asks the unchanged Broker to reconcile.
	if e = d.watchRead.SetReadDeadline(time.Now().Add(3 * time.Second)); e != nil {
		t.Fatal(e)
	}
	var one [1]byte
	if n, e := d.watchRead.Read(one[:]); n != 0 || !errors.Is(e, io.EOF) {
		t.Fatal("original watch endpoint did not drain")
	}
	reconcile := x.Control{Schema: x.ControlSchema, Operation: "reconcile", Binding: d.binding}
	d.status(t, 'S', reconcile)
	final := x.Control{Schema: x.ControlSchema, Operation: "status", Binding: d.binding}
	status = d.status(t, 'S', final)
	scope := d.scope(t)
	if !scope.DenyLatched || !scope.CleanupOwnershipCloseConfirmed || !scope.WatchOwnershipCloseConfirmed || scope.LeaseCloseUncertain || status.Code != "HELD" {
		t.Fatal("actual checked lifecycle cleanup absent")
	}
	usage, e := d.store.Usage(d.config.Manifest.ProjectID)
	if e != nil || usage.Charged != 60 || usage.Reserved != 0 || usage.Unknown != 0 || usage.Remaining != 0 {
		t.Fatal("exact two-slot accounting")
	}
	d.finish(t)
	entries, e = os.ReadDir(d.config.DataRoot)
	if e != nil || len(entries) != 0 {
		t.Fatal("payload directory remained after actual removal")
	}
	e = filepath.WalkDir(d.config.StateRoot, func(path string, entry os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if entry.IsDir() {
			return nil
		}
		f, e := os.Open(path)
		if e != nil {
			return e
		}
		raw, e := io.ReadAll(io.LimitReader(f, (4<<20)+1))
		ce := f.Close()
		if e != nil || ce != nil || len(raw) > 4<<20 {
			return executionSessionProtocolError
		}
		for _, poison := range []string{"PRIVATE_INPUT_POISON", "PRIVATE_OUTPUT_POISON", "PRIVATE_STDERR_POISON"} {
			if bytes.Contains(raw, []byte(poison)) {
				return executionSessionProtocolError
			}
		}
		return nil
	})
	if e != nil {
		t.Fatal("private phase content retained in durable state")
	}
}
