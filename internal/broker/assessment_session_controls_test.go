package broker

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/assessment"
	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

func TestExecutionSessionOriginalCleanupReturnDiffersFromWatchEOF(t *testing.T) {
	d := newExecutionSessionDriver(t, executionSessionDriverOptions{cleanupOrder: true})
	status := d.status(t, 'P', d.prepareControl())
	generation, writer := status.Generation, status.WriterEpoch
	length := int64(len(executionSessionInputSource))
	ingest := x.Control{Schema: x.ControlSchema, Operation: "ingest", Binding: d.binding, ExpectedGeneration: &generation, AdmittingOwnerEpoch: &d.config.OwnerEpoch, WriterEpoch: &writer, ContentLength: &length}
	d.stage(t, 'P', ingest)
	absolute, e := owneddata.Time(d.config.SessionDeadline, true)
	if e != nil {
		t.Fatal("causal fixture original deadline")
	}
	deadline := executionSessionMinDeadline(time.Now().Add(executionSessionReplyTime), absolute)
	if e := executionSessionWrite(d.primaryIn, []byte{1}); e != nil {
		t.Fatal(e)
	}
	if e := d.upload.SetWriteDeadline(deadline); e != nil {
		t.Fatal(e)
	}
	if e := executionSessionWrite(d.upload, executionSessionInputSource); e != nil {
		t.Fatal(e)
	}
	if e := d.closeFile(d.upload); e != nil {
		t.Fatal(e)
	}
	status, e = x.DecodeStatus(d.readReplyUntil(t, 'P', deadline), ingest)
	if e != nil {
		t.Fatal("causal fixture actual ingest", e)
	}
	generation = status.Generation
	seal := ingest
	seal.Operation, seal.ContentLength = "seal", nil
	d.status(t, 'P', seal)
	// The healthy original watcher is deterministically paused before its first
	// snapshot. Cancel denies the scope but cannot consume cleanup while watch
	// closure is outstanding. Only the first reconcile trigger releases watch.
	reason := "cancelled"
	cancel := x.Control{Schema: x.ControlSchema, Operation: "cancel", Binding: d.binding, Reason: &reason}
	d.stage(t, 'S', cancel)
	raw := d.trigger(t, 'S', false)
	var held x.Error
	if x.DecodeClosed(raw[:len(raw)-1], &held, x.MaxReplyBytes) != nil || held.Schema != x.ErrorSchema || held.Code != x.ErrHeld.Error() {
		t.Fatal("paused live watcher did not preserve unresolved cancellation")
	}
	scope := d.scope(t)
	if !scope.DenyLatched || scope.CleanupOwnerEpoch != nil || scope.WatchOwnershipCloseConfirmed || scope.LeaseCloseUncertain {
		t.Fatal("cancel consumed cleanup before original watch closure")
	}
	reconcile := x.Control{Schema: x.ControlSchema, Operation: "reconcile", Binding: d.binding}
	d.stage(t, 'S', reconcile)
	raw = d.trigger(t, 'S', false)
	held = x.Error{}
	if x.DecodeClosed(raw[:len(raw)-1], &held, x.MaxReplyBytes) != nil || held.Schema != x.ErrorSchema || held.Code != x.ErrHeld.Error() {
		t.Fatal("held original cleanup was promoted to successful reconcile")
	}
	// The child is parked in its original storage.Remove, after exact cleanup
	// acquisition/freshness and outside authority locks. This state is stable
	// until the second distinct reconcile releases the private gate.
	scope = d.scope(t)
	if scope.CleanupOwnerEpoch == nil || scope.CleanupOwnershipCloseConfirmed || !scope.TransientOwnershipClosed(true) || scope.ContentCloseUncertain || !scope.ContentCloseConfirmed {
		t.Fatal("original cleanup was not held after checked transient closure")
	}
	epoch := *scope.CleanupOwnerEpoch
	if _, e := os.Stat(filepath.Join(d.config.DataRoot, scope.ScopeID, "payload")); e != nil {
		t.Fatal("held original cleanup did not retain actual payload")
	}
	if e := d.watchRead.SetReadDeadline(time.Now().Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	var one [1]byte
	if n, e := d.watchRead.Read(one[:]); n != 0 || !errors.Is(e, io.EOF) {
		t.Fatal("checked original watch close did not produce transport EOF")
	}
	// There is no retry of the first invocation. The second allowed command
	// releases, joins the same original callback, then calls unchanged Broker.
	d.status(t, 'S', reconcile)
	scope = d.scope(t)
	if scope.CleanupOwnerEpoch == nil || *scope.CleanupOwnerEpoch != epoch || !scope.CleanupOwnershipCloseConfirmed || !scope.TransientOwnershipClosed(true) || scope.LeaseCloseUncertain {
		t.Fatal("original cleanup epoch did not supply actual checked closure")
	}
	if _, e := os.Stat(filepath.Join(d.config.DataRoot, scope.ScopeID)); !os.IsNotExist(e) {
		t.Fatal("joined original cleanup did not actually remove scope")
	}
	d.status(t, 'S', x.Control{Schema: x.ControlSchema, Operation: "status", Binding: d.binding})
	usage, e := d.store.Usage(d.config.Manifest.ProjectID)
	if e != nil || usage.Charged != 0 || usage.Reserved != 0 || usage.Unknown != 0 || usage.Remaining != 60 {
		t.Fatal("causal synchronization spent execution budget")
	}
	if e := d.store.View(func(data ledger.Data) error {
		if len(data.AssessmentExecution.Attempts) != 0 {
			return x.ErrInvalid
		}
		return nil
	}); e != nil {
		t.Fatal("causal synchronization created an execution attempt")
	}
	d.finish(t)
}

func (d *executionSessionDriver) failedExit(t *testing.T, limit time.Duration, inert bool) {
	t.Helper()
	select {
	case e := <-d.wait:
		d.cmd = nil
		var exit *exec.ExitError
		if !errors.As(e, &exit) || exit.ExitCode() != 2 {
			t.Fatal("expected unresolved original child exit")
		}
	case <-time.After(limit):
		t.Fatal("original child exceeded bounded transport shutdown")
	}
	e := d.store.View(func(data ledger.Data) error {
		if len(data.AssessmentExecution.Attempts) != 0 {
			return x.ErrInvalid
		}
		if inert && len(data.AssessmentExecution.Scopes) != 0 {
			return x.ErrInvalid
		}
		for _, s := range data.AssessmentExecution.Scopes {
			if !s.DenyLatched {
				return x.ErrInvalid
			}
		}
		return nil
	})
	if e != nil {
		t.Fatal("transport refusal created admission or left live scope")
	}
	if inert {
		entries, e := os.ReadDir(d.config.DataRoot)
		if e != nil || len(entries) != 0 {
			t.Fatal("inert refusal performed payload effects")
		}
	}
	usage, e := d.store.Usage(d.config.Manifest.ProjectID)
	if e != nil || usage.Charged != 0 || usage.Reserved != 0 || usage.Unknown != 0 || usage.Remaining != 60 {
		t.Fatal("transport refusal spent execution budget")
	}
}

func TestExecutionSessionRejectsTriggerQueuedBeforeACK(t *testing.T) {
	// The full prepare frame and trigger are present before the original helper
	// starts. There is no scheduling inference about whether ACK was consumed.
	d := newExecutionSessionDriver(t, executionSessionDriverOptions{prequeuePrepareTrigger: true})
	d.failedExit(t, 3*time.Second, true)
}

func TestExecutionSessionWithheldTriggerHasNoCoreEffects(t *testing.T) {
	d := newExecutionSessionDriver(t)
	d.stage(t, 'P', d.prepareControl())
	// This is a fully valid staged prepare with a checked ACK, unlike a partial
	// prefix. Leave the command peer open and never deliver its trigger.
	d.failedExit(t, 7*time.Second, true)
	// A late byte cannot revive that original process or authorize replacement.
	if _, e := d.primaryIn.Write([]byte{1}); e == nil {
		t.Fatal("late trigger retained a live receiver")
	}
}

func TestExecutionSessionOriginalOwnerLossUsesCheckedWatchDrain(t *testing.T) {
	d := newExecutionSessionDriver(t)
	d.status(t, 'P', d.prepareControl())
	if e := d.closeFile(d.ownerWrite); e != nil {
		t.Fatal(e)
	}
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		s := d.scope(t)
		if s.DenyLatched && s.WatchOwnershipCloseConfirmed && s.CleanupOwnershipCloseConfirmed && !s.LeaseCloseUncertain {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("original watcher did not commit checked denial/drain")
		}
	}
	// Actual Store facts precede this observation. EOF/exit alone is never used
	// to construct release proof or import/recreate the lost opaque owner.
	if e := d.watchRead.SetReadDeadline(time.Now().Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	var one [1]byte
	if n, e := d.watchRead.Read(one[:]); n != 0 || !errors.Is(e, io.EOF) {
		t.Fatal("original watch transport remained open")
	}
	d.status(t, 'S', x.Control{Schema: x.ControlSchema, Operation: "reconcile", Binding: d.binding})
	d.status(t, 'S', x.Control{Schema: x.ControlSchema, Operation: "status", Binding: d.binding})
	usage, e := d.store.Usage(d.config.Manifest.ProjectID)
	if e != nil || usage.Charged != 0 || usage.Reserved != 0 || usage.Unknown != 0 || usage.Remaining != 60 {
		t.Fatal("owner loss spent execution budget")
	}
	d.finish(t)
}

func TestExecutionSessionBlockedStageAndParentLoss(t *testing.T) {
	t.Run("partial-stage-keeps-peer-open", func(t *testing.T) {
		d := newExecutionSessionDriver(t)
		if e := executionSessionWrite(d.primaryIn, []byte{0}); e != nil {
			t.Fatal(e)
		}
		// The peer remains open. Only the child's fixed five-second stage bound
		// can end this read; no parent Close is used to manufacture its wakeup.
		d.failedExit(t, 7*time.Second, true)
	})
	t.Run("parent-command-loss", func(t *testing.T) {
		d := newExecutionSessionDriver(t)
		if e := d.closeFile(d.primaryIn); e != nil {
			t.Fatal(e)
		}
		d.failedExit(t, 3*time.Second, true)
	})
}

func executionSessionFillPipe(file *os.File) error {
	raw, e := file.SyscallConn()
	if e != nil {
		return e
	}
	var result error
	e = raw.Control(func(fd uintptr) {
		flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFL, 0)
		if errno != 0 || flags&syscall.O_NONBLOCK == 0 {
			result = executionSessionProtocolError
			return
		}
		var padding [4096]byte
		for i := 0; i < 128; i++ {
			_, e := syscall.Write(int(fd), padding[:])
			if e == syscall.EAGAIN || e == syscall.EWOULDBLOCK {
				return
			}
			if e != nil {
				result = e
				return
			}
		}
		result = executionSessionProtocolError
	})
	if e != nil {
		return e
	}
	return result
}

func TestExecutionSessionBlockedReplyDrainsOriginalChild(t *testing.T) {
	d := newExecutionSessionDriver(t, executionSessionDriverOptions{retainReplyFiller: true})
	d.stage(t, 'P', d.prepareControl())
	// The explicitly owned parent fault endpoint fills the exact reply pipe
	// after its inert ACK. The parent never reads or closes it to help the child.
	if e := executionSessionFillPipe(d.primaryFiller); e != nil {
		t.Fatal("bounded pipe fill", e)
	}
	if e := executionSessionWrite(d.primaryIn, []byte{1}); e != nil {
		t.Fatal(e)
	}
	d.failedExit(t, 14*time.Second, false)
	// No public reply, exit, or full conservative accounting is interpreted as
	// close proof. Only the core's original cleanup may record truthful facts.
}

func TestExecutionSessionSafetyProgressDuringBlockedUpload(t *testing.T) {
	d := newExecutionSessionDriver(t)
	status := d.status(t, 'P', d.prepareControl())
	generation, writer, length := status.Generation, status.WriterEpoch, int64(len(executionSessionInputSource))
	ingest := x.Control{Schema: x.ControlSchema, Operation: "ingest", Binding: d.binding, ExpectedGeneration: &generation, WriterEpoch: &writer, AdmittingOwnerEpoch: &d.config.OwnerEpoch, ContentLength: &length}
	d.stage(t, 'P', ingest)
	if e := executionSessionWrite(d.primaryIn, []byte{1}); e != nil {
		t.Fatal(e)
	}
	// Wait only for actual current metadata showing an owned content reader is
	// about to block on the empty upload pipe. Polling makes no observation rows.
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		s := d.scope(t)
		if s.WriterStarted && !s.WriterOwnershipCloseConfirmed && s.FilesystemIdentity.Content != nil {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("original writer did not reach blocked upload")
		}
	}
	reason := "cancelled"
	cancel := x.Control{Schema: x.ControlSchema, Operation: "cancel", Binding: d.binding, Reason: &reason}
	d.stage(t, 'S', cancel)
	raw := d.trigger(t, 'S', false)
	var held x.Error
	if x.DecodeClosed(raw[:len(raw)-1], &held, x.MaxReplyBytes) != nil || held.Schema != x.ErrorSchema || held.Code != x.ErrHeld.Error() {
		t.Fatal("active cleanup probe was not definitive HELD")
	}
	scope := d.scope(t)
	if !scope.DenyLatched || scope.CleanupOwnerEpoch != nil || scope.CleanupOwnershipCloseConfirmed || scope.LeaseCloseUncertain || scope.WriterOwnershipCloseConfirmed {
		t.Fatal("busy probe consumed cleanup ownership or poisoned normal cancellation")
	}
	// Only now release the original upload reader. The original writer performs
	// its own checked suppression/close path; the safety lane never adopts it.
	if e := d.closeFile(d.upload); e != nil {
		t.Fatal(e)
	}
	raw = d.readReply(t, 'P', false)
	if x.DecodeClosed(raw[:len(raw)-1], &held, x.MaxReplyBytes) != nil || held.Schema != x.ErrorSchema || (held.Code != x.ErrHeld.Error() && held.Code != x.ErrDenied.Error()) {
		t.Fatal("denied original writer reported success")
	}
	if e := d.watchRead.SetReadDeadline(time.Now().Add(3 * time.Second)); e != nil {
		t.Fatal(e)
	}
	var one [1]byte
	if n, e := d.watchRead.Read(one[:]); n != 0 || !errors.Is(e, io.EOF) {
		t.Fatal("original watch did not close after denial")
	}
	reconcile := x.Control{Schema: x.ControlSchema, Operation: "reconcile", Binding: d.binding}
	d.status(t, 'S', reconcile)
	final := x.Control{Schema: x.ControlSchema, Operation: "status", Binding: d.binding}
	d.status(t, 'S', final)
	scope = d.scope(t)
	if !scope.CleanupOwnershipCloseConfirmed || !scope.WriterOwnershipCloseConfirmed || !scope.WatchOwnershipCloseConfirmed || scope.LeaseCloseUncertain {
		t.Fatal("same owners failed normal cancel cleanup after busy probe")
	}
	usage, e := d.store.Usage(d.config.Manifest.ProjectID)
	if e != nil || usage.Charged != 0 || usage.Reserved != 0 || usage.Unknown != 0 || usage.Remaining != 60 {
		t.Fatal("cancelled upload spent execution budget")
	}
	d.finish(t)
}

type executionSessionUnit struct {
	s       *executionSessionTransport
	in, out *os.File
	files   []*os.File
	workers sync.WaitGroup
}

func newExecutionSessionUnit(t *testing.T) *executionSessionUnit {
	t.Helper()
	u := &executionSessionUnit{}
	t.Cleanup(func() {
		if u.s != nil {
			u.s.abort(context.Canceled)
			u.s.stopTimer()
		}
		u.workers.Wait()
		for _, f := range u.files {
			if e := f.Close(); e != nil {
				t.Error("checked unit pipe close")
			}
		}
	})
	pipe := func() (*os.File, *os.File) {
		r, w, e := os.Pipe()
		if r != nil {
			u.files = append(u.files, r)
		}
		if w != nil {
			u.files = append(u.files, w)
		}
		if e != nil {
			t.Fatal(e)
		}
		return r, w
	}
	input, parentIn := pipe()
	parentOut, output := pipe()
	safetyIn, safetyParent := pipe()
	safetyParentOut, safetyOut := pipe()
	_ = safetyParent
	_ = safetyParentOut
	var e error
	u.s, e = newExecutionSessionTransport(context.Background(), input, output, safetyIn, safetyOut, nil, time.Now().Add(time.Second))
	if e != nil {
		t.Fatal(e)
	}
	u.in, u.out = parentIn, parentOut
	u.s.validate = func(byte, uint64, x.Control) error { return nil }
	return u
}

func (u *executionSessionUnit) startLane() <-chan error {
	done := make(chan error, 1)
	u.workers.Add(1)
	go func() { defer u.workers.Done(); done <- u.s.runLane(&u.s.primary) }()
	return done
}

func (u *executionSessionUnit) noReplyBytes(t *testing.T) {
	t.Helper()
	if e := u.out.SetReadDeadline(time.Now().Add(10 * time.Millisecond)); e != nil {
		t.Fatal(e)
	}
	var one [1]byte
	if n, e := u.out.Read(one[:]); n != 0 || !errors.Is(e, os.ErrDeadlineExceeded) {
		t.Fatal("unexpected reply bytes after poisoned invocation")
	}
}

func executionSessionUnitControl(t *testing.T) x.Control {
	t.Helper()
	var m assessment.ManifestV2
	if e := x.DecodeClosed(executionSessionManifestSource, &m, 65536); e != nil {
		t.Fatal(e)
	}
	c := executionSessionConfig{Manifest: m, WaypointAuthorityID: "44444444444444444444444444444444", ScopeID: "33333333333333333333333333333333"}
	_, binding := executionSessionBindings(c)
	control := x.Control{Schema: x.ControlSchema, Operation: "status", Binding: binding}
	if control.Validate() != nil {
		t.Fatal("unit control fixture")
	}
	return control
}

func executionSessionUnitStage(t *testing.T, u *executionSessionUnit, c x.Control) {
	t.Helper()
	raw, e := assessment.Canonical(c)
	if e != nil {
		t.Fatal(e)
	}
	raw = append(raw, '\n')
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(raw)))
	if e = u.in.SetWriteDeadline(time.Now().Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	if e = u.out.SetReadDeadline(time.Now().Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	if e = executionSessionWrite(u.in, append(prefix[:], raw...)); e != nil {
		t.Fatal(e)
	}
	var ack [executionSessionACKBytes]byte
	if _, e = io.ReadFull(u.out, ack[:]); e != nil || string(ack[:8]) != "EXACK001" {
		t.Fatal("unit inert ACK", e)
	}
}

func TestExecutionSessionRejectsQueuedNextFrameDuringDispatch(t *testing.T) {
	u := newExecutionSessionUnit(t)
	control := executionSessionUnitControl(t)
	entered, release := make(chan struct{}), make(chan struct{})
	calls := 0
	u.s.dispatch = func(ctx context.Context, _ x.Control) (any, error) {
		calls++
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return x.Error{Schema: x.ErrorSchema, Code: x.ErrHeld.Error()}, nil
	}
	done := u.startLane()
	executionSessionUnitStage(t, u, control)
	if e := executionSessionWrite(u.in, []byte{1}); e != nil {
		t.Fatal(e)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("unit dispatch not reached")
	}
	raw, e := assessment.Canonical(control)
	if e != nil {
		t.Fatal(e)
	}
	raw = append(raw, '\n')
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(raw)))
	queued := append(prefix[:], raw...)
	queued = append(queued, 1)
	if e = executionSessionWrite(u.in, queued); e != nil {
		t.Fatal(e)
	}
	close(release)
	select {
	case e := <-done:
		if !errors.Is(e, executionSessionProtocolError) || calls != 1 {
			t.Fatal("pipeline accepted or re-dispatched")
		}
	case <-time.After(time.Second):
		t.Fatal("pipeline did not terminate")
	}
	u.noReplyBytes(t)
}

func TestExecutionSessionRejectsUnknownReplyWithoutRawOutput(t *testing.T) {
	u := newExecutionSessionUnit(t)
	u.s.dispatch = func(context.Context, x.Control) (any, error) { return []byte("PRIVATE_OUTPUT_POISON"), nil }
	done := u.startLane()
	executionSessionUnitStage(t, u, executionSessionUnitControl(t))
	if e := executionSessionWrite(u.in, []byte{1}); e != nil {
		t.Fatal(e)
	}
	select {
	case e := <-done:
		if !errors.Is(e, x.ErrInvalid) {
			t.Fatal("unknown reply escaped the closed encoder")
		}
	case <-time.After(time.Second):
		t.Fatal("unknown reply did not terminate")
	}
	u.noReplyBytes(t)
}

func TestExecutionSessionCancelBeforeDeadlineReset(t *testing.T) {
	t.Run("next-read-peer-open", func(t *testing.T) {
		u := newExecutionSessionUnit(t)
		u.s.stopTimer()
		u.s.beforeDeadline = func(_ *os.File, write bool) {
			if !write {
				u.s.abort(context.Canceled)
			}
		}
		_, _, e := u.s.readStage(&u.s.primary)
		if !errors.Is(e, context.Canceled) {
			t.Fatal("cancellation overwritten by next read deadline")
		}
	})
	t.Run("next-write-before-dispatch", func(t *testing.T) {
		u := newExecutionSessionUnit(t)
		u.s.stopTimer()
		writes, calls := 0, 0
		u.s.beforeDeadline = func(_ *os.File, write bool) {
			if write {
				writes++
				if writes == 2 {
					u.s.abort(context.Canceled)
				}
			}
		}
		u.s.dispatch = func(context.Context, x.Control) (any, error) {
			calls++
			return x.Error{Schema: x.ErrorSchema, Code: x.ErrHeld.Error()}, nil
		}
		done := u.startLane()
		executionSessionUnitStage(t, u, executionSessionUnitControl(t))
		if e := executionSessionWrite(u.in, []byte{1}); e != nil {
			t.Fatal(e)
		}
		select {
		case e := <-done:
			if !errors.Is(e, context.Canceled) || calls != 0 {
				t.Fatal("cancelled write setup reached dispatch")
			}
		case <-time.After(time.Second):
			t.Fatal("cancel-reset write did not terminate")
		}
	})
}

func TestExecutionSessionExpiryUsesEarliestRealGrant(t *testing.T) {
	now := time.Date(2026, 10, 20, 0, 0, 0, 900000000, time.UTC)
	grant := now.Add(17 * time.Second)
	if got := executionSessionExpiry(now, now.Add(time.Hour), grant, now.Add(time.Minute)); !got.Equal(grant.Truncate(time.Second)) {
		t.Fatal("grant ceiling replaced with thirty minutes")
	}
	if got := executionSessionExpiry(now, now.Add(time.Hour)); !got.Equal(now.Add(30 * time.Minute).Truncate(time.Second)) {
		t.Fatal("thirty-minute cap extended")
	}
	var m assessment.ManifestV2
	if x.DecodeClosed(executionSessionManifestSource, &m, 65536) != nil {
		t.Fatal("fixed manifest")
	}
	expiry := now.Add(time.Hour).Truncate(time.Second).Format(owneddata.SecondUTC)
	c := executionSessionConfig{Manifest: m, AuthorizationExpiry: expiry, ProjectExpiry: expiry, PolicyExpiry: expiry}
	digest := x.Digest(c.Manifest)
	if c.bindSessionDeadline(now) != nil {
		t.Fatal("bind launch deadline")
	}
	deadline, e := owneddata.Time(c.SessionDeadline, true)
	if e != nil || !deadline.Equal(now.Add(executionSessionLifetime).Truncate(time.Microsecond)) || digest != x.Digest(c.Manifest) {
		t.Fatal("launch bound reset or manifest changed")
	}
	p, scope, e := c.grants(deadline.Add(time.Second))
	if e != nil || p.PolicyExpiry.After(deadline) || !scope.Equal(p.PolicyExpiry) || p.PolicyExpiry.After(deadline.Add(time.Second)) {
		t.Fatal("expired strict cap unavailable for negative cleanup")
	}
}

func TestExecutionSessionPhaseProbeRejectsActualLeakedEndpoint(t *testing.T) {
	// Negative actual process: deliberately pass a forbidden endpoint as FD5.
	// The selector must reject it before consuming payload or producing output.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r, w, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	defer w.Close()
	id, e := openedIdentity(r, "pipe")
	if e != nil {
		t.Fatal(e)
	}
	meta, e := os.CreateTemp(t.TempDir(), "metadata")
	if e != nil {
		t.Fatal(e)
	}
	defer meta.Close()
	prior, e := os.CreateTemp(t.TempDir(), "prior")
	if e != nil {
		t.Fatal(e)
	}
	defer prior.Close()
	envelope := executionSessionPhaseEnvelope{Forbidden: make([]x.FilesystemIdentity, 9)}
	for i := range envelope.Forbidden {
		envelope.Forbidden[i] = id
	}
	raw, e := assessment.Canonical(envelope)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = meta.Write(raw); e != nil {
		t.Fatal(e)
	}
	if _, e = meta.Seek(0, 0); e != nil {
		t.Fatal(e)
	}
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExecutionPrivateSessionPhaseHelper$")
	cmd.Env = []string{"OUTFITTER_PRIVATE_SESSION_PHASE=1", "GOMAXPROCS=2"}
	cmd.ExtraFiles = []*os.File{meta, prior, r}
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	e = cmd.Run()
	var exit *exec.ExitError
	if !errors.As(e, &exit) || exit.ExitCode() != 73 {
		t.Fatal("actual forbidden session endpoint was not rejected")
	}
}
