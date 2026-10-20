package ledger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

// This fixture drives sanctioned journal events with real private files,
// flocks and in-process watch handles. Its clock/payload are synthetic; it
// does not run a provider, recipe, real sampler or operational cleanup check.
type ownershipLedgerFixture struct {
	store      *Store
	scope      x.ExecutionScope
	roots      AssessmentRoots
	guard      AssessmentBeginGuard
	now        AssessmentBeginClock
	clockErr   bool
	watch      *AssessmentOperation
	stop, done chan struct{}
}

func newOwnershipLedgerFixture(t *testing.T) *ownershipLedgerFixture {
	t.Helper()
	store, _ := freshExecutionAuthority(t)
	raw, e := os.ReadFile("../assessmentexec/testdata/fresh-scope.literal.json")
	if e != nil {
		t.Fatal(e)
	}
	var row x.ExecutionScope
	if e = json.Unmarshal(raw, &row); e != nil {
		t.Fatal(e)
	}
	data := filepath.Join(t.TempDir(), "data")
	if e = os.Mkdir(data, 0700); e != nil {
		t.Fatal(e)
	}
	data, e = filepath.EvalSymlinks(data)
	if e != nil {
		t.Fatal(e)
	}
	dataRoot, e := os.OpenRoot(data)
	if e != nil {
		t.Fatal(e)
	}
	stateRoot, e := os.OpenRoot(store.Dir)
	if e != nil {
		t.Fatal(e)
	}
	ownerRead, ownerWrite, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	watchRead, watchWrite, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	identity := func(f *os.File) x.FilesystemIdentity {
		st, e := f.Stat()
		if e != nil {
			t.Fatal(e)
		}
		id, e := FileIdentity(st)
		if e != nil {
			t.Fatal(e)
		}
		return id
	}
	st, e := os.Stat(data)
	if e != nil {
		t.Fatal(e)
	}
	rootID, e := FileIdentity(st)
	if e != nil {
		t.Fatal(e)
	}
	row.FilesystemIdentity = x.ExecutionFilesystemBinding{Root: rootID, OwnerRead: identity(ownerRead), WatchRead: identity(watchRead), WatchWrite: identity(watchWrite)}
	wall, _ := owneddata.Time(row.CreatedAt, true)
	f := &ownershipLedgerFixture{store: store, scope: row, roots: AssessmentRoots{Data: dataRoot, State: stateRoot}, now: AssessmentBeginClock{Wall: wall, TickMS: 0, Epoch: row.ClockEpoch}, stop: make(chan struct{}), done: make(chan struct{})}
	f.guard = func(d Data) (AssessmentBeginClock, error) {
		if f.clockErr {
			return f.now, errors.New("synthetic clock unavailable")
		}
		if d.Version != 7 || d.Authority.ID != row.BrokerAuthorityID {
			return f.now, x.ErrDenied
		}
		return f.now, nil
	}
	t.Cleanup(func() {
		store.assessmentWriteFault = nil
		store.assessmentOwnershipFault = nil
		close(f.stop)
		select {
		case <-f.done:
		case <-time.After(time.Second):
		}
		_ = ownerRead.Close()
		_ = ownerWrite.Close()
		_ = watchRead.Close()
		_ = watchWrite.Close()
		_ = dataRoot.Close()
		_ = stateRoot.Close()
	})
	setup, fresh, e := store.BeginAssessmentPrepareV7(row, f.roots, f.guard)
	if e != nil || !fresh {
		t.Fatal("prepare", e)
	}
	fs, e := setup.CreateSetup()
	if e != nil {
		t.Fatal("setup", e)
	}
	for _, part := range []struct {
		name string
		id   *x.FilesystemIdentity
	}{{"scope", fs.Scope}, {"writer", fs.WriterLease}, {"watch", fs.WatchLease}, {"execution", fs.ExecutionLease}, {"work", fs.WorkRoot}, {"output", fs.OutputRoot}} {
		f.scopeStep(t, x.ScopeIdentity, func(v *x.ExecutionScope) {
			switch part.name {
			case "scope":
				v.FilesystemIdentity.Scope = part.id
			case "writer":
				v.FilesystemIdentity.WriterLease = part.id
			case "watch":
				v.FilesystemIdentity.WatchLease = part.id
			case "execution":
				v.FilesystemIdentity.ExecutionLease = part.id
			case "work":
				v.FilesystemIdentity.WorkRoot = part.id
			case "output":
				v.FilesystemIdentity.OutputRoot = part.id
			}
		})
	}
	child, e := setup.ReserveWatch(watchWrite, f.guard)
	if e != nil {
		t.Fatal(e)
	}
	f.watch = child
	readyErr := make(chan error, 1)
	if e = child.LaunchWatch(func(op *AssessmentOperation) {
		readyErr <- op.PublishWatchReady(f.guard)
		<-f.stop
		_ = op.CloseAndCommitOwnership()
		close(f.done)
	}); e != nil {
		t.Fatal(e)
	}
	if e = setup.ReadAndValidateReady(context.Background()); e != nil {
		t.Fatal("ready", e)
	}
	if e = <-readyErr; e != nil {
		t.Fatal(e)
	}
	if e = setup.CloseAndCommitOwnership(); e != nil {
		t.Fatal(e)
	}
	f.refresh(t)
	c := f.control("ingest")
	_, writer, e := store.BeginAssessmentScopeRoleV7(c, "writer", f.roots, f.guard)
	if e != nil {
		t.Fatal(e)
	}
	if writer.AcquireExecutionSet() != nil || writer.FreshCurrent(f.guard) != nil {
		t.Fatal("writer locks")
	}
	content, id, e := writer.OpenContent()
	if e != nil {
		t.Fatal(e)
	}
	payload := []byte("SYNTHETIC\n")
	if _, e = content.Write(payload); e != nil {
		t.Fatal(e)
	}
	if e = content.Sync(); e != nil {
		t.Fatal(e)
	}
	f.scopeStep(t, x.ScopeContentIdentity, func(v *x.ExecutionScope) { v.FilesystemIdentity.Content = &id })
	if e = content.Close(); e != nil {
		t.Fatal(e)
	}
	f.scopeStep(t, x.ScopeContentClosed, func(v *x.ExecutionScope) { v.ContentCloseConfirmed = true })
	if e = writer.CloseAndCommitOwnership(); e != nil {
		t.Fatal(e)
	}
	f.scopeStep(t, x.ScopeUploadComplete, func(v *x.ExecutionScope) { v.UploadComplete = true; n := int64(len(payload)); v.SealedLengthBytes = &n })
	f.refresh(t)
	c = f.control("seal")
	_, seal, e := store.BeginAssessmentScopeRoleV7(c, "seal", f.roots, f.guard)
	if e != nil {
		t.Fatal(e)
	}
	if seal.AcquireExecutionSet() != nil || seal.FreshCurrent(f.guard) != nil {
		t.Fatal("seal locks")
	}
	if e = seal.CloseAndCommitOwnership(); e != nil {
		t.Fatal(e)
	}
	f.scopeStep(t, x.ScopeSeal, func(v *x.ExecutionScope) { v.State = "sealed" })
	f.refresh(t)
	return f
}
func (f *ownershipLedgerFixture) refresh(t *testing.T) {
	t.Helper()
	if e := f.store.View(func(d Data) error { f.scope = d.AssessmentExecution.Scopes[f.scope.ScopeID]; return nil }); e != nil {
		t.Fatal(e)
	}
}
func (f *ownershipLedgerFixture) control(op string) x.Control {
	gen := f.scope.Generation
	epoch := f.scope.WriterEpoch
	owner := f.scope.AdmittingOwnerEpoch
	return x.Control{Schema: x.ControlSchema, Operation: op, Binding: f.scope.Binding(), ExpectedGeneration: &gen, AdmittingOwnerEpoch: &owner, WriterEpoch: &epoch}
}
func (f *ownershipLedgerFixture) scopeStep(t *testing.T, event x.Event, fn func(*x.ExecutionScope)) {
	t.Helper()
	e := f.store.Update(func(d *Data) error {
		s := d.AssessmentExecution.Scopes[f.scope.ScopeID]
		next, e := x.AdvanceScope(s, event, owneddata.Stamp(f.now.Wall), f.now.TickMS, fn)
		if e == nil {
			d.AssessmentExecution.Scopes[s.ScopeID] = next
		}
		return e
	})
	if e != nil {
		t.Fatal(event, e)
	}
}
func (f *ownershipLedgerFixture) claim(t *testing.T) (x.AssessmentAttempt, *AssessmentOperation) {
	t.Helper()
	f.refresh(t)
	c := f.control("execute")
	c.WriterEpoch = nil
	slot := f.scope.Slots[0]
	nonce := strings.Repeat("a", 32)
	c.Slot = &slot
	c.HandoffNonce = &nonce
	a, op, fresh, e := f.store.BeginAssessmentAttemptV7(c, f.roots, f.guard)
	if e != nil || !fresh {
		t.Fatal("claim", e)
	}
	return a, op
}
func (f *ownershipLedgerFixture) finish(t *testing.T, a x.AssessmentAttempt) {
	t.Helper()
	if e := f.store.Update(func(d *Data) error {
		v := d.AssessmentExecution.Attempts[a.Slot.AttemptID]
		next, e := x.AdvanceAttempt(v, d.AssessmentExecution.Scopes[v.Binding.ScopeID], x.AttemptFinish, v.WallHighWater, v.TickHighWaterMS, func(n *x.AssessmentAttempt) { n.State = "finishing" })
		if e == nil {
			d.AssessmentExecution.Attempts[a.Slot.AttemptID] = next
		}
		return e
	}); e != nil {
		t.Fatal(e)
	}
}

func TestExecutionOwnershipPostClosePublicationACKIsNotPhysicalFailure(t *testing.T) {
	for _, point := range []string{"before_rename", "after_rename", "before_directory_sync", "after_directory_sync"} {
		t.Run(point, func(t *testing.T) {
			f := newOwnershipLedgerFixture(t)
			a, op := f.claim(t)
			if op.AcquireExecutionSet() != nil || op.FreshCurrent(f.guard) != nil {
				t.Fatal("acquire")
			}
			f.finish(t, a)
			fired := false
			f.store.assessmentWriteFault = func(p string) error {
				if p == point && !fired {
					fired = true
					return errors.New("synthetic final publication ACK loss")
				}
				return nil
			}
			if !errors.Is(op.CloseAndCommitOwnership(), x.ErrHeld) || !fired {
				t.Fatal("lost ACK not held")
			}
			for _, h := range op.state.handles {
				if !h.attempted || h.result != nil {
					t.Fatal("physical release unknown")
				}
				if h.file != nil {
					if _, e := h.file.Stat(); !errors.Is(e, os.ErrClosed) {
						t.Fatal("actual descriptor remained open", e)
					}
				}
			}
			var stored x.AssessmentAttempt
			if e := f.store.View(func(d Data) error { stored = d.AssessmentExecution.Attempts[a.Slot.AttemptID]; return nil }); e != nil {
				t.Fatal(e)
			}
			if stored.OwnershipCloseUncertain || stored.OwnershipCloseConfirmed != (point != "before_rename") {
				t.Fatal("publication reclassified actual successful close", point)
			}
			before, _ := os.ReadFile(filepath.Join(f.store.Dir, "ledger.json"))
			copy := *op
			if !errors.Is(copy.CloseAndCommitOwnership(), x.ErrHeld) || copy.AcquireExecutionSet() == nil || copy.FreshCurrent(f.guard) == nil {
				t.Fatal("lost ACK operation resumed")
			}
			after, _ := os.ReadFile(filepath.Join(f.store.Dir, "ledger.json"))
			if !bytes.Equal(before, after) {
				t.Fatal("cached HELD republished")
			}
			f.store.assessmentWriteFault = nil
			reopened, e := Open(f.store.Dir, false)
			if e != nil {
				t.Fatal(e)
			}
			if e = reopened.View(func(d Data) error {
				r := d.AssessmentExecution.Attempts[a.Slot.AttemptID]
				if r.OwnershipCloseConfirmed != stored.OwnershipCloseConfirmed || r.OwnershipCloseUncertain {
					t.Fatal("restart invented close origin")
				}
				return nil
			}); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestExecutionOwnershipActualCloseFailureWithLostNegativePublication(t *testing.T) {
	for _, kind := range []string{"execution.lock", "assessment-execution", "writer"} {
		t.Run(kind, func(t *testing.T) {
			f := newOwnershipLedgerFixture(t)
			a, op := f.claim(t)
			if op.AcquireExecutionSet() != nil || op.FreshCurrent(f.guard) != nil {
				t.Fatal("acquire")
			}
			f.finish(t, a)
			name := kind
			if kind != "execution.lock" {
				name = "owned-" + f.scope.ScopeID + "-" + kind + ".lock"
			}
			calls := 0
			f.store.assessmentOwnershipFault = func(p string) error {
				if p == "ownership-close:"+name {
					calls++
					return errors.New("synthetic Close refusal before physical close")
				}
				return nil
			}
			f.store.assessmentWriteFault = func(p string) error {
				if p == "before_rename" {
					return errors.New("synthetic negative publication loss")
				}
				return nil
			}
			if !errors.Is(op.CloseAndCommitOwnership(), x.ErrHeld) || calls != 1 {
				t.Fatal("close failure lost or retried", calls)
			}
			for _, h := range op.state.handles {
				if !h.attempted {
					t.Fatal("remaining handle not attempted")
				}
				if h.file != nil && h.label != name {
					if _, e := h.file.Stat(); !errors.Is(e, os.ErrClosed) {
						t.Fatal("remaining descriptor retained", e)
					}
				}
			}
			before, _ := os.ReadFile(filepath.Join(f.store.Dir, "ledger.json"))
			copy := *op
			_ = copy.CloseAndCommitOwnership()
			if calls != 1 {
				t.Fatal("copied failed Close retried")
			}
			after, _ := os.ReadFile(filepath.Join(f.store.Dir, "ledger.json"))
			if !bytes.Equal(before, after) {
				t.Fatal("failed negative publication replayed")
			}
			f.store.assessmentWriteFault = nil
			f.store.assessmentOwnershipFault = nil
			reopened, e := Open(f.store.Dir, false)
			if e != nil {
				t.Fatal(e)
			}
			if e = reopened.View(func(d Data) error {
				r := d.AssessmentExecution.Attempts[a.Slot.AttemptID]
				if r.OwnershipCloseConfirmed || r.SerialClear() || CanClaimNextSlotV7(d.AssessmentExecution, d.Runs, d.NativeRuns) == nil {
					t.Fatal("failed physical close escaped hold")
				}
				return nil
			}); e != nil {
				t.Fatal(e)
			}
			// Test teardown only: abandoned synthetic handles are not used as business
			// completion evidence. The operation itself never retries their Close.
			t.Cleanup(func() {
				for _, h := range op.state.handles {
					if h.file != nil && h.result != nil {
						_ = h.file.Close()
					}
				}
			})
		})
	}
}

func TestExecutionOwnershipEveryLeasePartialAndReleaseFailure(t *testing.T) {
	for _, kind := range []string{"execution.lock", "assessment-execution", "writer"} {
		for _, mode := range []string{"after-open", "unlock", "close", "both"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				f := newOwnershipLedgerFixture(t)
				a, op := f.claim(t)
				name := kind
				if kind != "execution.lock" {
					name = "owned-" + f.scope.ScopeID + "-" + kind + ".lock"
				}
				calls := map[string]int{}
				f.store.assessmentOwnershipFault = func(p string) error {
					hit := mode == "after-open" && p == "ownership-after-open:"+name || mode == "unlock" && p == "ownership-unlock:"+name || mode == "close" && p == "ownership-close:"+name || mode == "both" && (p == "ownership-unlock:"+name || p == "ownership-close:"+name)
					if hit {
						calls[p]++
						return errors.New("synthetic exact lease failure")
					}
					return nil
				}
				acquired := op.AcquireExecutionSet()
				if mode == "after-open" {
					if acquired == nil {
						t.Fatal("partial constructor succeeded")
					}
					if e := op.Abort(owneddata.Stamp(f.now.Wall), f.now.TickMS); e != nil {
						t.Fatal(e)
					}
				} else {
					if acquired != nil || op.FreshCurrent(f.guard) != nil {
						t.Fatal("acquire", acquired)
					}
				}
				f.finish(t, a)
				e := op.CloseAndCommitOwnership()
				if mode != "after-open" && !errors.Is(e, x.ErrHeld) {
					t.Fatal("release uncertainty not held", e)
				}
				for _, h := range op.state.handles {
					if !h.attempted {
						t.Fatal("known handle not attempted")
					}
					if h.result == nil && h.file != nil {
						if _, e := h.file.Stat(); !errors.Is(e, os.ErrClosed) {
							t.Fatal("actual descriptor not closed", e)
						}
					}
				}
				before, _ := os.ReadFile(filepath.Join(f.store.Dir, "ledger.json"))
				copy := *op
				_ = copy.CloseAndCommitOwnership()
				after, _ := os.ReadFile(filepath.Join(f.store.Dir, "ledger.json"))
				if !bytes.Equal(before, after) {
					t.Fatal("copy repeated a final mutation")
				}
				for point, n := range calls {
					if n != 1 {
						t.Fatal("release/partial repeated", point, n)
					}
				}
				if e = f.store.View(func(d Data) error {
					r := d.AssessmentExecution.Attempts[a.Slot.AttemptID]
					if r.SerialClear() || !r.AccountingUnresolved || r.Compute.ChargedCPUSeconds != 0 {
						t.Fatal("partial or uncertain release cleared full reservation")
					}
					if mode != "after-open" && (!r.OwnershipCloseUncertain || r.OwnershipCloseConfirmed) {
						t.Fatal("uncertain release became positive")
					}
					return nil
				}); e != nil {
					t.Fatal(e)
				}
				t.Cleanup(func() {
					for _, h := range op.state.handles {
						if h.file != nil && h.result != nil {
							_ = h.file.Close()
						}
					}
				})
			})
		}
	}
}

func TestExecutionOwnershipStickyGateAndDirectOldCleanupGuard(t *testing.T) {
	f := newOwnershipLedgerFixture(t)
	a, op := f.claim(t)
	if e := f.store.Update(func(d *Data) error {
		v := d.AssessmentExecution.Attempts[a.Slot.AttemptID]
		next, e := x.AdvanceAttempt(v, d.AssessmentExecution.Scopes[v.Binding.ScopeID], x.AttemptOwnershipUncertain, v.WallHighWater, v.TickHighWaterMS, func(n *x.AssessmentAttempt) { n.OwnershipCloseUncertain = true })
		if e == nil {
			d.AssessmentExecution.Attempts[a.Slot.AttemptID] = next
		}
		return e
	}); e != nil {
		t.Fatal(e)
	}
	if op.AcquireExecutionSet() != nil {
		t.Fatal("owned descriptor set")
	}
	if op.FreshCurrent(f.guard) == nil {
		t.Fatal("own sticky uncertainty ignored")
	}
	before, _ := os.ReadFile(filepath.Join(f.store.Dir, "ledger.json"))
	if e := f.store.Update(func(d *Data) error {
		v := d.AssessmentExecution.Attempts[a.Slot.AttemptID]
		next, e := x.AdvanceAttempt(v, d.AssessmentExecution.Scopes[v.Binding.ScopeID], x.AttemptReader, v.WallHighWater, v.TickHighWaterMS, func(n *x.AssessmentAttempt) { n.ReaderStarted = true })
		if e == nil {
			d.AssessmentExecution.Attempts[a.Slot.AttemptID] = next
		}
		return e
	}); e == nil {
		t.Fatal("forward reader after sticky ownership uncertainty")
	}
	after, _ := os.ReadFile(filepath.Join(f.store.Dir, "ledger.json"))
	if !bytes.Equal(before, after) {
		t.Fatal("refused forward mutation wrote journal")
	}
	f.finish(t, a)
	_ = op.Abort(owneddata.Stamp(f.now.Wall), f.now.TickMS)
	_ = op.CloseAndCommitOwnership()
	if e := f.watch.CloseAndCommitOwnership(); e != nil {
		t.Fatal(e)
	}
	f.refresh(t)
	before, _ = os.ReadFile(filepath.Join(f.store.Dir, "ledger.json"))
	epoch := strings.Repeat("f", 32)
	e := f.store.Update(func(d *Data) error {
		s := d.AssessmentExecution.Scopes[f.scope.ScopeID]
		next, e := x.AdvanceScope(s, x.ScopeCleanupBegin, s.WallHighWater, s.TickHighWaterMS, func(n *x.ExecutionScope) { n.CleanupOwnerEpoch = &epoch })
		if e == nil {
			d.AssessmentExecution.Scopes[s.ScopeID] = next
		}
		return e
	})
	if !errors.Is(e, x.ErrHeld) {
		t.Fatal("generic callback consumed cleanup epoch with non-SerialClear old attempt", e)
	}
	after, _ = os.ReadFile(filepath.Join(f.store.Dir, "ledger.json"))
	if !bytes.Equal(before, after) {
		t.Fatal("active cleanup changed bytes")
	}
}

func TestExecutionCleanupObservationUsesCurrentClockOrRemainsUndated(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "advanced", true: "unavailable"}[unknown], func(t *testing.T) {
			f := newOwnershipLedgerFixture(t)
			if e := f.watch.CloseAndCommitOwnership(); e != nil {
				t.Fatal(e)
			}
			f.scopeStep(t, x.ScopeDeny, func(v *x.ExecutionScope) {
				v.DenyLatched = true
				v.ExecutionDenyLatched = true
				v.State = "cleaning"
				v.Reason = "cancelled"
			})
			f.refresh(t)
			_, op, e := f.store.BeginAssessmentScopeRoleV7(x.Control{Binding: f.scope.Binding()}, "cleanup", f.roots, f.guard)
			if e != nil {
				t.Fatal(e)
			}
			if op.AcquireExecutionSet() != nil || op.FreshCurrent(f.guard) != nil {
				t.Fatal("cleanup acquisition")
			}
			prior := f.scope.WallHighWater
			f.now.Wall = f.now.Wall.Add(5 * time.Second)
			f.now.TickMS += 5000
			f.clockErr = unknown
			e = op.RemoveOwnedScopeAndClose()
			if unknown && !errors.Is(e, x.ErrHeld) || !unknown && e != nil {
				t.Fatal("cleanup clock result", e)
			}
			f.refresh(t)
			if !f.scope.CleanupOwnershipCloseConfirmed {
				t.Fatal("clock error lost truthful descriptor close")
			}
			found := false
			for _, o := range f.scope.Observations {
				if o.OwnedRemoval == "observed_absent" {
					found = true
					if unknown || o.ObservedAt != owneddata.Stamp(f.now.Wall) || o.ObservedAt <= prior {
						t.Fatal("historical clock advertised as actual observation", o.ObservedAt)
					}
				}
			}
			if found == unknown {
				t.Fatal("unknown clock fabricated dated absence or advanced clock lost it")
			}
			if _, e := f.roots.Data.Lstat(f.scope.ScopeID); !os.IsNotExist(e) {
				t.Fatal("actual owned removal missing", e)
			}
		})
	}
}
