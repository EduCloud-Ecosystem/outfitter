package broker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
)

func TestExecutionOwnershipPositiveLifecycleAndCleanup(t *testing.T) {
	f := newExecutionFixture(t)
	status := f.seal(t)
	scope, _, e := f.b.assessmentRead(f.c.Binding)
	if e != nil || !scope.PrepareOwnershipCloseConfirmed || !scope.WriterOwnershipCloseConfirmed || !scope.SealStarted || !scope.SealOwnershipCloseConfirmed || scope.WatchOwnershipCloseConfirmed {
		t.Fatal("real role completion order", e, scope.State)
	}
	c := executionControl(f, status, 0)
	result, e := f.b.RunAssessmentOnce(context.Background(), c)
	if e != nil || result.Code != "OK" {
		t.Fatal("execution", e, result.Code)
	}
	_, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil || !a.SerialClear() || !a.OwnershipCloseConfirmed || a.OwnershipCloseUncertain || a.Compute.ChargedCPUSeconds != 30 {
		t.Fatal("whole owner did not close", e, a.State)
	}
	reason := "cancelled"
	cancel := x.Control{Schema: x.ControlSchema, Operation: "cancel", Binding: c.Binding, Reason: &reason}
	_, e = f.b.AssessmentData(context.Background(), cancel, nil)
	if e != nil && !errors.Is(e, x.ErrHeld) {
		t.Fatal(e)
	}
	select {
	case <-f.watchDone:
	case <-time.After(3 * time.Second):
		t.Fatal("watch owner did not retire")
	}
	reconcile := x.Control{Schema: x.ControlSchema, Operation: "reconcile", Binding: c.Binding}
	if _, e = f.b.AssessmentData(context.Background(), reconcile, nil); e != nil {
		t.Fatal("quiescent cleanup", e)
	}
	scope, _, e = f.b.assessmentRead(c.Binding)
	if e != nil || !scope.WatchOwnershipCloseConfirmed || scope.CleanupOwnerEpoch == nil || !scope.CleanupOwnershipCloseConfirmed || scope.LeaseCloseUncertain {
		t.Fatal("cleanup ownership", e)
	}
	removed := false
	for _, o := range scope.Observations {
		if o.OwnedRemoval == "observed_absent" && o.WritersDrained == "observed" && o.ReadersDrained == "observed" && o.CollectorDrained == "observed" {
			removed = true
		}
	}
	if !removed {
		t.Fatal("no actual removal projection")
	}
	if _, e := f.b.assessment.owned.root.Lstat(scope.ScopeID); !os.IsNotExist(e) {
		t.Fatal("owned scope retained", e)
	}
	before, e := os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.b.AssessmentData(context.Background(), reconcile, nil); e != nil {
		t.Fatal("completed metadata repeat", e)
	}
	after, _ := os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
	if !bytes.Equal(before, after) {
		t.Fatal("completed cleanup rewrote authority")
	}
}

func TestExecutionOwnershipStoreRefusesForgedCompletionAndCopiesShareOnce(t *testing.T) {
	f := newExecutionFixture(t)
	status := f.seal(t)
	c := executionControl(f, status, 0)
	a, owner, fresh, e := f.b.assessmentBeginAttempt(c)
	if e != nil || !fresh {
		t.Fatal(e)
	}
	if owner.AcquireExecutionSet() != nil || owner.FreshCurrent(f.b.assessmentBeginGuard(c.Binding, false)) != nil {
		t.Fatal("owner acquisition")
	}
	defer owner.CloseAndCommitOwnership()
	a, e = f.b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptFinish, func(v *x.AssessmentAttempt) error { v.State = "finishing"; return nil })
	if e != nil {
		t.Fatal(e)
	}
	before, _ := os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
	e = f.b.Store.Update(func(d *ledger.Data) error {
		next, e := x.AdvanceAttempt(a, d.AssessmentExecution.Scopes[c.Binding.ScopeID], x.AttemptOwnershipClosed, a.WallHighWater, a.TickHighWaterMS, func(v *x.AssessmentAttempt) { v.OwnershipCloseConfirmed = true })
		if e != nil {
			return e
		}
		d.AssessmentExecution.Attempts[c.Slot.AttemptID] = next
		return nil
	})
	if !errors.Is(e, x.ErrDenied) {
		t.Fatal("generic Store callback minted physical completion", e)
	}
	after, _ := os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
	if !bytes.Equal(before, after) {
		t.Fatal("forged close changed journal")
	}
	copy := *owner
	if copy.AcquireExecutionSet() == nil {
		t.Fatal("copy reacquired leases")
	}
	if e = owner.CloseAndCommitOwnership(); e != nil {
		t.Fatal("actual checked closure", e)
	}
	_, a, e = f.b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil || !a.OwnershipCloseConfirmed || a.OwnershipCloseUncertain {
		t.Fatal("actual completion missing", e)
	}
	before, _ = os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
	if e = copy.CloseAndCommitOwnership(); e != nil {
		t.Fatal("copy did not share cached close", e)
	}
	after, _ = os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
	if !bytes.Equal(before, after) {
		t.Fatal("copied close republished/retried")
	}
}

func TestExecutionPrelaunchWatchFailureClosesOriginalOwnersOnce(t *testing.T) {
	f := newExecutionFixture(t)
	var captured *ledger.AssessmentOperation
	f.b.assessment.spawnWatch = func(owner *ledger.AssessmentOperation) error {
		captured = owner
		return errors.New("synthetic prelaunch refusal")
	}
	if _, e := f.b.AssessmentData(context.Background(), f.c, nil); e == nil {
		t.Fatal("failed launch succeeded")
	}
	scope, _, e := f.b.assessmentRead(f.c.Binding)
	if e != nil || !scope.DenyLatched || !scope.PrepareOwnershipCloseConfirmed || !scope.WatchOwnershipCloseConfirmed {
		t.Fatal("unlaunched known handles not closed", e)
	}
	if captured == nil || captured.LaunchWatch(func(*ledger.AssessmentOperation) { t.Error("aborted child launched") }) == nil {
		t.Fatal("aborted handoff reusable")
	}
	copy := *captured
	if e = copy.CloseAndCommitOwnership(); e != nil {
		t.Fatal("cached child close", e)
	}
}

func TestExecutionPartialWatchConstructionClosesUnlaunchedHandles(t *testing.T) {
	for _, point := range []string{"watch-after-dup", "watch-after-pipe"} {
		t.Run(point, func(t *testing.T) {
			f := newExecutionFixture(t)
			f.b.assessment.owned.fault = func(p string) error {
				if p == point {
					return errors.New("synthetic known partial watch failure")
				}
				return nil
			}
			if _, e := f.b.AssessmentData(context.Background(), f.c, nil); e == nil {
				t.Fatal("partial watch succeeded")
			}
			scope, _, e := f.b.assessmentRead(f.c.Binding)
			if e != nil || !scope.DenyLatched || !scope.WatchOwnershipCloseConfirmed || !scope.PrepareOwnershipCloseConfirmed || scope.LeaseCloseUncertain {
				t.Fatal("known partial handles not definitively closed", e, scope.State)
			}
			if livePipe(f.b.assessment.owned.watchRead, scope.FilesystemIdentity.WatchRead) != "lost" {
				t.Fatal("known duplicate or parent write endpoint still open")
			}
		})
	}
}

func TestExecutionLaunchedWatchIsNotClosedByAbortingParent(t *testing.T) {
	f := newExecutionFixture(t)
	launch := f.b.assessment.spawnWatch
	launched, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	f.b.assessment.spawnWatch = func(owner *ledger.AssessmentOperation) error {
		// Consume the actual one-shot launch, then hold the launched owner before
		// readiness. Parent failure must not close any child-owned handle.
		e := owner.LaunchWatch(func(op *ledger.AssessmentOperation) {
			close(launched)
			<-release
			_ = op.CloseAndCommitOwnership()
			close(done)
		})
		if e != nil {
			return e
		}
		<-launched
		_ = launch // retain original fixture boundary without invoking it twice
		return errors.New("synthetic parent failure after actual child launch")
	}
	if _, e := f.b.AssessmentData(context.Background(), f.c, nil); e == nil {
		t.Fatal("failed parent succeeded")
	}
	scope, _, e := f.b.assessmentRead(f.c.Binding)
	if e != nil || !scope.DenyLatched || scope.WatchOwnershipCloseConfirmed {
		t.Fatal("parent closed launched child", e)
	}
	once.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("test child did not drain")
	}
}

func TestExecutionOwnedMarkerRequiresPrivateSingleLinkRegularFile(t *testing.T) {
	for _, kind := range []string{"public-mode", "hardlink", "directory"} {
		t.Run(kind, func(t *testing.T) {
			f := newExecutionFixture(t)
			status := f.seal(t)
			c := executionControl(f, status, 0)
			marker := filepath.Join(f.b.assessment.owned.root.Name(), c.Binding.ScopeID, "scope-owner.json")
			switch kind {
			case "public-mode":
				if e := os.Chmod(marker, 0644); e != nil {
					t.Fatal(e)
				}
			case "hardlink":
				if e := os.Link(marker, filepath.Join(f.b.Store.Dir, "marker-hardlink")); e != nil {
					t.Fatal(e)
				}
			case "directory":
				if e := os.Remove(marker); e != nil {
					t.Fatal(e)
				}
				if e := os.Mkdir(marker, 0700); e != nil {
					t.Fatal(e)
				}
			}
			if _, e := f.b.RunAssessmentOnce(context.Background(), c); e == nil {
				t.Fatal("invalid owned marker accepted")
			}
			_, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
			if e != nil || !a.AccountingUnresolved || a.Compute.ChargedCPUSeconds != 0 || a.SerialClear() || f.p.starts != 0 {
				t.Fatal("marker refusal dispatched or released unknown ownership", e)
			}
		})
	}
}

func TestExecutionWatchLeasePartialAndReleaseFailures(t *testing.T) {
	for _, mode := range []string{"after-open", "unlock", "close", "both"} {
		t.Run(mode, func(t *testing.T) {
			f := newExecutionFixture(t)
			name := "owned-" + f.c.Binding.ScopeID + "-watch.lock"
			calls := map[string]int{}
			fault := func(p string) error {
				hit := mode == "after-open" && p == "ownership-after-open:"+name || mode == "unlock" && p == "ownership-unlock:"+name || mode == "close" && p == "ownership-close:"+name || mode == "both" && (p == "ownership-unlock:"+name || p == "ownership-close:"+name)
				if hit {
					calls[p]++
					return errors.New("synthetic watch lease uncertainty")
				}
				return nil
			}
			if mode == "after-open" {
				f.b.assessment.owned.fault = fault
				if _, e := f.b.AssessmentData(context.Background(), f.c, nil); e == nil {
					t.Fatal("watch partial open succeeded")
				}
			} else {
				_ = f.seal(t)
				f.b.assessment.owned.fault = fault
				reason := "cancelled"
				c := x.Control{Schema: x.ControlSchema, Operation: "cancel", Binding: f.c.Binding, Reason: &reason}
				_, _ = f.b.AssessmentData(context.Background(), c, nil)
			}
			select {
			case <-f.watchDone:
			case <-time.After(3 * time.Second):
				t.Fatal("failed watch did not retire")
			}
			scope, _, e := f.b.assessmentRead(f.c.Binding)
			if e != nil || !scope.LeaseCloseUncertain || scope.WatchOwnershipCloseConfirmed || scope.CleanupOwnerEpoch != nil {
				t.Fatal("unknown watch ownership escaped hold", e)
			}
			if len(calls) == 0 {
				t.Fatal("fault seam not reached")
			}
			for point, n := range calls {
				if n != 1 {
					t.Fatal("watch release retried", point, n)
				}
			}
			before, _ := os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
			c := x.Control{Schema: x.ControlSchema, Operation: "reconcile", Binding: f.c.Binding}
			if _, e = f.b.AssessmentData(context.Background(), c, nil); !errors.Is(e, x.ErrHeld) {
				t.Fatal("uncertain watch cleanup admitted", e)
			}
			after, _ := os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
			if !bytes.Equal(before, after) {
				t.Fatal("uncertain watch poll rewrote journal")
			}
		})
	}
}

type executionCleanupGateStorage struct {
	executionTestStorage
	entered, release chan struct{}
}

func (s *executionCleanupGateStorage) Remove(ctx context.Context, _ x.ExecutionScope) error {
	close(s.entered)
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestExecutionCleanupLateUncertaintyCannotPublishAbsence(t *testing.T) {
	for _, stage := range []string{"after-acquire", "after-removal"} {
		for _, kind := range []string{"reader", "collector", "transport", "ownership"} {
			t.Run(stage+"/"+kind, func(t *testing.T) {
				f := newExecutionFixture(t)
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				defer once.Do(func() { close(release) })
				if stage == "after-acquire" {
					f.b.assessment.storage = &executionCleanupGateStorage{entered: entered, release: release}
				} else {
					f.b.assessment.owned.fault = func(p string) error {
						if p == "ownership-after-removal" {
							close(entered)
							<-release
						}
						return nil
					}
				}
				// The watcher copies these fixture dependencies at prepare launch.
				// Install the barrier first so either original owner reaches it.
				status := f.seal(t)
				c := executionControl(f, status, 0)
				if _, e := f.b.RunAssessmentOnce(context.Background(), c); e != nil {
					t.Fatal(e)
				}
				reason := "cancelled"
				cancel := x.Control{Schema: x.ControlSchema, Operation: "cancel", Binding: c.Binding, Reason: &reason}
				cancelDone := make(chan error, 1)
				go func() { _, e := f.b.AssessmentData(context.Background(), cancel, nil); cancelDone <- e }()
				select {
				case <-entered:
				case <-time.After(3 * time.Second):
					scope, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
					t.Fatal("cleanup boundary not reached", e, scope.State, scope.CleanupOwnerEpoch, scope.CleanupOwnershipCloseConfirmed, scope.WatchOwnershipCloseConfirmed, a.State, a.SerialClear())
				}
				e := f.b.Store.Update(func(d *ledger.Data) error {
					a := d.AssessmentExecution.Attempts[c.Slot.AttemptID]
					event := x.AttemptReaderUncertain
					switch kind {
					case "collector":
						event = x.AttemptCollectorUncertain
					case "transport":
						event = x.AttemptPhaseSafety
					case "ownership":
						event = x.AttemptOwnershipUncertain
					}
					next, e := x.AdvanceAttempt(a, d.AssessmentExecution.Scopes[c.Binding.ScopeID], event, a.WallHighWater, a.TickHighWaterMS, func(v *x.AssessmentAttempt) {
						switch kind {
						case "reader":
							v.ReaderCloseUncertain = true
						case "collector":
							v.CollectorCloseUncertain = true
						case "transport":
							v.PhaseRecords.Workload.TransportCloseUncertain = true
						case "ownership":
							v.OwnershipCloseUncertain = true
						}
					})
					if e == nil {
						d.AssessmentExecution.Attempts[c.Slot.AttemptID] = next
					}
					return e
				})
				if e != nil {
					t.Fatal("late uncertainty must stay representable", e)
				}
				once.Do(func() { close(release) })
				select {
				case <-cancelDone:
				case <-time.After(3 * time.Second):
					t.Fatal("cancel cleanup did not drain")
				}
				select {
				case <-f.watchDone:
				case <-time.After(3 * time.Second):
					t.Fatal("watch cleanup did not drain")
				}
				scope, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
				if e != nil || a.SerialClear() || a.AccountingUnresolved || a.Compute.ChargedCPUSeconds != 30 || a.State != "terminal" {
					t.Fatal("late uncertainty erased charge/history or restored clearance", e)
				}
				for _, o := range scope.Observations {
					if o.OwnedRemoval == "observed_absent" {
						t.Fatal("late uncertainty produced new all-drained positive")
					}
				}
				_, se := f.b.assessment.owned.root.Lstat(scope.ScopeID)
				if stage == "after-acquire" && se != nil || stage == "after-removal" && !os.IsNotExist(se) {
					t.Fatal("physical removal boundary mismatch", stage, se)
				}
			})
		}
	}
}

type executionUnknownStartProvider struct{ *executionTestProvider }

func (p *executionUnknownStartProvider) Start(ctx context.Context, id x.DockerResourceIdentity, in io.Reader, prior []byte, out, stderr io.Writer) (assessmentCompletion, error) {
	r, e := p.executionTestProvider.Start(ctx, id, in, prior, out, stderr)
	r.StartedAt = nil
	return r, e
}
func TestExecutionStartTimestampCannotBecomeSeparateLateEvent(t *testing.T) {
	f := newExecutionFixture(t)
	status := f.seal(t)
	f.b.assessment.provider = &executionUnknownStartProvider{f.p}
	c := executionControl(f, status, 0)
	var checked atomic.Bool
	f.b.assessment.owned.fault = func(point string) error {
		if point != "before-journal:"+string(x.AttemptPhaseSafety) || checked.Load() {
			return nil
		}
		_, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
		if e != nil || a.PhaseRecords.Workload == nil || !a.PhaseRecords.Workload.StartedObserved || a.PhaseRecords.Workload.StartedAt != nil {
			return nil
		}
		if checked.Swap(true) {
			return nil
		}
		before, _ := os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
		e = f.b.Store.Update(func(d *ledger.Data) error {
			v := d.AssessmentExecution.Attempts[c.Slot.AttemptID]
			next, e := x.AdvanceAttempt(v, d.AssessmentExecution.Scopes[v.Binding.ScopeID], x.AttemptPhaseSafety, v.WallHighWater, v.TickHighWaterMS, func(n *x.AssessmentAttempt) { stamp := v.WallHighWater; n.PhaseRecords.Workload.StartedAt = &stamp })
			if e == nil {
				d.AssessmentExecution.Attempts[c.Slot.AttemptID] = next
			}
			return e
		})
		if e == nil {
			t.Fatal("late timestamp spent a second event after already-observed start")
		}
		after, _ := os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
		if !bytes.Equal(before, after) {
			t.Fatal("refused split event changed journal")
		}
		return nil
	}
	if _, e := f.b.RunAssessmentOnce(context.Background(), c); e != nil {
		t.Fatal(e)
	}
	_, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil || !checked.Load() || !a.SerialClear() || a.PhaseRecords.Workload.StartedAt != nil || a.PhaseRecords.Checker.StartedAt != nil {
		t.Fatal("missing time was not preserved", e)
	}
}
