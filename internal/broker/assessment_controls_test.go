package broker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

// The metadata test helper never reconstructs operation authority from a claim.
func (b *Broker) assessmentClaim(c x.Control) (x.AssessmentAttempt, bool, error) {
	a, _, fresh, e := b.assessmentBeginAttempt(c)
	return a, fresh, e
}

func executionControl(f *executionFixture, s x.Status, index int) x.Control {
	gen := s.Generation
	slot := f.c.Manifest.Reservations[index]
	nonce := strings.Repeat("e", 32)
	return x.Control{Schema: x.ControlSchema, Operation: "execute", Binding: f.c.Binding, ExpectedGeneration: &gen, AdmittingOwnerEpoch: f.c.AdmittingOwnerEpoch, Slot: &slot, HandoffNonce: &nonce}
}
func executionWait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("bounded protocol barrier was not reached")
	}
}

// The barrier is immediately before the authority snapshot. The original
// watcher sampled before this barrier; the corrected watcher samples inside
// the snapshot. Ingest commits newer high-water while the watcher is blocked.
func TestExecutionWatchConcurrentCommitIsNotClockRollback(t *testing.T) {
	f := newExecutionFixture(t)
	sampled, release, processed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	f.b.assessment.owned.fault = func(point string) error {
		if point == "watch-before-snapshot" {
			switch calls.Add(1) {
			case 1:
				close(sampled)
				<-release
			case 2:
				close(processed)
			}
		}
		return nil
	}
	s, e := f.b.AssessmentData(context.Background(), f.c, nil)
	if e != nil {
		t.Fatal(e)
	}
	executionWait(t, sampled)
	payload := []byte("PRIVATE_INPUT_POISON\n")
	n := int64(len(payload))
	gen := s.Generation
	writer := s.WriterEpoch
	c := x.Control{Schema: x.ControlSchema, Operation: "ingest", Binding: f.c.Binding, ExpectedGeneration: &gen, AdmittingOwnerEpoch: f.c.AdmittingOwnerEpoch, WriterEpoch: &writer, ContentLength: &n}
	s, e = f.b.AssessmentData(context.Background(), c, bytes.NewReader(payload))
	if e != nil {
		t.Fatal("ingest while watcher paused", e)
	}
	once.Do(func() { close(release) })
	executionWait(t, processed)
	current, _, e := f.b.assessmentRead(f.c.Binding)
	if e != nil || current.DenyLatched || current.State != "accepting" || !current.UploadComplete {
		t.Fatalf("legitimate newer commit denied: %v state=%s reason=%s", e, current.State, current.Reason)
	}
	gen = s.Generation
	c.Operation = "seal"
	c.ContentLength = nil
	if _, e = f.b.AssessmentData(context.Background(), c, nil); e != nil {
		t.Fatal("seal after concurrent snapshot", e)
	}
}

func TestExecutionLostClaimACKCannotDispatchOrReopen(t *testing.T) {
	f := newExecutionFixture(t)
	s := f.seal(t)
	c := executionControl(f, s, 0)
	f.b.assessment.owned.fault = func(point string) error {
		if point == "after-journal:claim" {
			return errors.New("synthetic lost acknowledgement")
		}
		return nil
	}
	// Watcher holds its copied pre-fault boundary; this fault is invocation-local.
	if _, e := f.b.RunAssessmentOnce(context.Background(), c); e == nil {
		t.Fatal("lost ACK accepted")
	}
	f.b.assessment.owned.fault = nil
	_, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil || a.Generation != 1 || !a.AccountingUnresolved || a.Compute.ReservedCPUSeconds != 30 || a.Compute.ChargedCPUSeconds != 0 {
		t.Fatal(a, e)
	}
	scope, _, e := f.b.assessmentRead(c.Binding)
	if e != nil || scope.NormalTransitionsUsed == 0 {
		t.Fatal(scope, e)
	}
	u, e := f.b.Store.Usage(c.Binding.ProjectID)
	if e != nil || u.Reserved != 30 || u.Unknown != 30 || u.Charged != 0 || u.Remaining != 30 {
		t.Fatal(u, e)
	}
	reopened, e := ledger.Open(f.b.Store.Dir, false)
	if e != nil {
		t.Fatal(e)
	}
	b := New(reopened, nil)
	b.assessment = f.b.assessment
	for i := 0; i < 2; i++ {
		r, e := b.RunAssessmentOnce(context.Background(), c)
		if e != nil || r.Disposition != "existing_attempt" || r.Code != "HELD" {
			t.Fatal(r.Code, r.Disposition, e)
		}
	}
	if f.p.starts != 0 || len(f.p.requests) != 0 {
		t.Fatal("claim readback replayed effects")
	}
	before, e := os.ReadFile(filepath.Join(b.Store.Dir, "ledger.json"))
	if e != nil {
		t.Fatal(e)
	}
	if e = b.Store.Update(func(d *ledger.Data) error { delete(d.AssessmentExecution.Attempts, c.Slot.AttemptID); return nil }); e == nil {
		t.Fatal("removed permanent attempt")
	}
	after, _ := os.ReadFile(filepath.Join(b.Store.Dir, "ledger.json"))
	if !bytes.Equal(before, after) {
		t.Fatal("refusal changed authority")
	}
}

func TestExecutionWholeAttemptOutputBudgetAndDiscard(t *testing.T) {
	budget := &assessmentOutputBudget{limit: 10}
	first := &assessmentCapture{budget: budget}
	second := &assessmentCapture{budget: budget}
	if n, e := (assessmentCaptureWriter{first, false}).Write([]byte("123456")); e != nil || n != 6 {
		t.Fatal(n, e)
	}
	if n, e := (assessmentCaptureWriter{second, true}).Write([]byte("7890")); e != nil || n != 4 {
		t.Fatal(n, e)
	}
	if n, e := (assessmentCaptureWriter{second, false}).Write([]byte("X")); e == nil || n != 0 || !second.overflow {
		t.Fatal("per-phase budget multiplication")
	}
	if second.out.Len() != 0 || budget.used != 10 {
		t.Fatal("stderr retained or charge mismatch")
	}
}

type executionFaultCollector struct {
	closed *atomic.Int32
	fail   bool
}

func (c *executionFaultCollector) Observe(context.Context, x.DockerResourceIdentity) (x.MetricSet, error) {
	return x.UnknownMetrics(), nil
}
func (c *executionFaultCollector) Finish(context.Context) (x.MetricSet, error) {
	return x.UnknownMetrics(), nil
}
func (c *executionFaultCollector) Close() error {
	c.closed.Add(1)
	if c.fail {
		return errors.New("uncertain synthetic collector close")
	}
	return nil
}

func TestExecutionConstructorCloseUncertaintyIsSticky(t *testing.T) {
	for _, kind := range []string{"collector", "transport"} {
		t.Run(kind, func(t *testing.T) {
			f := newExecutionFixture(t)
			s := f.seal(t)
			c := executionControl(f, s, 0)
			var closed atomic.Int32
			// Fail the first uncertainty publication as well: a later successful
			// Close must not turn a nonnil+error constructor into confirmation.
			var failed atomic.Bool
			event := "before-journal:" + string(x.AttemptCollectorUncertain)
			if kind == "transport" {
				event = "before-journal:" + string(x.AttemptPhaseSafety)
			}
			f.b.assessment.owned.fault = func(p string) error {
				if p == event && !failed.Swap(true) {
					return errors.New("synthetic publication refusal")
				}
				return nil
			}
			if kind == "collector" {
				f.b.assessment.collector = func(context.Context, x.ExecutionScope) (assessmentCollector, error) {
					return &executionFaultCollector{closed: &closed}, errors.New("nonnull constructor failure")
				}
			} else {
				f.b.assessment.transport = func(context.Context, x.ExecutionScope, string) (io.Closer, error) {
					return executionCloserFunc(func() error { closed.Add(1); return nil }), errors.New("nonnull constructor failure")
				}
			}
			if _, e := f.b.RunAssessmentOnce(context.Background(), c); e == nil {
				t.Fatal("constructor uncertainty accepted")
			}
			f.b.assessment.owned.fault = nil
			_, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
			if e != nil {
				t.Fatal(e)
			}
			if closed.Load() != 1 || !a.AccountingUnresolved || a.Compute.ChargedCPUSeconds != 0 || a.SerialClear() {
				t.Fatal("uncertain constructor settled", closed.Load(), a.Compute, a.State)
			}
			if kind == "collector" {
				if !a.CollectorCloseUncertain || a.CollectorCloseConfirmed {
					t.Fatal("lost sticky collector uncertainty")
				}
			} else {
				if a.PhaseRecords.Workload == nil || !a.PhaseRecords.Workload.TransportCloseUncertain || a.PhaseRecords.Workload.TransportCloseConfirmed {
					t.Fatal("lost sticky transport uncertainty")
				}
			}
			if f.p.starts != 0 || len(f.p.requests) != 0 {
				t.Fatal("uncertain constructor dispatched provider")
			}
		})
	}
}

type executionCloserFunc func() error

func (f executionCloserFunc) Close() error { return f() }

func TestExecutionClaimIdentityAndPlanCannotBeRebound(t *testing.T) {
	f := newExecutionFixture(t)
	s := f.seal(t)
	c := executionControl(f, s, 0)
	if _, _, e := f.b.assessmentClaim(c); e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
	if e != nil {
		t.Fatal(e)
	}
	mutations := map[string]func(*ledger.Data){
		"scope-delete": func(d *ledger.Data) { delete(d.AssessmentExecution.Scopes, c.Binding.ScopeID) },
		"plan-shrink": func(d *ledger.Data) {
			v := d.AssessmentExecution.Scopes[c.Binding.ScopeID]
			v.Slots = v.Slots[:1]
			d.AssessmentExecution.Scopes[v.ScopeID] = v
		},
		"attempt-rebind": func(d *ledger.Data) {
			v := d.AssessmentExecution.Attempts[c.Slot.AttemptID]
			v.HandoffNonce = strings.Repeat("f", 32)
			v.BindingDigest = x.AttemptBindingDigest(v.Binding, v.Slot, v.HandoffNonce)
			d.AssessmentExecution.Attempts[c.Slot.AttemptID] = v
		},
		"charge-refund": func(d *ledger.Data) {
			v := d.AssessmentExecution.Attempts[c.Slot.AttemptID]
			v.Compute.ReservedCPUSeconds = 0
			d.AssessmentExecution.Attempts[c.Slot.AttemptID] = v
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			if e := f.b.Store.Update(func(d *ledger.Data) error { mutate(d); return nil }); e == nil {
				t.Fatal("permanent identity mutated")
			}
			after, _ := os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
			if !bytes.Equal(before, after) {
				t.Fatal("failed update changed authority")
			}
		})
	}
	c2 := c
	nonce := strings.Repeat("f", 32)
	c2.HandoffNonce = &nonce
	if _, _, e := f.b.assessmentClaim(c2); e != x.ErrCollision {
		t.Fatal("unrelated duplicate accepted", e)
	}
	if f.p.starts != 0 || len(f.p.requests) != 0 {
		t.Fatal("identity-only controls touched provider")
	}
}

func TestExecutionDirectJournalCannotFinalizeMissingResourceIdentity(t *testing.T) {
	f := newExecutionFixture(t)
	s := f.seal(t)
	c := executionControl(f, s, 0)
	if _, _, e := f.b.assessmentClaim(c); e != nil {
		t.Fatal(e)
	}
	advance := func(event x.Event, mutate func(*x.AssessmentAttempt)) error {
		return f.b.Store.Update(func(d *ledger.Data) error {
			scope := d.AssessmentExecution.Scopes[c.Binding.ScopeID]
			a := d.AssessmentExecution.Attempts[c.Slot.AttemptID]
			next, e := x.AdvanceAttempt(a, scope, event, a.WallHighWater, a.TickHighWaterMS, mutate)
			if e == nil {
				d.AssessmentExecution.Attempts[a.Slot.AttemptID] = next
			}
			return e
		})
	}
	steps := []struct {
		event x.Event
		f     func(*x.AssessmentAttempt)
	}{
		{x.AttemptReader, func(a *x.AssessmentAttempt) { a.ReaderStarted = true }},
		{x.AttemptCollector, func(a *x.AssessmentAttempt) { a.CollectorStarted = true }},
		{x.AttemptPhase, func(a *x.AssessmentAttempt) {
			a.PhaseRecords.Workload = &x.PhaseRecord{Phase: "workload", OwnerName: x.ResourceName(a.Binding.BrokerAuthorityID, a.Slot.AttemptID, "workload")}
		}},
		{x.AttemptPhaseForward, func(a *x.AssessmentAttempt) { a.PhaseRecords.Workload.TransportStarted = true }},
		{x.AttemptPhaseForward, func(a *x.AssessmentAttempt) { a.PhaseRecords.Workload.CreateClaimed = true }},
		{x.AttemptPhaseForward, func(a *x.AssessmentAttempt) { a.PhaseRecords.Workload.CreateReturned = true }},
		{x.AttemptPhaseSafety, func(a *x.AssessmentAttempt) { a.PhaseRecords.Workload.StoppedObserved = true }},
	}
	for i, step := range steps {
		if e := advance(step.event, step.f); e != nil {
			t.Fatal(i, e)
		}
	}
	before, e := os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
	if e != nil {
		t.Fatal(e)
	}
	if e = advance(x.AttemptPhaseSafety, func(a *x.AssessmentAttempt) { a.PhaseRecords.Workload.RemovalObserved = true }); e == nil {
		t.Fatal("claimed create became absent without exact owned identity")
	}
	after, _ := os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
	if !bytes.Equal(before, after) {
		t.Fatal("invalid finality changed durable state")
	}
	_, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil || a.EffectsClosed() || a.SerialClear() || !a.AccountingUnresolved || a.Compute.ChargedCPUSeconds != 0 {
		t.Fatal(a, e)
	}
	if f.p.starts != 0 || len(f.p.requests) != 0 {
		t.Fatal("journal-only control touched provider")
	}
}

func TestExecutionSerialSlotTwoUsesSameInputAndBudget(t *testing.T) {
	f := newExecutionFixture(t)
	s := f.seal(t)
	for i := 0; i < 2; i++ {
		if i > 0 {
			v, _, e := f.b.assessmentRead(f.c.Binding)
			if e != nil {
				t.Fatal(e)
			}
			s = v.Status("status", "OK", "unknown", nil)
		}
		c := executionControl(f, s, i)
		r, e := f.b.RunAssessmentOnce(context.Background(), c)
		if e != nil || r.Code != "OK" || r.Attempt == nil || r.Attempt.Outcome != "succeeded" {
			t.Fatal(i, r.Disposition, e)
		}
	}
	u, e := f.b.Store.Usage(f.c.Binding.ProjectID)
	if e != nil || u.Budget != 60 || u.Charged != 60 || u.Remaining != 0 || u.Reserved != 0 || u.Unknown != 0 {
		t.Fatal(u, e)
	}
	if f.p.starts != 4 || f.p.maxActive != 1 {
		t.Fatal("phase overlap or duplicate", f.p.starts, f.p.maxActive)
	}
}

func TestExecutionDisabledDirectEntrypointsHaveNoEffects(t *testing.T) {
	f := newExecutionFixture(t)
	b := New(f.b.Store, nil)
	var e error
	for _, op := range []string{"prepare", "ingest", "seal"} {
		c := f.c
		c.Operation = op
		if _, e = b.AssessmentData(context.Background(), c, executionPanicReader{}); e != x.ErrDisabled {
			t.Fatal(op, e)
		}
	}
	if _, e = b.RunAssessmentOnce(context.Background(), x.Control{}); e != x.ErrDisabled {
		t.Fatal(e)
	}
	if e = b.AssessmentWatch(context.Background(), f.c.Binding); e != x.ErrDisabled {
		t.Fatal(e)
	}
	if _, _, e = b.Reserve(provider.Request{}); e == nil {
		t.Fatal("ordinary Reserve accepted v7")
	}
	if _, e = b.Run(context.Background(), provider.Request{}); e == nil {
		t.Fatal("ordinary Run accepted v7")
	}
	if _, _, e = b.RunOnce(context.Background(), provider.Request{}); e == nil {
		t.Fatal("ordinary RunOnce accepted v7")
	}
	if _, e = b.Cancel("none", "cancelled"); e == nil {
		t.Fatal("ordinary Cancel accepted v7")
	}
	if _, e = b.Reconcile("none"); e == nil {
		t.Fatal("ordinary Reconcile accepted v7")
	}
	if _, e = b.Reap(); e == nil {
		t.Fatal("ordinary Reap accepted v7")
	}
	if e = b.Watch(context.Background(), "none"); e == nil {
		t.Fatal("ordinary Watch accepted v7")
	}
	if _, e = os.Stat(filepath.Join(f.b.Store.Dir, "outputs")); !os.IsNotExist(e) {
		t.Fatal("ordinary guard created outputs")
	}
	if f.p.starts != 0 || len(f.p.requests) != 0 {
		t.Fatal("disabled boundary touched provider")
	}
}

type executionPanicReader struct{}

func (executionPanicReader) Read([]byte) (int, error) { panic("disabled content gate read input") }

func TestExecutionMetadataSlotRefusalHasNoScopeEffects(t *testing.T) {
	f := newExecutionFixture(t)
	status := f.seal(t)
	before, e := os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
	if e != nil {
		t.Fatal(e)
	}
	slot := f.c.Manifest.Reservations[0]
	// An unclaimed planned slot is not a consumed attempt. Invalid or mismatched
	// slots likewise cannot widen cancel/reconcile into scope cancellation.
	for _, op := range []string{"status", "cancel", "reconcile"} {
		for _, which := range []string{"unclaimed", "unknown"} {
			c := x.Control{Schema: x.ControlSchema, Operation: op, Binding: f.c.Binding, Slot: &slot}
			if which == "unknown" {
				copy := slot
				copy.AttemptID = strings.Repeat("b", 32)
				c.Slot = &copy
			}
			if op == "cancel" {
				reason := "cancelled"
				c.Reason = &reason
			}
			if _, e := f.b.AssessmentData(context.Background(), c, nil); e == nil {
				t.Fatalf("accepted %s %s", op, which)
			}
			after, e := os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
			if e != nil || !bytes.Equal(before, after) {
				t.Fatalf("%s %s changed authority", op, which)
			}
		}
	}
	scope, _, e := f.b.assessmentRead(f.c.Binding)
	if e != nil || scope.DenyLatched || scope.Generation != status.Generation {
		t.Fatal("scope changed", e)
	}
	if f.p.starts != 0 || len(f.p.requests) != 0 {
		t.Fatal("provider effects")
	}
}
func TestExecutionSlotCancellationDoesNotDenyRetainedScope(t *testing.T) {
	f := newExecutionFixture(t)
	status := f.seal(t)
	c := executionControl(f, status, 0)
	_, owner, fresh, e := f.b.assessmentBeginAttempt(c)
	if e != nil || !fresh {
		t.Fatal("original claim", e)
	}
	defer owner.CloseAndCommitOwnership()
	if e = owner.AcquireExecutionSet(); e != nil {
		t.Fatal(e)
	}
	if e = owner.FreshCurrent(f.b.assessmentBeginGuard(c.Binding, false)); e != nil {
		t.Fatal(e)
	}
	reason := "cancelled"
	cc := x.Control{Schema: x.ControlSchema, Operation: "cancel", Binding: c.Binding, Slot: c.Slot, Reason: &reason}
	if _, e = f.b.AssessmentData(context.Background(), cc, nil); !errors.Is(e, x.ErrHeld) {
		t.Fatal("metadata cancel cannot close the live original owner", e)
	}
	// The original live owner has never started a reader, collector or phase.
	// Only its actual release can allow the already-denied claim to settle.
	if _, e = f.b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptFinish, func(v *x.AssessmentAttempt) error { v.State = "finishing"; return nil }); e != nil {
		t.Fatal(e)
	}
	if e = owner.CloseAndCommitOwnership(); e != nil {
		t.Fatal("original checked close", e)
	}
	if e = f.b.assessmentFinalize(context.Background(), c); e != nil {
		t.Fatal("original owner settlement", e)
	}
	scope, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil || scope.DenyLatched || scope.State != "sealed" || !a.DenyLatched || a.Outcome != "cancelled" || a.Compute.ChargedCPUSeconds != 30 {
		t.Fatalf("slot cancellation widened or lost charge: %v scope=%s denied=%v attempt=%s", e, scope.State, scope.DenyLatched, a.Outcome)
	}
	if f.p.starts != 0 || len(f.p.requests) != 0 {
		t.Fatal("cancel created provider work")
	}
	before, e := os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
	if e != nil {
		t.Fatal(e)
	}
	cc.Operation = "reconcile"
	cc.Reason = nil
	if _, e := f.b.AssessmentData(context.Background(), cc, nil); e != nil {
		t.Fatal(e)
	}
	after, e := os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
	if e != nil || !bytes.Equal(before, after) {
		t.Fatal("repeated completed reconcile changed authority")
	}
	payload, pe := os.ReadFile(filepath.Join(f.b.assessment.owned.root.Name(), scope.ScopeID, "payload"))
	if pe != nil || string(payload) != "PRIVATE_INPUT_POISON\n" {
		t.Fatal("slot cancellation removed or changed retained input", pe)
	}
	clear(payload)
	u, e := f.b.Store.Usage(c.Binding.ProjectID)
	if e != nil || u.Charged != 30 || u.Reserved != 0 || u.Remaining != 30 || !a.SerialClear() || !a.OwnershipCloseConfirmed {
		t.Fatal("completed original owner did not clear exact charge", u, e)
	}
	second := executionControl(f, scope.Status("status", "OK", "observed_current", nil), 1)
	r, e := f.b.RunAssessmentOnce(context.Background(), second)
	if e != nil || r.Code != "OK" || r.Disposition != "new_attempt" {
		t.Fatal("distinct preplanned slot after original closure", r.Code, e)
	}
	u, e = f.b.Store.Usage(c.Binding.ProjectID)
	if e != nil || u.Charged != 60 || u.Reserved != 0 || u.Remaining != 0 {
		t.Fatal("distinct slot accounting", u, e)
	}
}

func TestExecutionOrphanedClaimCancellationRemainsHeld(t *testing.T) {
	f := newExecutionFixture(t)
	c := executionControl(f, f.seal(t), 0)
	if _, _, e := f.b.assessmentClaim(c); e != nil {
		t.Fatal(e)
	}
	reason := "cancelled"
	cc := x.Control{Schema: x.ControlSchema, Operation: "cancel", Binding: c.Binding, Slot: c.Slot, Reason: &reason}
	if _, e := f.b.AssessmentData(context.Background(), cc, nil); !errors.Is(e, x.ErrHeld) {
		t.Fatal("lost original owner was reconstructed", e)
	}
	scope, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil || scope.DenyLatched || scope.State != "sealed" || !a.DenyLatched || a.OwnershipCloseConfirmed || !a.AccountingUnresolved || a.Compute.ChargedCPUSeconds != 0 || a.SerialClear() {
		t.Fatal("unknown original ownership lost held state", e)
	}
	payload, pe := os.ReadFile(filepath.Join(f.b.assessment.owned.root.Name(), scope.ScopeID, "payload"))
	if pe != nil || string(payload) != "PRIVATE_INPUT_POISON\n" {
		t.Fatal("slot cancellation removed or changed retained input", pe)
	}
	clear(payload)
	u, e := f.b.Store.Usage(c.Binding.ProjectID)
	if e != nil || u.Reserved != 30 || u.Unknown != 30 || u.Charged != 0 || u.Remaining != 30 {
		t.Fatal("orphan claim lost permanent reservation", u, e)
	}
	// Conservative CPU settlement is allowed to account for an unstarted claim;
	// it cannot replace the missing original descriptor owner or clear seriality.
	if e = f.b.assessmentFinalize(context.Background(), c); !errors.Is(e, x.ErrHeld) {
		t.Fatal("full charge certified orphaned ownership", e)
	}
	scope, a, e = f.b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil || a.AccountingUnresolved || a.Compute.ChargedCPUSeconds != 30 || a.OwnershipCloseConfirmed || a.SerialClear() || scope.DenyLatched {
		t.Fatal("full charge erased original ownership hold", e)
	}
	second := executionControl(f, scope.Status("status", "OK", "observed_current", nil), 1)
	if _, e = f.b.RunAssessmentOnce(context.Background(), second); e == nil {
		t.Fatal("orphaned owner allowed next slot")
	}
	if e = f.b.Store.View(func(d ledger.Data) error {
		if len(d.AssessmentExecution.Attempts) != 1 {
			t.Error("second slot was claimed")
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	u, e = f.b.Store.Usage(c.Binding.ProjectID)
	if e != nil || u.Charged != 30 || u.Reserved != 0 || u.Remaining != 30 {
		t.Fatal("orphaned full charge changed budget twice", u, e)
	}
	before, _ := os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
	cc.Operation, cc.Reason = "reconcile", nil
	if _, e = f.b.AssessmentData(context.Background(), cc, nil); !errors.Is(e, x.ErrHeld) {
		t.Fatal("metadata replay certified an unknown owner", e)
	}
	after, e := os.ReadFile(filepath.Join(f.b.Store.Dir, "ledger.json"))
	if e != nil || !bytes.Equal(before, after) || f.p.starts != 0 || len(f.p.requests) != 0 {
		t.Fatal("orphan poll changed authority or dispatched", e)
	}
}

// A successful capture can still fail when the deferred transport closes or its
// proof publication loses acknowledgement. Keep the returned slice alive and
// inspect it: clearing only the capture buffer is insufficient.
func TestExecutionDeferredPhaseFailureClearsReturnedOutput(t *testing.T) {
	for _, kind := range []string{"transport-close", "close-publication"} {
		t.Run(kind, func(t *testing.T) {
			f := newExecutionFixture(t)
			status := f.seal(t)
			c := executionControl(f, status, 0)
			if _, _, e := f.b.assessmentClaim(c); e != nil {
				t.Fatal(e)
			}
			for _, step := range []struct {
				event x.Event
				fn    func(*x.AssessmentAttempt) error
			}{
				{x.AttemptForward, func(a *x.AssessmentAttempt) error { a.State = "preparing"; return nil }},
				{x.AttemptReader, func(a *x.AssessmentAttempt) error { a.ReaderStarted = true; return nil }},
				{x.AttemptCollector, func(a *x.AssessmentAttempt) error { a.CollectorStarted = true; return nil }},
				{x.AttemptForward, func(a *x.AssessmentAttempt) error { a.State = "running"; return nil }},
			} {
				if _, e := f.b.assessmentChangeAttempt(c.Binding, *c.Slot, step.event, step.fn); e != nil {
					t.Fatal(e)
				}
			}
			closing, failed := false, false
			f.b.assessment.transport = func(context.Context, x.ExecutionScope, string) (io.Closer, error) {
				return executionCloserFunc(func() error {
					closing = true
					if kind == "transport-close" {
						return errors.New("synthetic close failure")
					}
					return nil
				}), nil
			}
			f.b.assessment.owned.fault = func(point string) error {
				if kind == "close-publication" && closing && !failed && point == "after-journal:"+string(x.AttemptPhaseSafety) {
					failed = true
					return errors.New("synthetic close proof ACK loss")
				}
				return nil
			}
			scope, _, e := f.b.assessmentRead(c.Binding)
			if e != nil {
				t.Fatal(e)
			}
			returned, _, e := f.b.assessmentPhase(context.Background(), c, scope, "workload", strings.NewReader("PRIVATE_INPUT_POISON\n"), nil, &executionTestCollector{}, &assessmentOutputBudget{limit: 65536})
			if e == nil || !closing || kind == "close-publication" && !failed {
				t.Fatal("fault did not reach deferred close", e)
			}
			_, a, ae := f.b.assessmentAttempt(c.Binding, *c.Slot)
			if ae != nil || a.PhaseRecords.Workload == nil || !a.PhaseRecords.Workload.TransportCloseUncertain || a.PhaseRecords.Checker != nil || !a.AccountingUnresolved {
				t.Fatal("uncertainty lost", ae)
			}
			for _, b := range returned {
				if b != 0 {
					t.Fatal("failed phase returned retained nonzero output bytes")
				}
			}
		})
	}
}

// Bypass the Broker close publisher: Store.Update itself must refuse a new
// global close proof while any phase effect or inherited transport is unknown.
func TestExecutionGlobalCloseRequiresDefinitivePhaseDrain(t *testing.T) {
	for _, kind := range []string{"transport-open", "create-pending", "create-ambiguous", "start-pending", "resource-present"} {
		t.Run(kind, func(t *testing.T) {
			f := newExecutionFixture(t)
			s := f.seal(t)
			c := executionControl(f, s, 0)
			a, owner, fresh, e := f.b.assessmentBeginAttempt(c)
			if e != nil || !fresh {
				t.Fatal("claim", e)
			}
			if owner.AcquireExecutionSet() != nil || owner.FreshCurrent(f.b.assessmentBeginGuard(c.Binding, false)) != nil {
				t.Fatal("direct control acquisition")
			}
			defer owner.CloseAndCommitOwnership()
			step := func(event x.Event, mutate func(*x.AssessmentAttempt) error) {
				t.Helper()
				var e error
				a, e = f.b.assessmentChangeAttempt(c.Binding, *c.Slot, event, mutate)
				if e != nil {
					t.Fatal(event, e)
				}
			}
			step(x.AttemptReader, func(v *x.AssessmentAttempt) error { v.ReaderStarted = true; return nil })
			step(x.AttemptCollector, func(v *x.AssessmentAttempt) error { v.CollectorStarted = true; return nil })
			step(x.AttemptPhase, func(v *x.AssessmentAttempt) error {
				v.PhaseRecords.Workload = &x.PhaseRecord{Phase: "workload", OwnerName: x.ResourceName(c.Binding.BrokerAuthorityID, c.Slot.AttemptID, "workload")}
				return nil
			})
			phase := func(mutate func(*x.PhaseRecord)) func(*x.AssessmentAttempt) error {
				return func(v *x.AssessmentAttempt) error { mutate(v.PhaseRecords.Workload); return nil }
			}
			step(x.AttemptPhaseForward, phase(func(p *x.PhaseRecord) { p.TransportStarted = true }))
			if kind != "transport-open" {
				step(x.AttemptPhaseForward, phase(func(p *x.PhaseRecord) { p.CreateClaimed = true }))
			}
			if kind == "create-ambiguous" {
				step(x.AttemptPhaseSafety, phase(func(p *x.PhaseRecord) { p.CreateAmbiguous = true }))
			}
			if kind == "start-pending" || kind == "resource-present" {
				step(x.AttemptPhaseSafety, phase(func(p *x.PhaseRecord) { p.CreateReturned = true }))
				step(x.AttemptPhaseSafety, phase(func(p *x.PhaseRecord) {
					p.ResourceIdentity = &x.DockerResourceIdentity{ContainerID: strings.Repeat("a", 64), BrokerOwner: c.Binding.BrokerAuthorityID, AttemptID: c.Slot.AttemptID, Phase: "workload", ScopeID: c.Binding.ScopeID, CapabilitySHA256: c.Binding.Capability.SHA256}
				}))
				if kind == "start-pending" {
					step(x.AttemptPhaseForward, phase(func(p *x.PhaseRecord) { p.StartClaimed = true }))
				}
			}
			if kind != "transport-open" {
				step(x.AttemptPhaseSafety, phase(func(p *x.PhaseRecord) { p.TransportCloseConfirmed = true }))
			}
			step(x.AttemptFinish, func(v *x.AssessmentAttempt) error { v.State = "finishing"; return nil })
			path := filepath.Join(f.b.Store.Dir, "ledger.json")
			before, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			for _, reader := range []bool{true, false} {
				e = f.b.Store.Update(func(d *ledger.Data) error {
					v := d.AssessmentExecution.Attempts[c.Slot.AttemptID]
					v.Generation++
					v.SafetyTransitionsUsed++
					if reader {
						v.ReaderCloseConfirmed = true
					} else {
						v.CollectorCloseConfirmed = true
					}
					d.AssessmentExecution.Attempts[c.Slot.AttemptID] = v
					return nil
				})
				if e == nil {
					t.Errorf("direct global close admitted: reader=%v kind=%s", reader, kind)
				}
				after, e := os.ReadFile(path)
				if e != nil || !bytes.Equal(before, after) {
					t.Fatal("refused global proof changed journal", e)
				}
			}
			if a.SerialClear() || f.p.starts != 0 || len(f.p.requests) != 0 {
				t.Fatal("direct proof control dispatched or settled")
			}
			if kind == "resource-present" {
				step(x.AttemptPhaseSafety, phase(func(p *x.PhaseRecord) { p.StoppedObserved = true }))
				step(x.AttemptPhaseSafety, phase(func(p *x.PhaseRecord) { p.RemovalObserved = true }))
				step(x.AttemptReaderClosed, func(v *x.AssessmentAttempt) error { v.ReaderCloseConfirmed = true; return nil })
				step(x.AttemptCollectorClosed, func(v *x.AssessmentAttempt) error { v.CollectorCloseConfirmed = true; return nil })
				step(x.AttemptFinish, func(v *x.AssessmentAttempt) error { v.State = "held"; return nil })
				step(x.AttemptSettle, func(v *x.AssessmentAttempt) error {
					v.ComputeCleanupObserved = true
					v.AccountingUnresolved = false
					v.Compute.ChargedCPUSeconds = v.Compute.ReservedCPUSeconds
					v.Compute.Provenance = "estimated_upper_bound"
					return nil
				})
				if e := owner.CloseAndCommitOwnership(); e != nil {
					t.Fatal("owner close", e)
				}
				step(x.AttemptFinish, func(v *x.AssessmentAttempt) error { v.State = "terminal"; return nil })
				if !a.SerialClear() {
					t.Fatal("definitive phase close could not complete")
				}
				step(x.AttemptPhaseSafety, phase(func(p *x.PhaseRecord) { p.TransportCloseUncertain = true }))
				if a.SerialClear() || a.AccountingUnresolved || a.Compute.ChargedCPUSeconds != 30 || !a.ReaderCloseConfirmed || !a.CollectorCloseConfirmed || a.State != "terminal" {
					t.Fatal("late uncertainty erased settled history or restored clearance")
				}
			}
		})
	}
}

func TestExecutionGlobalCloseNotInferredFromParentClose(t *testing.T) {
	f := newExecutionFixture(t)
	s := f.seal(t)
	c := executionControl(f, s, 0)
	f.b.assessment.transport = func(context.Context, x.ExecutionScope, string) (io.Closer, error) {
		return executionCloserFunc(func() error { return errors.New("synthetic phase drain uncertain") }), nil
	}
	if _, e := f.b.RunAssessmentOnce(context.Background(), c); e == nil {
		t.Fatal("uncertain phase close succeeded")
	}
	_, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil || a.ReaderCloseConfirmed || a.CollectorCloseConfirmed || a.PhaseRecords.Workload == nil || !a.PhaseRecords.Workload.TransportCloseUncertain || a.PhaseRecords.Checker != nil || !a.AccountingUnresolved || a.Compute.ChargedCPUSeconds != 0 {
		t.Fatal("parent closes fabricated global drain", e, a)
	}
}

// Start returning does not imply the independently owned transport has retired
// every supplied output writer. Retain the actual capture, not just a copied
// returned output slice, and perform a synchronous callback from Close.
type executionRetainedCaptureProvider struct {
	*executionTestProvider
	captures   []assessmentCaptureWriter
	startError bool
}

func (p *executionRetainedCaptureProvider) Start(ctx context.Context, id x.DockerResourceIdentity, input io.Reader, prior []byte, out, stderr io.Writer) (assessmentCompletion, error) {
	writer, ok := out.(assessmentCaptureWriter)
	if !ok {
		return assessmentCompletion{}, errors.New("missing actual capture writer")
	}
	p.captures = append(p.captures, writer)
	completion, e := p.executionTestProvider.Start(ctx, id, input, prior, out, stderr)
	if e == nil && p.startError {
		e = errors.New("synthetic Start error after supplied output")
	}
	return completion, e
}
func TestExecutionLateTransportCloseCannotRetainCapture(t *testing.T) {
	for _, mode := range []struct {
		name                   string
		closeFails, startError bool
	}{
		{"successful-close", false, false},
		{"failed-close", true, false},
		{"Start-error-before-return", false, true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			f := newExecutionFixture(t)
			status := f.seal(t)
			p := &executionRetainedCaptureProvider{executionTestProvider: f.p, startError: mode.startError}
			f.b.assessment.provider = p
			type callbackResult struct {
				n   int
				err error
			}
			var callbacks []callbackResult
			f.b.assessment.transport = func(context.Context, x.ExecutionScope, string) (io.Closer, error) {
				return executionCloserFunc(func() error {
					if len(p.captures) == 0 {
						return errors.New("capture was never supplied")
					}
					w := p.captures[len(p.captures)-1]
					n, e := w.Write([]byte("LATE_CLOSE_CAPTURE_POISON"))
					callbacks = append(callbacks, callbackResult{n, e})
					if mode.closeFails {
						return errors.New("synthetic transport close failure")
					}
					return nil
				}), nil
			}
			result, e := f.b.RunAssessmentOnce(context.Background(), executionControl(f, status, 0))
			if (mode.closeFails || mode.startError) && e == nil || !mode.closeFails && !mode.startError && (e != nil || result.Code != "OK") {
				t.Fatal("unexpected phase outcome", e, result.Code)
			}
			if len(callbacks) != len(p.captures) || len(callbacks) == 0 {
				t.Fatal("late Close callback not exercised", len(callbacks), len(p.captures))
			}
			for i, w := range p.captures {
				w.capture.mu.Lock()
				retained := w.capture.out.Len()
				w.capture.mu.Unlock()
				if callbacks[i].n != 0 || callbacks[i].err == nil || retained != 0 {
					t.Errorf("transport Close repopulated retired capture: phase=%d accepted=%d err=%v retained=%d", i, callbacks[i].n, callbacks[i].err, retained)
				}
				// A saved writer also cannot regain ownership after phase return.
				n, we := w.Write([]byte("AFTER_RETURN_CAPTURE_POISON"))
				w.capture.mu.Lock()
				after := w.capture.out.Len()
				w.capture.mu.Unlock()
				if n != 0 || we == nil || after != 0 {
					t.Errorf("retained writer reopened capture after return: phase=%d accepted=%d err=%v retained=%d", i, n, we, after)
				}
			}
		})
	}
}
