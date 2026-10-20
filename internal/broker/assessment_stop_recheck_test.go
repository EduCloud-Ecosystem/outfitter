package broker

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync/atomic"
	"testing"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
)

type executionStopRecheckProvider struct {
	assessmentProvider
	creates, starts, inspects, stops, removes atomic.Int32
}

func (p *executionStopRecheckProvider) Create(ctx context.Context, request assessmentPhaseRequest) (x.DockerResourceIdentity, error) {
	p.creates.Add(1)
	return p.assessmentProvider.Create(ctx, request)
}
func (p *executionStopRecheckProvider) Start(ctx context.Context, id x.DockerResourceIdentity, input io.Reader, prior []byte, out, stderr io.Writer) (assessmentCompletion, error) {
	p.starts.Add(1)
	return p.assessmentProvider.Start(ctx, id, input, prior, out, stderr)
}
func (p *executionStopRecheckProvider) Inspect(ctx context.Context, request assessmentPhaseRequest) (assessmentResourceObservation, error) {
	p.inspects.Add(1)
	return p.assessmentProvider.Inspect(ctx, request)
}
func (p *executionStopRecheckProvider) Stop(ctx context.Context, id x.DockerResourceIdentity) error {
	p.stops.Add(1)
	return p.assessmentProvider.Stop(ctx, id)
}
func (p *executionStopRecheckProvider) Remove(ctx context.Context, id x.DockerResourceIdentity) error {
	p.removes.Add(1)
	return p.assessmentProvider.Remove(ctx, id)
}

// A committed denial is reused without a no-op publication. This no-phase
// witness does not establish failed-first-ACK Stop authority; live guards do.
func TestExecutionStopAlreadyDeniedUsesCommittedState(t *testing.T) {
	f := newExecutionFixture(t)
	provider := &executionStopRecheckProvider{assessmentProvider: f.b.assessment.provider}
	f.b.assessment.provider = provider
	var collectors, transports atomic.Int32
	originalCollector := f.b.assessment.collector
	f.b.assessment.collector = func(ctx context.Context, scope x.ExecutionScope) (assessmentCollector, error) {
		collectors.Add(1)
		return originalCollector(ctx, scope)
	}
	originalTransport := f.b.assessment.transport
	f.b.assessment.transport = func(ctx context.Context, scope x.ExecutionScope, phase string) (io.Closer, error) {
		transports.Add(1)
		return originalTransport(ctx, scope, phase)
	}
	status := f.seal(t)
	c := executionControl(f, status, 0)
	_, owner, fresh, e := f.b.assessmentBeginAttempt(c)
	if e != nil || !fresh {
		t.Fatal("original claim missing")
	}
	t.Cleanup(func() {
		_, finishErr := f.b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptFinish, func(v *x.AssessmentAttempt) error {
			if v.State != "finishing" && v.State != "held" && v.State != "terminal" {
				v.State = "finishing"
			}
			return nil
		})
		if finishErr != nil {
			t.Error("original owner suppression failed")
		}
		if owner.CloseAndCommitOwnership() != nil {
			t.Error("original checked close failed")
		}
	})
	if owner.AcquireExecutionSet() != nil || owner.FreshCurrent(f.b.assessmentBeginGuard(c.Binding, false)) != nil {
		t.Fatal("original owner acquisition missing")
	}
	reason := "cancelled"
	cancel := x.Control{Schema: x.ControlSchema, Operation: "cancel", Binding: c.Binding, Slot: c.Slot, Reason: &reason}
	if _, e = f.b.AssessmentData(context.Background(), cancel, nil); !errors.Is(e, x.ErrHeld) {
		t.Fatal("metadata cancel cleared the original live owner")
	}
	var beforeCalls, afterCalls atomic.Int32
	// The watcher already copied its boundary at seal. Only this parent helper
	// receives these observational callbacks; every callback returns nil.
	f.b.assessment.owned.fault = func(point string) error {
		if point == "before-journal:"+string(x.AttemptDeny) {
			beforeCalls.Add(1)
		}
		if point == "after-journal:"+string(x.AttemptDeny) {
			afterCalls.Add(1)
		}
		return nil
	}
	beforeScope, before, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil || beforeScope.DenyLatched || beforeScope.State != "sealed" || !before.DenyLatched || before.FailureCode != "cancelled" || before.State != "claimed" || before.Outcome != "unknown" || !before.AccountingUnresolved || before.Compute.ReservedCPUSeconds != 30 || before.Compute.ChargedCPUSeconds != 0 || before.Compute.Provenance != "reserved" || before.ComputeCleanupObserved || before.OwnershipCloseConfirmed || before.OwnershipCloseUncertain || before.SerialClear() || before.ReaderStarted || before.CollectorStarted || before.ReaderCloseConfirmed || before.CollectorCloseConfirmed || before.ReaderCloseUncertain || before.CollectorCloseUncertain || before.PhaseRecords.Workload != nil || before.PhaseRecords.Checker != nil {
		t.Fatal("original denied no-phase liability missing")
	}
	if e = f.b.assessmentStopDeniedAttempts(context.Background(), c.Binding); e != nil {
		t.Fatal("committed denial helper failed")
	}
	afterScope, after, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil || !reflect.DeepEqual(beforeScope, afterScope) || !reflect.DeepEqual(before, after) {
		t.Fatal("committed denial read changed scope, generation, events or liability")
	}
	if beforeCalls.Load() != 0 || afterCalls.Load() != 0 {
		t.Fatal("committed denial redundantly attempted publication")
	}
	if provider.creates.Load() != 0 || provider.starts.Load() != 0 || provider.inspects.Load() != 0 || provider.stops.Load() != 0 || provider.removes.Load() != 0 || collectors.Load() != 0 || transports.Load() != 0 {
		t.Fatal("no-phase committed denial constructed or dispatched effects")
	}
	if _, e = f.b.assessment.owned.root.Lstat(afterScope.ScopeID); e != nil {
		t.Fatal("committed denial read removed retained scope")
	}
	// Zero callbacks witness the omitted no-op path, not writes/fsyncs,
	// latency cause, failed-new-denial authority or cleanup confirmation.
	t.Log("committed denial callbacks before=0 after=0; metadata unchanged; original owner still held")
}
