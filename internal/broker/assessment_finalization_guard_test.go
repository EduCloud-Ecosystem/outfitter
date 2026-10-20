package broker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
)

// Real Run owns every positive completion. The outside-lock hook publishes only
// legal negative history after its original mutation has definitively committed.
func TestExecutionFinalizeLateUncertaintyKeepsHeld(t *testing.T) {
	for _, stage := range []string{"after-held", "after-settle"} {
		t.Run(stage, func(t *testing.T) {
			f := newExecutionFixture(t)
			status := f.seal(t)
			c := executionControl(f, status, 0)
			var fired atomic.Bool
			var finishes atomic.Int32
			var seamErr error
			commits := 0
			// Installed after seal: only the original Run sees this hook; the
			// already-launched watcher's copied boundary remains unchanged.
			f.b.assessment.owned.fault = func(point string) error {
				if stage == "after-held" {
					if point != "after-journal:"+string(x.AttemptFinish) || finishes.Add(1) != 2 {
						return nil
					}
				} else if point != "after-journal:"+string(x.AttemptSettle) {
					return nil
				}
				// Latch before nested Store calls; do not depend on callback reentry.
				if !fired.CompareAndSwap(false, true) {
					return nil
				}
				_, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
				if e != nil {
					seamErr = e
					return e
				}
				settled := stage == "after-settle"
				if a.State != "held" || a.Outcome != "unknown" || a.DenyLatched || a.FailureCode != "none" || !a.ReaderStarted || !a.CollectorStarted || !a.ReaderCloseConfirmed || !a.CollectorCloseConfirmed || !a.OwnershipCloseConfirmed || a.ReaderCloseUncertain || a.CollectorCloseUncertain || a.OwnershipCloseUncertain || !a.EffectsClosed() || a.AccountingUnresolved == settled || a.ComputeCleanupObserved != settled || a.Compute.ReservedCPUSeconds != 30 || !settled && (a.Compute.ChargedCPUSeconds != 0 || a.Compute.Provenance != "reserved") || settled && (a.Compute.ChargedCPUSeconds != 30 || a.Compute.Provenance != "estimated_upper_bound") {
					seamErr = x.ErrInvalid
					return seamErr
				}
				for _, event := range []x.Event{x.AttemptDeny, x.AttemptReaderUncertain} {
					e = f.b.Store.Update(func(d *ledger.Data) error {
						if d.Version != 7 || d.AssessmentExecution == nil {
							return x.ErrInvalid
						}
						s, scopeOK := d.AssessmentExecution.Scopes[c.Binding.ScopeID]
						current, attemptOK := d.AssessmentExecution.Attempts[c.Slot.AttemptID]
						if !scopeOK || !attemptOK || s.Binding() != c.Binding || current.Binding != c.Binding || current.Slot != *c.Slot || current.State != "held" {
							return x.ErrInvalid
						}
						next, e := x.AdvanceAttempt(current, s, event, current.WallHighWater, current.TickHighWaterMS, func(v *x.AssessmentAttempt) {
							if event == x.AttemptDeny {
								v.DenyLatched = true
								v.FailureCode = "io_uncertain"
							} else {
								v.ReaderCloseUncertain = true
							}
						})
						if e == nil {
							d.AssessmentExecution.Attempts[c.Slot.AttemptID] = next
						}
						return e
					})
					if e != nil {
						seamErr = e
						return e
					}
					commits++
				}
				// Preserve the original ACK and its intentionally stale return.
				return nil
			}
			result, e := f.b.RunAssessmentOnce(context.Background(), c)
			if !fired.Load() || seamErr != nil || commits != 2 {
				t.Fatal("legal finalization seam or negative commits missing")
			}
			if !errors.Is(e, x.ErrHeld) || result.Code != "HELD" {
				t.Fatal("uncertain denied run did not remain held")
			}
			scope, a, readErr := f.b.assessmentAttempt(c.Binding, *c.Slot)
			if readErr != nil || scope.DenyLatched || scope.State != "sealed" || a.State != "held" || a.Outcome != "unknown" || !a.DenyLatched || a.FailureCode != "io_uncertain" || !a.ReaderCloseUncertain || a.EffectsClosed() || a.SerialClear() || !a.ReaderCloseConfirmed || !a.CollectorCloseConfirmed || !a.OwnershipCloseConfirmed || a.CollectorCloseUncertain || a.OwnershipCloseUncertain || a.Compute.ReservedCPUSeconds != 30 {
				t.Fatal("current uncertainty lost or stale finalization promoted terminal")
			}
			settled := stage == "after-settle"
			if a.AccountingUnresolved == settled || a.ComputeCleanupObserved != settled || !settled && (a.Compute.ChargedCPUSeconds != 0 || a.Compute.Provenance != "reserved") || settled && (a.Compute.ChargedCPUSeconds != 30 || a.Compute.Provenance != "estimated_upper_bound") {
				t.Fatal("uncertainty fabricated settlement or refunded committed charge")
			}
			for _, phase := range []*x.PhaseRecord{a.PhaseRecords.Workload, a.PhaseRecords.Checker} {
				if phase == nil || !phase.CreateReturned || !phase.StartReturned || phase.ResourceIdentity == nil || !phase.TransportCloseConfirmed || phase.TransportCloseUncertain || !phase.StoppedObserved || !phase.RemovalObserved {
					t.Fatal("actual original phase closure history lost")
				}
			}
			f.p.mu.Lock()
			physicalClosed := len(f.p.resources) == 2 && f.p.starts == 2 && f.p.active == 0
			for _, resource := range f.p.resources {
				physicalClosed = physicalClosed && !resource.Exists && resource.Stopped
			}
			f.p.mu.Unlock()
			if !physicalClosed {
				t.Fatal("original resources not actually removed or extra dispatch")
			}
			if _, e := f.b.assessment.owned.root.Lstat(scope.ScopeID); e != nil {
				t.Fatal("negative finalization removed retained scope")
			}
		})
	}
}
