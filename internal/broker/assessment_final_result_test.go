package broker

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/assessment"
	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
)

func executionFinalResultClosed(a x.AssessmentAttempt) bool {
	return a.ReaderStarted && a.CollectorStarted && a.ReaderCloseConfirmed && a.CollectorCloseConfirmed && a.OwnershipCloseConfirmed && !a.ReaderCloseUncertain && !a.CollectorCloseUncertain && !a.OwnershipCloseUncertain && a.EffectsClosed() && !a.AccountingUnresolved && a.ComputeCleanupObserved && a.Compute.ReservedCPUSeconds == 30 && a.Compute.ChargedCPUSeconds == 30 && a.Compute.Provenance == "estimated_upper_bound"
}

// Only actual original Run supplies positive facts. This mutation adds one
// legal negative to an already settled, checked-close terminal record.
func executionFinalResultReaderUncertain(b *Broker, c x.Control) error {
	return b.Store.Update(func(d *ledger.Data) error {
		if d.Version != 7 || d.AssessmentExecution == nil {
			return x.ErrInvalid
		}
		s, scopeOK := d.AssessmentExecution.Scopes[c.Binding.ScopeID]
		a, attemptOK := d.AssessmentExecution.Attempts[c.Slot.AttemptID]
		if !scopeOK || !attemptOK || s.Binding() != c.Binding || a.Binding != c.Binding || a.Slot != *c.Slot || a.State != "terminal" || a.Outcome != "succeeded" || a.DenyLatched || !executionFinalResultClosed(a) || !a.SerialClear() {
			return x.ErrInvalid
		}
		next, e := x.AdvanceAttempt(a, s, x.AttemptReaderUncertain, a.WallHighWater, a.TickHighWaterMS, func(v *x.AssessmentAttempt) { v.ReaderCloseUncertain = true })
		if e == nil {
			d.AssessmentExecution.Attempts[c.Slot.AttemptID] = next
		}
		return e
	})
}

func executionFinalResultPhysicalClosed(t *testing.T, f *executionFixture) {
	t.Helper()
	f.p.mu.Lock()
	defer f.p.mu.Unlock()
	if len(f.p.requests) != 2 || f.p.starts != 2 || f.p.active != 0 || f.p.maxActive != 1 || len(f.p.resources) != 2 {
		t.Fatal("original phase effects were missing or repeated")
	}
	for _, resource := range f.p.resources {
		if resource.Exists || !resource.Stopped {
			t.Fatal("original phase resource was not actually removed")
		}
	}
}

func executionFinalResultCalls(p *executionStopRecheckProvider) [5]int32 {
	return [5]int32{p.creates.Load(), p.starts.Load(), p.inspects.Load(), p.stops.Load(), p.removes.Load()}
}

func executionFinalResultWire(t *testing.T, c x.Control, s x.ExecutionScope, a x.AssessmentAttempt, r x.Result) []byte {
	t.Helper()
	if !executionFinalResultClosed(a) || !a.SerialClear() || a.State != "terminal" || a.Outcome != "succeeded" || a.DenyLatched || r.Schema != x.ResultSchema || r.Operation != "execute" || r.Code != "OK" || r.Disposition != "new_attempt" || r.Binding != c.Binding || r.Slot != *c.Slot || r.HandoffNonce != *c.HandoffNonce || r.BindingDigest != x.AttemptBindingDigest(c.Binding, *c.Slot, *c.HandoffNonce) || r.ScopeGeneration != s.Generation || r.Attempt == nil || r.Attempt.Generation != a.Generation || r.Attempt.State != "terminal" || r.Attempt.Outcome != "succeeded" || r.Compute.ReservedCPUSeconds != 30 || r.Compute.ChargedCPUSeconds != 30 || r.Compute.Provenance != "estimated_upper_bound" || r.AccountingUnresolved || r.Trust != x.Untrusted() || r.NormalizedReceipts == nil || r.NormalizedReceiptsSHA256 == nil {
		t.Fatal("actual closed Run lost public binding, accounting or trust")
	}
	set := r.NormalizedReceipts
	m, cleanup := set.Measurement, set.Cleanup
	if set.Schema != assessment.SetSchemaV2 || m.Schema != assessment.MeasurementSchemaV2 || cleanup.Schema != assessment.CleanupSchemaV2 || m.Binding.AttemptID != c.Slot.AttemptID || m.Binding.ReservationID != c.Slot.ReservationID || m.Binding.ManifestSHA256 != s.ManifestSHA256 || m.Binding.ProjectID != c.Binding.ProjectID || cleanup.Binding != m.Binding || m.Execution.Status != "succeeded" || !m.Execution.Admitted || !m.Execution.ExecutionStarted || m.Execution.CancelRequested || m.Execution.ExitCode == nil || *m.Execution.ExitCode != 0 || m.Execution.FailureCategory != "none" || m.Execution.DiagnosticsAvailable || m.ReservationCeiling.CPUSeconds != 30 || m.ReservationCeiling.Source != "unverified_profile_reservation" || m.ConservativeAccounting.ChargedCPUSeconds != 30 || m.ConservativeAccounting.Provenance != "estimated_upper_bound" || m.ConservativeAccounting.UnresolvedLiability || m.ProviderReportedCharge.Availability != "unknown" || m.ProviderReportedCharge.Value != nil || m.ProviderReportedCharge.Source != "none" || m.Quality.Status != "passed" || m.Observation.SourceKind != "synthetic_fixture" || m.Observation.Authenticated || m.Observation.IndependentlyVerified || cleanup.Observation != m.Observation {
		t.Fatal("actual synthetic measurement was relabeled or authenticated")
	}
	wantScopes := []assessment.ObservedScope{{Kind: "execution_resource", ScopeID: c.Slot.AttemptID, Deletion: "reported_complete"}, {Kind: "temporary_data", ScopeID: s.AssessmentID, Deletion: "unknown"}, {Kind: "recovery_copies", ScopeID: s.AssessmentID, Deletion: "unknown"}}
	wantLiabilities := []string{"termination_unresolved", "access_revocation_unknown", "data_cleanup_unknown", "recovery_cleanup_unknown"}
	if cleanup.ReceiptID != a.ClaimEpoch || m.CleanupReceiptID != a.ClaimEpoch || cleanup.Method.Name != "unverified_scope_observation" || cleanup.Method.Version != 1 || cleanup.Termination != "unknown" || cleanup.AccessRevocation != "unknown" || cleanup.Outcome != "unresolved" || !cleanup.Quarantine || !cleanup.ReuseDenied || !reflect.DeepEqual(cleanup.ObservedScopes, wantScopes) || !reflect.DeepEqual(cleanup.RemainingLiabilities, wantLiabilities) || m.CleanupReceiptSHA256 != x.Digest(cleanup) || *r.NormalizedReceiptsSHA256 != x.Digest(set) {
		t.Fatal("public observation fabricated trusted cleanup or changed receipt binding")
	}
	raw, e := x.EncodeReply(r)
	if e != nil {
		t.Fatal("closed result did not encode")
	}
	if _, e = x.DecodeResult(raw, c, s, &a); e != nil {
		t.Fatal("strict shared codec rejected exact actual result")
	}
	return raw
}

func TestExecutionFinalResultFreshObservationRejectsLateUncertainty(t *testing.T) {
	f := newExecutionFixture(t)
	c := executionControl(f, f.seal(t), 0)
	var fired atomic.Bool
	var seamErr error
	var terminalGeneration int64
	// Installed after seal: only the original parent sees this outside-lock hook.
	f.b.assessment.owned.fault = func(point string) error {
		if point != "after-journal:"+string(x.AttemptFinish) {
			return nil
		}
		_, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
		if e != nil {
			seamErr = e
			return e
		}
		if a.State != "terminal" || !fired.CompareAndSwap(false, true) {
			return nil
		}
		if !executionFinalResultClosed(a) || !a.SerialClear() {
			seamErr = x.ErrInvalid
			return seamErr
		}
		terminalGeneration = a.Generation
		seamErr = executionFinalResultReaderUncertain(f.b, c)
		// Preserve a successful terminal mutation ACK carrying its older value.
		return seamErr
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, e := f.b.RunAssessmentOnce(ctx, c)
	if ctx.Err() != nil {
		t.Fatal("late-negative control lost its live caller precondition")
	}
	if !fired.Load() || seamErr != nil || !errors.Is(e, x.ErrHeld) || r.Code != "HELD" {
		t.Fatal("final observation accepted stale terminal mutation state")
	}
	s, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil || s.DenyLatched || s.State != "sealed" || a.State != "terminal" || a.Outcome != "succeeded" || !a.ReaderCloseConfirmed || !a.ReaderCloseUncertain || !a.CollectorCloseConfirmed || !a.OwnershipCloseConfirmed || a.CollectorCloseUncertain || a.OwnershipCloseUncertain || a.SerialClear() || a.EffectsClosed() || a.AccountingUnresolved || !a.ComputeCleanupObserved || a.Compute.ReservedCPUSeconds != 30 || a.Compute.ChargedCPUSeconds != 30 || a.Compute.Provenance != "estimated_upper_bound" || a.Generation != terminalGeneration+1 || r.Attempt == nil || r.Attempt.Generation != a.Generation {
		t.Fatal("current negative or committed full charge was lost")
	}
	executionFinalResultPhysicalClosed(t, f)
	if _, e = f.b.assessment.owned.root.Lstat(s.ScopeID); e != nil {
		t.Fatal("negative observation removed retained scope")
	}
}

func TestExecutionFinalResultHistoricalObservationCannotClearCurrentHold(t *testing.T) {
	f := newExecutionFixture(t)
	c := executionControl(f, f.seal(t), 0)
	// Count all parent provider calls; the already-launched watcher retains its
	// original boundary. No historical public result can launch another effect.
	probe := &executionStopRecheckProvider{assessmentProvider: f.b.assessment.provider}
	f.b.assessment.provider = probe
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r, e := f.b.RunAssessmentOnce(ctx, c)
	if e != nil || ctx.Err() != nil {
		t.Fatal("actual original Run failed or caller ended")
	}
	s, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil {
		t.Fatal("actual completed state unavailable")
	}
	raw := executionFinalResultWire(t, c, s, a, r)
	executionFinalResultPhysicalClosed(t, f)
	if e = executionFinalResultReaderUncertain(f.b, c); e != nil {
		t.Fatal("legal later uncertainty did not commit")
	}
	s, current, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil || !current.ReaderCloseUncertain || current.SerialClear() || current.Generation != a.Generation+1 || current.Compute != a.Compute || current.AccountingUnresolved || current.State != "terminal" || current.Outcome != "succeeded" {
		t.Fatal("later negative cleared history or changed full charge")
	}
	if r.Code != "OK" || r.Attempt.Generation != a.Generation || r.Trust != x.Untrusted() {
		t.Fatal("historical observation was rewritten as current authority")
	}
	if _, e = x.DecodeResult(raw, c, s, &current); e == nil {
		t.Fatal("historical public bytes matched later trusted journal state")
	}
	beforeCalls := executionFinalResultCalls(probe)
	if ctx.Err() != nil {
		t.Fatal("historical control lost its live lookup precondition")
	}
	lookup, e := f.b.RunAssessmentOnce(ctx, c)
	if e != nil || lookup.Code != "HELD" || lookup.Disposition != "existing_attempt" || lookup.Attempt == nil || lookup.Attempt.Generation != current.Generation {
		t.Fatal("same-ID lookup reused historical clearance")
	}
	second := executionControl(f, s.Status("status", "OK", "observed_current", nil), 1)
	if ctx.Err() != nil {
		t.Fatal("distinct-slot control lost its live caller precondition")
	}
	if _, e = f.b.RunAssessmentOnce(ctx, second); e == nil {
		t.Fatal("later uncertainty admitted a distinct preplanned slot")
	}
	if ctx.Err() != nil {
		t.Fatal("distinct-slot control ended its caller during refusal")
	}
	if executionFinalResultCalls(probe) != beforeCalls {
		t.Fatal("historical observation or refused slot dispatched provider effects")
	}
	if e = f.b.Store.View(func(d ledger.Data) error {
		if len(d.AssessmentExecution.Attempts) != 1 {
			return x.ErrInvalid
		}
		return nil
	}); e != nil {
		t.Fatal("refused distinct slot created another attempt")
	}
	u, e := f.b.Store.Usage(c.Binding.ProjectID)
	if e != nil || u.Charged != 30 || u.Reserved != 0 || u.Remaining != 30 {
		t.Fatal("historical result changed exact accounting")
	}
	_, after, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil || !reflect.DeepEqual(current, after) {
		t.Fatal("lookup or refused slot cleared the current hold")
	}
	if _, e = f.b.assessment.owned.root.Lstat(s.ScopeID); e != nil {
		t.Fatal("historical result removed retained scope")
	}
}

func TestExecutionFinalResultTerminalPublicationFaults(t *testing.T) {
	for _, mode := range []string{"before", "after"} {
		t.Run(mode, func(t *testing.T) {
			f := newExecutionFixture(t)
			c := executionControl(f, f.seal(t), 0)
			var fired atomic.Bool
			var targets atomic.Int32
			var seamErr error
			f.b.assessment.owned.fault = func(point string) error {
				if point != mode+"-journal:"+string(x.AttemptFinish) {
					return nil
				}
				_, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
				if e != nil {
					seamErr = e
					return e
				}
				state := "held"
				if mode == "after" {
					state = "terminal"
				}
				if a.State != state || a.AccountingUnresolved {
					return nil
				}
				targets.Add(1)
				if !executionFinalResultClosed(a) || a.DenyLatched || mode == "before" && a.Outcome != "unknown" || mode == "after" && (a.Outcome != "succeeded" || !a.SerialClear()) {
					seamErr = x.ErrInvalid
					return seamErr
				}
				if !fired.CompareAndSwap(false, true) {
					return nil
				}
				return x.ErrHeld
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			r, e := f.b.RunAssessmentOnce(ctx, c)
			if ctx.Err() != nil {
				t.Fatal("terminal fault control lost its live caller precondition")
			}
			if !fired.Load() || targets.Load() != 1 || seamErr != nil || !errors.Is(e, x.ErrHeld) {
				t.Fatal("terminal fault retried or lost original finalization error")
			}
			s, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
			if e != nil || s.DenyLatched || s.State != "sealed" || !executionFinalResultClosed(a) || a.DenyLatched {
				t.Fatal("terminal fault changed actual closure or committed accounting")
			}
			if mode == "before" {
				if a.State != "held" || a.Outcome != "unknown" || a.SerialClear() || r.Code != "HELD" {
					t.Fatal("failed terminal publication manufactured clearance")
				}
			} else if a.State != "terminal" || a.Outcome != "succeeded" || !a.SerialClear() || r.Code != "OK" {
				t.Fatal("lost ACK did not retain truthful observational terminal result")
			}
			if r.Attempt == nil || r.Attempt.Generation != a.Generation || r.Compute != a.Compute || r.AccountingUnresolved || r.Disposition != "new_attempt" || r.Trust != x.Untrusted() {
				t.Fatal("legacy fallback lost current public observation")
			}
			executionFinalResultPhysicalClosed(t, f)
			u, e := f.b.Store.Usage(c.Binding.ProjectID)
			if e != nil || u.Charged != 30 || u.Reserved != 0 || u.Remaining != 30 {
				t.Fatal("terminal fault retried or refunded accounting")
			}
		})
	}
}
