package broker

import (
	"bytes"
	"context"
	"io"
	"sync"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

func (b *Broker) assessmentBeginAttempt(c x.Control) (x.AssessmentAttempt, *ledger.AssessmentOperation, bool, error) {
	a, op, fresh, e := b.Store.BeginAssessmentAttemptV7(c, b.assessmentRoots(), b.assessmentBeginGuard(c.Binding, false))
	if e == nil && b.assessmentFault("after-journal:claim") != nil {
		return a, nil, false, x.ErrHeld
	}
	return a, op, fresh, assessmentError(e)
}

// RunAssessmentOnce admits exactly one immutable planned slot through the real
// Store. No public production construction can supply the private capability.
// Same-ID lookup and uncertain ACK never continue an execution.
func (b *Broker) RunAssessmentOnce(ctx context.Context, c x.Control) (result x.Result, err error) {
	if !b.assessmentAvailable() {
		return result, x.ErrDisabled
	}
	if c.Operation != "execute" || c.Validate() != nil {
		return result, x.ErrInvalid
	}
	s, _, e := b.assessmentRead(c.Binding)
	if e != nil {
		return result, e
	}
	a, owner, fresh, e := b.assessmentBeginAttempt(c)
	if e != nil {
		return result, e
	}
	if !fresh {
		return b.assessmentResult(c, "existing_attempt")
	}
	ownershipReady := false
	defer func() {
		if !ownershipReady {
			_, _ = b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptFinish, func(v *x.AssessmentAttempt) error {
				if v.State != "held" && v.State != "terminal" {
					v.State = "finishing"
				}
				return nil
			})
			n, _ := b.assessmentNow()
			_ = owner.Abort(owneddata.Stamp(n.Wall), n.TickMS)
			_ = owner.CloseAndCommitOwnership()
		}
	}()
	if owner.AcquireExecutionSet() != nil || owner.FreshCurrent(b.assessmentBeginGuard(c.Binding, false)) != nil {
		return result, x.ErrHeld
	}
	// Deadline includes preparation, both phases, collector and finalization. A
	// deadline never renews when a caller returns, a checker starts, or we reopen.
	deadline, _ := owneddata.Time(a.Deadline, false)
	runCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	var input io.ReadCloser
	var collector assessmentCollector
	localUncertain := false
	collectorUncertain := false
	outputBudget := &assessmentOutputBudget{limit: s.Limits.OutputBudgetBytes, cancel: cancel}
	defer func() {
		// Finish is irreversible suppression of any uncreated checker. Global
		// close is never published at the workload/checker boundary.
		_, _ = b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptFinish, func(v *x.AssessmentAttempt) error {
			if v.State == "held" || v.State == "terminal" || v.State == "finishing" {
				return nil
			}
			v.State = "finishing"
			return nil
		})
		// Exact phase teardown must precede any attempt-wide close proof. A
		// parent descriptor close cannot prove a possibly inherited reader drained.
		_ = b.assessmentCleanupPhases(context.WithoutCancel(ctx), c)
		// Close both local owners before publishing either global confirmation.
		var inputCloseErr error
		if input != nil {
			inputCloseErr = input.Close()
			localUncertain = localUncertain || inputCloseErr != nil
		}
		if collector != nil {
			collectorUncertain = collector.Close() != nil || collectorUncertain
			localUncertain = localUncertain || collectorUncertain
		}
		_, closing, closeReadErr := b.assessmentAttempt(c.Binding, *c.Slot)
		phaseCloseReady := closeReadErr == nil && closing.GlobalCloseReady()
		if input != nil {
			ce := inputCloseErr
			event := x.AttemptReaderClosed
			if ce != nil {
				event = x.AttemptReaderUncertain
			}
			_, pe := b.assessmentChangeAttempt(c.Binding, *c.Slot, event, func(v *x.AssessmentAttempt) error {
				if ce != nil {
					v.ReaderCloseUncertain = true
				} else if phaseCloseReady {
					v.ReaderCloseConfirmed = true
				}
				return nil
			})
			if pe != nil {
				localUncertain = true
				_, _ = b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptReaderUncertain, func(v *x.AssessmentAttempt) error { v.ReaderCloseUncertain = true; return nil })
			}
		}
		if collector != nil {
			event := x.AttemptCollectorClosed
			if collectorUncertain {
				event = x.AttemptCollectorUncertain
			}
			_, pe := b.assessmentChangeAttempt(c.Binding, *c.Slot, event, func(v *x.AssessmentAttempt) error {
				if collectorUncertain {
					v.CollectorCloseUncertain = true
				} else if phaseCloseReady {
					v.CollectorCloseConfirmed = true
				}
				return nil
			})
			if pe != nil {
				localUncertain = true
				_, _ = b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptCollectorUncertain, func(v *x.AssessmentAttempt) error { v.CollectorCloseUncertain = true; return nil })
			}
		}
		if !localUncertain {
			if e := owner.CloseAndCommitOwnership(); e != nil {
				localUncertain = true
			}
		} else {
			n, _ := b.assessmentNow()
			_ = owner.Abort(owneddata.Stamp(n.Wall), n.TickMS)
			_ = owner.CloseAndCommitOwnership()
		}
		ownershipReady = true
		var re error
		if localUncertain {
			_, _ = b.assessmentDeny(c.Binding, "io_uncertain")
			err = x.ErrHeld
			result, re = b.assessmentResult(c, "new_attempt")
		} else {
			var finalizeErr error
			result, finalizeErr, re = b.assessmentFinalizeResult(context.WithoutCancel(ctx), c)
			if finalizeErr != nil {
				err = x.ErrHeld
			}
		}
		if re != nil {
			err = re
		}
		_ = b.assessmentCleanup(context.WithoutCancel(ctx), c.Binding)
	}()
	fail := func(code string) error {
		_, _ = b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptDeny, func(v *x.AssessmentAttempt) error {
			if v.DenyLatched {
				return nil
			}
			v.DenyLatched = true
			v.FailureCode = code
			return nil
		})
		return x.ErrHeld
	}
	if runCtx.Err() != nil {
		return result, fail("expired")
	}
	if _, e = b.assessmentFresh(c.Binding, c.Slot); e != nil {
		return result, fail("access_ended")
	}
	if _, e = b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptForward, func(v *x.AssessmentAttempt) error { v.State = "preparing"; return nil }); e != nil {
		return result, fail("io_uncertain")
	}
	if runCtx.Err() != nil {
		return result, fail("expired")
	}
	if _, e = b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptReader, func(v *x.AssessmentAttempt) error {
		if runCtx.Err() != nil {
			return x.ErrDenied
		}
		v.ReaderStarted = true
		return nil
	}); e != nil {
		return result, fail("io_uncertain")
	}
	if _, e = b.assessmentFresh(c.Binding, c.Slot); e != nil {
		return result, fail("access_ended")
	}
	if runCtx.Err() != nil {
		return result, fail("expired")
	}
	f, e := owner.OpenInput()
	if e != nil {
		return result, fail("io_uncertain")
	}
	input = f
	if runCtx.Err() != nil {
		return result, fail("expired")
	}
	if _, e = b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptCollector, func(v *x.AssessmentAttempt) error {
		if runCtx.Err() != nil {
			return x.ErrDenied
		}
		v.CollectorStarted = true
		return nil
	}); e != nil {
		return result, fail("io_uncertain")
	}
	if _, e = b.assessmentFresh(c.Binding, c.Slot); e != nil {
		return result, fail("access_ended")
	}
	if runCtx.Err() != nil {
		return result, fail("expired")
	}
	collector, e = b.assessment.collector(runCtx, s)
	if e != nil || collector == nil {
		collectorUncertain = true
		localUncertain = true
		_, _ = b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptCollectorUncertain, func(v *x.AssessmentAttempt) error { v.CollectorCloseUncertain = true; return nil })
		return result, fail("measurement_unknown")
	}
	if _, e = b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptForward, func(v *x.AssessmentAttempt) error { v.State = "running"; return nil }); e != nil {
		return result, fail("io_uncertain")
	}
	work, workMetric, e := b.assessmentPhase(runCtx, c, s, "workload", f, nil, collector, outputBudget)
	defer clear(work)
	if e != nil {
		if outputBudget.exceeded() {
			return result, fail("output_limit")
		}
		return result, fail("execution_failed")
	}
	if _, e = b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptForward, func(v *x.AssessmentAttempt) error { v.State = "checking"; return nil }); e != nil {
		return result, fail("io_uncertain")
	}
	if _, e = f.Seek(0, io.SeekStart); e != nil {
		return result, fail("io_uncertain")
	}
	check, checkMetric, e := b.assessmentPhase(runCtx, c, s, "checker", f, work, collector, outputBudget)
	defer clear(check)
	if e != nil {
		if outputBudget.exceeded() {
			return result, fail("output_limit")
		}
		return result, fail("execution_failed")
	}
	quality, e := b.assessment.checkQuality(check)
	if e != nil || quality != "passed" && quality != "failed" {
		return result, fail("quality_failed")
	}
	_, e = b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptQuality, func(v *x.AssessmentAttempt) error {
		v.Quality.Status = quality
		v.Quality.Source = "unverified_checker"
		return nil
	})
	if e != nil {
		return result, fail("io_uncertain")
	}
	metrics, e := collector.Finish(runCtx)
	if e != nil || metrics.Validate() != nil {
		return result, fail("measurement_unknown")
	}
	_, e = b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptMetrics, func(v *x.AssessmentAttempt) error {
		v.Metrics = x.Measurements{Workload: workMetric, Checker: checkMetric, Attempt: metrics}
		digest := x.Digest(v.Metrics)
		v.MeasurementDigest = &digest
		return nil
	})
	if e != nil {
		return result, fail("io_uncertain")
	}
	if quality != "passed" {
		return result, fail("quality_failed")
	}
	return result, nil
}

type assessmentCapture struct {
	mu       sync.Mutex
	budget   *assessmentOutputBudget
	out      bytes.Buffer
	overflow bool
	sealed   bool
}
type assessmentOutputBudget struct {
	mu          sync.Mutex
	limit, used int64
	overflow    bool
	cancel      context.CancelFunc
}

func (b *assessmentOutputBudget) exceeded() bool { b.mu.Lock(); defer b.mu.Unlock(); return b.overflow }

type assessmentCaptureWriter struct {
	capture *assessmentCapture
	stderr  bool
}

func (w assessmentCaptureWriter) Write(p []byte) (int, error) {
	c := w.capture
	c.mu.Lock()
	if c.sealed {
		c.mu.Unlock()
		return 0, x.ErrDenied
	}
	c.budget.mu.Lock()
	if int64(len(p)) > c.budget.limit-c.budget.used {
		c.overflow = true
		c.budget.overflow = true
		cancel := c.budget.cancel
		c.budget.mu.Unlock()
		c.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return 0, x.ErrDenied
	}
	c.budget.used += int64(len(p))
	if !w.stderr {
		_, _ = c.out.Write(p)
	}
	c.budget.mu.Unlock()
	c.mu.Unlock()
	return len(p), nil
}

func (b *Broker) assessmentPhase(ctx context.Context, c x.Control, s x.ExecutionScope, phase string, input io.Reader, prior []byte, collector assessmentCollector, budget *assessmentOutputBudget) (output []byte, metric x.MetricSet, err error) {
	var capture *assessmentCapture
	// Registered first so retirement/wipe runs after transport teardown on all
	// paths. Saved writers share this same sealed capture after phase return.
	defer func() {
		if capture != nil {
			capture.mu.Lock()
			capture.sealed = true
			clear(capture.out.Bytes())
			capture.out.Reset()
			capture.mu.Unlock()
		}
		// A copied successful output must not survive a later deferred failure.
		if err != nil {
			clear(output)
			output = nil
		}
	}()
	metric = x.UnknownMetrics()
	if ctx.Err() != nil {
		return nil, metric, x.ErrDenied
	}
	if _, e := b.assessmentFresh(c.Binding, c.Slot); e != nil {
		return nil, metric, e
	}
	get := func(a *x.AssessmentAttempt) *x.PhaseRecord {
		if phase == "workload" {
			return a.PhaseRecords.Workload
		}
		return a.PhaseRecords.Checker
	}
	change := func(event x.Event, fn func(*x.PhaseRecord)) error {
		if event == x.AttemptPhaseForward && ctx.Err() != nil {
			return x.ErrDenied
		}
		_, e := b.assessmentChangeAttempt(c.Binding, *c.Slot, event, func(a *x.AssessmentAttempt) error {
			// Recheck after journal-lock acquisition; safety publication stays allowed.
			if event == x.AttemptPhaseForward && ctx.Err() != nil {
				return x.ErrDenied
			}
			p := get(a)
			if p == nil {
				return x.ErrHeld
			}
			fn(p)
			return nil
		})
		return e
	}
	if ctx.Err() != nil {
		return nil, metric, x.ErrDenied
	}
	_, e := b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptPhase, func(a *x.AssessmentAttempt) error {
		if ctx.Err() != nil {
			return x.ErrDenied
		}
		p := &x.PhaseRecord{Phase: phase, OwnerName: x.ResourceName(c.Binding.BrokerAuthorityID, c.Slot.AttemptID, phase)}
		if phase == "workload" {
			a.PhaseRecords.Workload = p
		} else {
			a.PhaseRecords.Checker = p
		}
		return nil
	})
	if e != nil {
		return nil, metric, e
	}
	if e = change(x.AttemptPhaseForward, func(p *x.PhaseRecord) { p.TransportStarted = true }); e != nil {
		return nil, metric, e
	}
	if _, e = b.assessmentFresh(c.Binding, c.Slot); e != nil {
		return nil, metric, e
	}
	// Fresh/journal work may consume the caller deadline. A committed claim
	// remains unresolved if this last negative gate suppresses its callback.
	if ctx.Err() != nil {
		return nil, metric, x.ErrDenied
	}
	transport, e := b.assessment.transport(ctx, s, phase)
	constructorUncertain := e != nil || transport == nil
	defer func() {
		if transport == nil {
			return
		}
		ce := transport.Close()
		uncertain := constructorUncertain || ce != nil
		event := x.AttemptPhaseSafety
		pe := change(event, func(p *x.PhaseRecord) {
			if !uncertain {
				p.TransportCloseConfirmed = true
			} else {
				p.TransportCloseUncertain = true
			}
		})
		if pe != nil {
			_ = change(x.AttemptPhaseSafety, func(p *x.PhaseRecord) { p.TransportCloseUncertain = true })
		}
		if uncertain || pe != nil {
			err = x.ErrHeld
		}
	}()
	if constructorUncertain {
		_ = change(x.AttemptPhaseSafety, func(p *x.PhaseRecord) { p.TransportCloseUncertain = true })
		return nil, metric, x.ErrHeld
	}
	if ctx.Err() != nil {
		return nil, metric, x.ErrDenied
	}
	if e = b.assessment.storage.BeforePhase(ctx, s, phase); e != nil {
		return nil, metric, x.ErrHeld
	}
	if e = change(x.AttemptPhaseForward, func(p *x.PhaseRecord) { p.CreateClaimed = true }); e != nil {
		return nil, metric, e
	}
	if _, e = b.assessmentFresh(c.Binding, c.Slot); e != nil {
		return nil, metric, e
	}
	_, a, e := b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil {
		return nil, metric, e
	}
	req, e := assessmentPhaseRequestFor(s, a, phase)
	if e != nil {
		return nil, metric, e
	}
	if ctx.Err() != nil {
		return nil, metric, x.ErrDenied
	}
	resource, e := b.assessment.provider.Create(ctx, req)
	if e != nil {
		_ = change(x.AttemptPhaseSafety, func(p *x.PhaseRecord) { p.CreateAmbiguous = true })
		return nil, metric, x.ErrHeld
	}
	if resource.BrokerOwner != c.Binding.BrokerAuthorityID || resource.AttemptID != c.Slot.AttemptID || resource.Phase != phase || resource.ScopeID != s.ScopeID || resource.CapabilitySHA256 != s.CapabilitySHA256 || !owneddata.Hex(resource.ContainerID, 32) {
		_ = change(x.AttemptPhaseSafety, func(p *x.PhaseRecord) { p.CreateAmbiguous = true })
		return nil, metric, x.ErrHeld
	}
	// Publish the actual successful Create and exact identity even if the caller
	// canceled during Create; original cleanup still needs these safety facts.
	if e = change(x.AttemptPhaseSafety, func(p *x.PhaseRecord) { p.CreateReturned = true }); e != nil {
		return nil, metric, e
	}
	if e = change(x.AttemptPhaseSafety, func(p *x.PhaseRecord) { p.ResourceIdentity = &resource }); e != nil {
		return nil, metric, e
	}
	if e = change(x.AttemptPhaseForward, func(p *x.PhaseRecord) { p.StartClaimed = true }); e != nil {
		return nil, metric, e
	}
	if _, e = b.assessmentFresh(c.Binding, c.Slot); e != nil {
		return nil, metric, e
	}
	capture = &assessmentCapture{budget: budget}
	if ctx.Err() != nil {
		return nil, metric, x.ErrDenied
	}
	completion, e := b.assessment.provider.Start(ctx, resource, input, prior, assessmentCaptureWriter{capture, false}, assessmentCaptureWriter{capture, true})
	capture.mu.Lock()
	capture.sealed = true
	capture.mu.Unlock()
	if e != nil {
		return nil, metric, x.ErrHeld
	}
	if e = change(x.AttemptPhaseSafety, func(p *x.PhaseRecord) { p.StartReturned = true }); e != nil {
		return nil, metric, e
	}
	if completion.ExitCode < 0 || completion.ExitCode > 255 {
		return nil, metric, x.ErrHeld
	}
	if e = change(x.AttemptPhaseSafety, func(p *x.PhaseRecord) {
		p.StartedObserved = true
		if completion.StartedAt != nil {
			at := owneddata.Stamp(*completion.StartedAt)
			p.StartedAt = &at
		}
	}); e != nil {
		return nil, metric, e
	}
	if completion.FinishedAt != nil {
		at := owneddata.Stamp(*completion.FinishedAt)
		if e = change(x.AttemptPhaseSafety, func(p *x.PhaseRecord) { p.ExitCode = &completion.ExitCode; p.FinishedAt = &at }); e != nil {
			return nil, metric, e
		}
	}
	metric, e = collector.Observe(ctx, resource)
	if e != nil || metric.Validate() != nil {
		return nil, x.UnknownMetrics(), x.ErrHeld
	}
	observation, e := b.assessment.provider.Inspect(ctx, req)
	if e != nil || observation.Identity != resource || !observation.Stopped {
		return nil, metric, x.ErrHeld
	}
	if e = change(x.AttemptPhaseSafety, func(p *x.PhaseRecord) { p.StoppedObserved = true }); e != nil {
		return nil, metric, e
	}
	if e = b.assessment.provider.Remove(ctx, resource); e != nil {
		return nil, metric, x.ErrHeld
	}
	observation, e = b.assessment.provider.Inspect(ctx, req)
	if e != nil || observation.Exists {
		return nil, metric, x.ErrHeld
	}
	if e = change(x.AttemptPhaseSafety, func(p *x.PhaseRecord) { p.RemovalObserved = true }); e != nil {
		return nil, metric, e
	}
	if e = b.assessment.storage.AfterPhase(ctx, s, phase); e != nil {
		return nil, metric, x.ErrHeld
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if capture.overflow || completion.ExitCode != 0 {
		return nil, metric, x.ErrDenied
	}
	output = append([]byte(nil), capture.out.Bytes()...)
	clear(capture.out.Bytes())
	return output, metric, nil
}

func (b *Broker) assessmentResult(c x.Control, disposition string) (x.Result, error) {
	s, a, e := b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil {
		return x.Result{}, e
	}
	return assessmentResultProjection(c, disposition, s, a)
}

// The caller supplies one exact fresh observation. This pure untrusted reply
// neither retains that snapshot nor authorizes claims, effects or scope cleanup.
func assessmentResultProjection(c x.Control, disposition string, s x.ExecutionScope, a x.AssessmentAttempt) (x.Result, error) {
	code := "HELD"
	if a.SerialClear() {
		code = "OK"
	}
	set, e := x.Project(s, a)
	if e != nil {
		return x.Result{}, e
	}
	digest := x.Digest(set)
	return x.Result{Schema: x.ResultSchema, Operation: "execute", Code: code, Disposition: disposition, Binding: c.Binding, Slot: *c.Slot, HandoffNonce: a.HandoffNonce, BindingDigest: a.BindingDigest, ScopeGeneration: s.Generation, Attempt: a.Public(), Compute: a.Compute, AccountingUnresolved: a.AccountingUnresolved, NormalizedReceipts: &set, NormalizedReceiptsSHA256: &digest, Trust: x.Untrusted()}, nil
}
