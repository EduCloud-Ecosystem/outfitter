package broker

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
)

type executionCleanupACKCollector struct {
	assessmentCollector
	fail   bool
	failed *atomic.Bool
}

func (c *executionCleanupACKCollector) Observe(ctx context.Context, id x.DockerResourceIdentity) (x.MetricSet, error) {
	metric, e := c.assessmentCollector.Observe(ctx, id)
	if e == nil && c.fail {
		c.failed.Store(true)
		return metric, x.ErrHeld
	}
	return metric, e
}

type executionCleanupACKState struct {
	mode                                                               string
	b                                                                  *Broker
	c                                                                  x.Control
	caller                                                             context.Context
	collectorFailed, transportClosed, deferred, entered, active, ended atomic.Bool
	absenceSeen, faulted                                               atomic.Bool
	before, after, negativeEvents                                      atomic.Int32
	cleanupCtx                                                         context.Context
	entryScope, endScope                                               x.ExecutionScope
	entry, end                                                         x.AssessmentAttempt
	removeAtEntry                                                      int32
	liveAtBoundary                                                     bool
	seamErr                                                            error
}

type executionCleanupACKProvider struct {
	*executionStopRecheckProvider
	state *executionCleanupACKState
}

func (p *executionCleanupACKProvider) Inspect(ctx context.Context, request assessmentPhaseRequest) (assessmentResourceObservation, error) {
	observation, e := p.executionStopRecheckProvider.Inspect(ctx, request)
	s := p.state
	if e != nil || request.Phase != "workload" || !s.deferred.Load() || !s.transportClosed.Load() || s.mode != "absent-entry" && !s.collectorFailed.Load() {
		return observation, e
	}
	if s.entered.CompareAndSwap(false, true) {
		s.entryScope, s.entry, e = s.b.assessmentAttempt(s.c.Binding, *s.c.Slot)
		if e != nil {
			s.seamErr = e
			return observation, e
		}
		phase := s.entry.PhaseRecords.Workload
		absent := s.mode == "absent-entry"
		if s.caller.Err() != nil || ctx.Err() != nil || s.entryScope.Binding() != s.c.Binding || s.entryScope.DenyLatched || s.entryScope.State != "sealed" || s.entry.State != "finishing" || s.entry.Binding != s.c.Binding || s.entry.Slot != *s.c.Slot || phase == nil || !phase.CreateClaimed || !phase.CreateReturned || phase.ResourceIdentity == nil || *phase.ResourceIdentity != observation.Identity || !phase.StartClaimed || !phase.StartReturned || !phase.StartedObserved || phase.ExitCode == nil || *phase.ExitCode != 0 || !phase.TransportStarted || !phase.TransportCloseConfirmed || phase.TransportCloseUncertain || observation.Exists == absent || !observation.Stopped || phase.StoppedObserved != absent || phase.RemovalObserved != absent || s.entry.OwnershipCloseConfirmed || !s.entry.AccountingUnresolved || s.entry.Compute.ReservedCPUSeconds != 30 || s.entry.Compute.ChargedCPUSeconds != 0 || !s.entry.ReaderStarted || !s.entry.CollectorStarted || s.entry.ReaderCloseConfirmed || s.entry.CollectorCloseConfirmed || !absent && (!s.entry.DenyLatched || s.entry.FailureCode != "execution_failed" || s.entry.PhaseRecords.Checker != nil) || absent && s.entry.DenyLatched {
			s.seamErr = x.ErrInvalid
			return observation, s.seamErr
		}
		s.cleanupCtx = ctx
		s.removeAtEntry = p.removes.Load()
		s.active.Store(true)
	}
	if s.active.Load() && !observation.Exists {
		s.absenceSeen.Store(true)
	}
	return observation, e
}

func (s *executionCleanupACKState) negative() error {
	e := s.b.Store.Update(func(d *ledger.Data) error {
		if d.Version != 7 || d.AssessmentExecution == nil {
			return x.ErrInvalid
		}
		scope, scopeOK := d.AssessmentExecution.Scopes[s.c.Binding.ScopeID]
		a, attemptOK := d.AssessmentExecution.Attempts[s.c.Slot.AttemptID]
		phase := a.PhaseRecords.Workload
		if !scopeOK || !attemptOK || scope.Binding() != s.c.Binding || a.Binding != s.c.Binding || a.Slot != *s.c.Slot || a.State != "finishing" || !a.DenyLatched || a.FailureCode != "execution_failed" || phase == nil || !phase.StoppedObserved || phase.RemovalObserved || !phase.TransportStarted || !phase.TransportCloseConfirmed || phase.TransportCloseUncertain {
			return x.ErrInvalid
		}
		next, e := x.AdvanceAttempt(a, scope, x.AttemptPhaseSafety, a.WallHighWater, a.TickHighWaterMS, func(v *x.AssessmentAttempt) { v.PhaseRecords.Workload.TransportCloseUncertain = true })
		if e == nil {
			d.AssessmentExecution.Attempts[s.c.Slot.AttemptID] = next
		}
		return e
	})
	if e == nil {
		s.negativeEvents.Add(1)
	}
	return e
}

func (s *executionCleanupACKState) hook(point string) error {
	if point == "after-journal:"+string(x.AttemptFinish) {
		// Only the original parent sees this post-seal hook. The first finish
		// ACK precedes its deferred CleanupPhases, not normal phase cleanup.
		s.deferred.Store(true)
	}
	if !s.active.Load() {
		return nil
	}
	if point == "before-journal:"+string(x.AttemptPhaseSafety) {
		n := s.before.Add(1)
		if s.caller.Err() != nil || s.cleanupCtx.Err() != nil {
			s.seamErr = x.ErrInvalid
			return s.seamErr
		}
		if s.mode == "before" && n == 1 {
			s.liveAtBoundary = true
			s.faulted.Store(true)
			return x.ErrHeld
		}
	}
	if point != "after-journal:"+string(x.AttemptPhaseSafety) {
		return nil
	}
	n := s.after.Add(1)
	if s.caller.Err() != nil || s.cleanupCtx.Err() != nil {
		s.seamErr = x.ErrInvalid
		return s.seamErr
	}
	if n == 1 && s.mode == "after" {
		s.liveAtBoundary = true
		s.faulted.Store(true)
		return x.ErrHeld
	}
	if n == 1 && s.mode == "late-uncertainty" {
		if e := s.negative(); e != nil {
			s.seamErr = e
			return e
		}
	}
	pairs := int32(2)
	if s.mode == "absent-entry" {
		pairs = 2
	}
	if n == pairs {
		// Bracket only stopped/removal, excluding later global close
		// and finalization. This second snapshot never supplies positive proof.
		s.endScope, s.end, s.seamErr = s.b.assessmentAttempt(s.c.Binding, *s.c.Slot)
		s.liveAtBoundary = s.caller.Err() == nil && s.cleanupCtx.Err() == nil
		if s.seamErr != nil {
			return s.seamErr
		}
		if !s.absenceSeen.Load() || s.end.PhaseRecords.Workload == nil || !s.end.PhaseRecords.Workload.RemovalObserved {
			s.seamErr = x.ErrInvalid
			return s.seamErr
		}
		s.active.Store(false)
		s.ended.Store(true)
	}
	return nil
}

// These candidate controls retain actual original Run and its synthetic
// resource. Two callbacks retain the same two semantic cleanup events; the
// frozen baseline observed three. This is not a write/fsync or latency claim.
func TestExecutionDeferredCleanupStoppedACKGuards(t *testing.T) {
	for _, mode := range []string{"mechanism", "before", "after", "absent-entry", "late-uncertainty"} {
		t.Run(mode, func(t *testing.T) {
			f := newExecutionFixture(t)
			c := executionControl(f, f.seal(t), 0)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			state := &executionCleanupACKState{mode: mode, b: f.b, c: c, caller: ctx}
			probe := &executionCleanupACKProvider{executionStopRecheckProvider: &executionStopRecheckProvider{assessmentProvider: f.b.assessment.provider}, state: state}
			f.b.assessment.provider = probe
			originalCollector := f.b.assessment.collector
			f.b.assessment.collector = func(ctx context.Context, scope x.ExecutionScope) (assessmentCollector, error) {
				collector, e := originalCollector(ctx, scope)
				if collector != nil {
					collector = &executionCleanupACKCollector{collector, mode != "absent-entry", &state.collectorFailed}
				}
				return collector, e
			}
			originalTransport := f.b.assessment.transport
			f.b.assessment.transport = func(ctx context.Context, scope x.ExecutionScope, phase string) (io.Closer, error) {
				transport, e := originalTransport(ctx, scope, phase)
				if transport == nil || phase != "workload" {
					return transport, e
				}
				return executionCloserFunc(func() error {
					closeErr := transport.Close()
					if closeErr == nil {
						state.transportClosed.Store(true)
					}
					return closeErr
				}), e
			}
			// The authentic watcher copied its original boundary at seal. It may
			// Stop, but cannot Remove or publish phase/global positive facts.
			f.b.assessment.owned.fault = state.hook
			result, runErr := f.b.RunAssessmentOnce(ctx, c)
			if ctx.Err() != nil || !state.entered.Load() || state.seamErr != nil || !state.liveAtBoundary {
				t.Fatal("deferred cleanup target or live bounded context was missing")
			}
			scope, a, readErr := f.b.assessmentAttempt(c.Binding, *c.Slot)
			if readErr != nil || scope.DenyLatched || scope.State != "sealed" || a.Compute.ReservedCPUSeconds != 30 || result.Disposition != "new_attempt" || result.Trust != x.Untrusted() {
				t.Fatal("cleanup control lost exact retained scope or public binding")
			}
			phase := a.PhaseRecords.Workload
			if phase == nil || !phase.CreateReturned || phase.ResourceIdentity == nil || !phase.StartReturned || !phase.TransportCloseConfirmed || !a.OwnershipCloseConfirmed || a.OwnershipCloseUncertain {
				t.Fatal("real original resource or checked ownership history missing")
			}
			fault := mode == "before" || mode == "after"
			late := mode == "late-uncertainty"
			absent := mode == "absent-entry"
			wantCreates, wantRemoves := int32(1), int32(1)
			if fault {
				wantRemoves = 0
			}
			if absent {
				wantCreates, wantRemoves = 2, 2
			}
			if probe.creates.Load() != wantCreates || probe.starts.Load() != wantCreates || probe.removes.Load() != wantRemoves {
				t.Fatal("cleanup dispatched missing or repeated provider effects")
			}
			f.p.mu.Lock()
			physical := f.p.active == 0 && f.p.starts == int(wantCreates) && len(f.p.resources) == int(wantCreates)
			for _, resource := range f.p.resources {
				physical = physical && resource.Stopped && resource.Exists == fault
			}
			f.p.mu.Unlock()
			if !physical {
				t.Fatal("actual backend resource observation disagreed with Remove oracle")
			}
			if fault {
				wantAfter := int32(0)
				if mode == "after" {
					wantAfter = 1
				}
				if !state.faulted.Load() || state.ended.Load() || state.before.Load() != 1 || state.after.Load() != wantAfter || state.removeAtEntry != 0 || phase.StoppedObserved != (mode == "after") || phase.RemovalObserved || a.ReaderCloseConfirmed || a.CollectorCloseConfirmed || a.SerialClear() || !a.AccountingUnresolved || a.ComputeCleanupObserved || a.Compute.ChargedCPUSeconds != 0 || a.Compute.Provenance != "reserved" || result.Code != "HELD" || !errors.Is(runErr, x.ErrHeld) {
					t.Fatal("failed first stopped ACK retried or continued to Remove/clearance")
				}
			} else {
				pairs, semantic := int32(2), int64(2)
				if absent {
					pairs, semantic = 2, 0
				}
				if late {
					semantic++
				}
				if !state.ended.Load() || state.before.Load() != pairs || state.after.Load() != pairs || state.end.Generation-state.entry.Generation != semantic || state.end.SafetyTransitionsUsed-state.entry.SafetyTransitionsUsed != semantic || state.end.NormalTransitionsUsed != state.entry.NormalTransitionsUsed || state.endScope.Generation != state.entryScope.Generation || !phase.StoppedObserved || !phase.RemovalObserved {
					t.Fatal("baseline stopped ACK callbacks or bracketed semantic events changed")
				}
				if absent && (!reflect.DeepEqual(state.entry, state.end) || probe.removes.Load()-state.removeAtEntry != 0) || !absent && probe.removes.Load()-state.removeAtEntry != 1 {
					t.Fatal("absence-entry ACKs changed state or repeated actual removal")
				}
				if late {
					if state.negativeEvents.Load() != 1 || !phase.TransportCloseUncertain || !a.DenyLatched || a.FailureCode != "execution_failed" || a.ReaderCloseConfirmed || a.CollectorCloseConfirmed || a.SerialClear() || !a.AccountingUnresolved || a.ComputeCleanupObserved || a.Compute.ChargedCPUSeconds != 0 || a.Compute.Provenance != "reserved" || result.Code != "HELD" || !errors.Is(runErr, x.ErrHeld) {
						t.Fatal("post-ACK legal uncertainty was cleared or falsely settled")
					}
				} else if !a.SerialClear() || a.State != "terminal" || a.AccountingUnresolved || !a.ComputeCleanupObserved || a.Compute.ChargedCPUSeconds != 30 || a.Compute.Provenance != "estimated_upper_bound" || result.Code != "OK" || absent && (a.Outcome != "succeeded" || runErr != nil) || !absent && (a.Outcome != "failed" || !a.DenyLatched || !errors.Is(runErr, x.ErrHeld)) {
					t.Fatal("actual closure lost charge or promoted earlier failed Run error")
				}
			}
			if !absent && (a.PhaseRecords.Checker != nil || !a.DenyLatched || a.FailureCode != "execution_failed") {
				t.Fatal("collector failure lost denial or dispatched a checker")
			}
			if _, e := f.b.assessment.owned.root.Lstat(scope.ScopeID); e != nil {
				t.Fatal("cleanup control removed retained input scope")
			}
		})
	}
}
