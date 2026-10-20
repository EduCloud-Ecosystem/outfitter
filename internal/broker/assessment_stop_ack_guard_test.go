package broker

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
)

type executionStopACKInspect struct {
	ctx         context.Context
	observation assessmentResourceObservation
	err         error
}

type executionStopACKProbe struct {
	assessmentProvider
	caller                                    context.Context
	creates, starts, inspects, stops, removes atomic.Int32
	inspectGate                               bool
	inspectGated                              atomic.Bool
	inspectEntryLive                          atomic.Bool
	inspectEntered                            chan executionStopACKInspect
	inspectRelease                            <-chan struct{}
	inspectReturnLive                         atomic.Bool
	stopIdentity                              chan x.DockerResourceIdentity
}

func (p *executionStopACKProbe) Create(ctx context.Context, request assessmentPhaseRequest) (x.DockerResourceIdentity, error) {
	p.creates.Add(1)
	return p.assessmentProvider.Create(ctx, request)
}
func (p *executionStopACKProbe) Start(ctx context.Context, id x.DockerResourceIdentity, input io.Reader, prior []byte, out, stderr io.Writer) (assessmentCompletion, error) {
	p.starts.Add(1)
	return p.assessmentProvider.Start(ctx, id, input, prior, out, stderr)
}
func (p *executionStopACKProbe) Inspect(ctx context.Context, request assessmentPhaseRequest) (assessmentResourceObservation, error) {
	p.inspects.Add(1)
	p.inspectEntryLive.Store(ctx.Err() == nil && p.caller.Err() == nil)
	observation, e := p.assessmentProvider.Inspect(ctx, request)
	if p.inspectGate && p.inspectGated.CompareAndSwap(false, true) {
		select {
		case p.inspectEntered <- executionStopACKInspect{ctx, observation, e}:
		case <-ctx.Done():
			return observation, ctx.Err()
		case <-p.caller.Done():
			return observation, p.caller.Err()
		}
		select {
		case <-p.inspectRelease:
		case <-ctx.Done():
			return observation, ctx.Err()
		case <-p.caller.Done():
			return observation, p.caller.Err()
		}
	}
	// The helper cancels this context on return. Sample at the actual provider
	// return boundary, before that normal cancellation can obscure liveness.
	p.inspectReturnLive.Store(ctx.Err() == nil && p.caller.Err() == nil)
	return observation, e
}
func (p *executionStopACKProbe) Stop(ctx context.Context, id x.DockerResourceIdentity) error {
	p.stops.Add(1)
	select {
	case p.stopIdentity <- id:
	default:
	}
	return p.assessmentProvider.Stop(ctx, id)
}
func (p *executionStopACKProbe) Remove(ctx context.Context, id x.DockerResourceIdentity) error {
	p.removes.Add(1)
	return p.assessmentProvider.Remove(ctx, id)
}

// These real live-phase controls exercise first-denial ACK, committed-negative
// Stop-only rescue and generation checks. Identity mismatch remains separate.
func TestExecutionStopFirstDenialACKAndGenerationGuards(t *testing.T) {
	for _, mode := range []string{"before", "after", "positive", "generation", "already-denied-positive", "already-denied-generation"} {
		t.Run(mode, func(t *testing.T) {
			alreadyDenied := mode == "already-denied-positive" || mode == "already-denied-generation"
			positive := mode == "positive" || mode == "already-denied-positive"
			generation := mode == "generation" || mode == "already-denied-generation"
			targetDenials := int64(1)
			if alreadyDenied {
				targetDenials = 0
			}
			started := time.Now()
			ctx, cancel := context.WithDeadline(context.Background(), started.Add(20*time.Second))
			t.Cleanup(cancel)
			f := newExecutionFixture(t)
			var trace *executionExpiryTrace
			if positive {
				trace = &executionExpiryTrace{}
				trace.bind(started, ctx)
			}
			blocked := &executionBlockedProvider{executionTestProvider: f.p, started: make(chan executionActiveEntry, 1), stopped: make(chan struct{}), release: make(chan struct{}), diagnostic: newExecutionContextTrace(), expiry: trace}
			f.b.assessment.provider = blocked
			if trace != nil {
				originalTransport := f.b.assessment.transport
				f.b.assessment.transport = func(ctx context.Context, scope x.ExecutionScope, phase string) (io.Closer, error) {
					transport, e := originalTransport(ctx, scope, phase)
					if transport != nil {
						transport = &executionExpiryTransport{transport, trace}
					}
					return transport, e
				}
			}
			watchEntered, watchRelease := make(chan struct{}), make(chan struct{})
			inspectRelease := make(chan struct{})
			var watchEnterOnce, watchReleaseOnce, inspectReleaseOnce, startReleaseOnce sync.Once
			var runDrained, targetDrained chan struct{}
			activeCancel := func() {}
			// This existing TEST seam is after real WatchReady and outside every
			// authority lock. Child isolation is not a production watcher change.
			f.b.assessment.owned.fault = func(point string) error {
				_ = trace.hook(point)
				if point == "watch-before-snapshot" {
					watchEnterOnce.Do(func() { close(watchEntered) })
					select {
					case <-watchRelease:
					case <-ctx.Done():
					}
				}
				return nil
			}
			t.Cleanup(func() {
				// Open every gate before cancellation or any original-owner join,
				// including partial setup and failed assertions.
				trace.record("release", nil)
				watchReleaseOnce.Do(func() { close(watchRelease) })
				inspectReleaseOnce.Do(func() { close(inspectRelease) })
				startReleaseOnce.Do(func() { close(blocked.release) })
				activeCancel()
				cancel()
				drainDeadline := time.Now().Add(4 * time.Second)
				if cap := started.Add(19 * time.Second); cap.Before(drainDeadline) {
					drainDeadline = cap
				}
				drain, stopDrain := context.WithDeadline(context.Background(), drainDeadline)
				defer stopDrain()
				failureSnapshot := false
				for _, join := range []struct {
					label string
					done  chan struct{}
				}{{"target_invocation", targetDrained}, {"original_run", runDrained}} {
					if join.done == nil {
						continue
					}
					select {
					case <-join.done:
					case <-drain.Done():
						if !failureSnapshot {
							trace.log(t, "before_fatal")
							failureSnapshot = true
						}
						t.Errorf("original live Stop control did not drain during cleanup: join=%s", join.label)
					}
				}
				// This snapshot follows join attempts, not proven drainage when a
				// join timed out. No second wait or later success is inferred.
				trace.log(t, "after_cleanup")
			})
			status := f.seal(t)
			ready := executionReadyTimer(started)
			defer ready.Stop()
			select {
			case <-watchEntered:
			case <-ready.C:
				t.Fatal("authentic watcher isolation not ready within original setup budget")
			case <-ctx.Done():
				t.Fatal("original caller expired during watcher setup")
			}
			c := executionControl(f, status, 0)
			probe := &executionStopACKProbe{assessmentProvider: blocked, caller: ctx, inspectGate: generation, inspectEntered: make(chan executionStopACKInspect, 1), inspectRelease: inspectRelease, stopIdentity: make(chan x.DockerResourceIdentity, 1)}
			// Installed after seal: the copied child remains blocked at its own
			// original hook and cannot mask this parent's provider continuation.
			f.b.assessment.provider = probe
			var beforeCalls, afterCalls atomic.Int32
			var faulted atomic.Bool
			f.b.assessment.owned.fault = func(point string) error {
				_ = trace.hook(point)
				if point == "before-journal:"+string(x.AttemptDeny) {
					beforeCalls.Add(1)
					if mode == "before" && faulted.CompareAndSwap(false, true) {
						return x.ErrHeld
					}
				}
				if point == "after-journal:"+string(x.AttemptDeny) {
					afterCalls.Add(1)
					if mode == "after" && faulted.CompareAndSwap(false, true) {
						return x.ErrHeld
					}
				}
				return nil
			}
			runDrained = make(chan struct{})
			go func() {
				defer close(runDrained)
				_, runErr := f.b.RunAssessmentOnce(ctx, c)
				trace.record("run_return", runErr)
			}()
			var entry executionActiveEntry
			select {
			case entry = <-blocked.started:
			case <-runDrained:
				t.Fatal("original Run returned before actual Start")
			case <-ready.C:
				t.Fatal("actual Start not ready within original setup budget")
			case <-ctx.Done():
				t.Fatal("original caller expired before actual Start")
			}
			activeDeadline := executionActiveDeadline(t, f, c, ctx, started, entry)
			activeCtx, stopActive := context.WithDeadline(ctx, activeDeadline)
			activeCancel = stopActive
			if _, e := f.b.assessmentDeny(c.Binding, "cancelled"); e != nil {
				t.Fatal("real scope-only denial failed")
			}
			if alreadyDenied {
				committed, denialErr := f.b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptDeny, func(v *x.AssessmentAttempt) error {
					if v.DenyLatched || v.State != "running" {
						return x.ErrHeld
					}
					v.DenyLatched = true
					v.FailureCode = "cancelled"
					return nil
				})
				if denialErr != nil || !committed.DenyLatched || committed.FailureCode != "cancelled" || beforeCalls.Load() != 1 || afterCalls.Load() != 1 || faulted.Load() {
					t.Fatal("prior committed denial lacks its actual definitive ACK")
				}
				// Reset only target-hook observations, never metadata or provider
				// counts. The following snapshot includes the real prior event.
				beforeCalls.Store(0)
				afterCalls.Store(0)
			}
			scope, before, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
			phase := before.PhaseRecords.Workload
			if e != nil || !scope.DenyLatched || scope.Reason != "cancelled" || before.DenyLatched != alreadyDenied || before.State != "running" || phase == nil || !phase.CreateReturned || phase.ResourceIdentity == nil || !phase.StartClaimed || phase.StartReturned || !before.ReaderStarted || !before.CollectorStarted || before.ReaderCloseConfirmed || before.CollectorCloseConfirmed || before.OwnershipCloseConfirmed || !before.AccountingUnresolved || before.Compute.ChargedCPUSeconds != 0 || before.Compute.ReservedCPUSeconds != 30 {
				t.Fatal("first-denial target lacks original live phase and unresolved owner")
			}
			identity := *phase.ResourceIdentity
			blocked.mu.Lock()
			resource := blocked.resources[phase.OwnerName]
			blocked.mu.Unlock()
			if !resource.Exists || resource.Stopped || resource.Identity != identity || probe.creates.Load() != 1 || probe.starts.Load() != 1 || probe.inspects.Load() != 0 || probe.stops.Load() != 0 || probe.removes.Load() != 0 || beforeCalls.Load() != 0 || afterCalls.Load() != 0 || ctx.Err() != nil || entry.ctx.Err() != nil || activeCtx.Err() != nil {
				t.Fatal("actual running target or independent callback counters missing")
			}
			targetDone := make(chan error, 1)
			targetDrained = make(chan struct{})
			go func() {
				defer close(targetDrained)
				targetDone <- f.b.assessmentStopDeniedAttempts(activeCtx, c.Binding)
			}()
			if generation {
				var inspected executionStopACKInspect
				select {
				case inspected = <-probe.inspectEntered:
				case <-targetDrained:
					t.Fatal("target returned before actual Inspect barrier")
				case <-activeCtx.Done():
					t.Fatal("active budget expired before actual Inspect")
				}
				if inspected.err != nil || !inspected.observation.Exists || inspected.observation.Stopped || inspected.observation.Identity != identity || inspected.ctx.Err() != nil || ctx.Err() != nil || entry.ctx.Err() != nil {
					t.Fatal("actual helper Inspect context or identity invalid before generation change")
				}
				current, changeErr := f.b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptFinish, func(v *x.AssessmentAttempt) error {
					if !v.DenyLatched || v.State != "running" || v.PhaseRecords.Workload == nil || v.PhaseRecords.Workload.ResourceIdentity == nil || *v.PhaseRecords.Workload.ResourceIdentity != identity || v.PhaseRecords.Workload.StartReturned {
						return x.ErrHeld
					}
					v.State = "finishing"
					return nil
				})
				if changeErr != nil || current.Generation != before.Generation+targetDenials+1 || current.SafetyTransitionsUsed != before.SafetyTransitionsUsed+targetDenials+1 || current.NormalTransitionsUsed != before.NormalTransitionsUsed || inspected.ctx.Err() != nil || activeCtx.Err() != nil || entry.ctx.Err() != nil {
					t.Fatal("legal generation ACK or helper three-second budget missing")
				}
				inspectReleaseOnce.Do(func() { close(inspectRelease) })
			}
			select {
			case <-targetDone:
				// The helper deliberately swallows per-candidate mutation errors;
				// its returned error is not the failed-ACK continuation oracle.
			case <-activeCtx.Done():
				t.Fatal("target invocation exceeded original active deadline")
			}
			if ctx.Err() != nil || entry.ctx.Err() != nil || activeCtx.Err() != nil {
				t.Fatal("timeout cannot qualify provider suppression")
			}
			select {
			case <-runDrained:
				t.Fatal("original callback returned before target observations")
			default:
			}
			_, after, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
			wantDelta := targetDenials
			if mode == "before" {
				wantDelta = 0
			}
			if generation {
				wantDelta = targetDenials + 1
			}
			wantBeforeCalls := int32(targetDenials)
			wantAfterCalls := int32(targetDenials)
			if mode == "before" {
				wantAfterCalls = 0
			}
			if e != nil || beforeCalls.Load() != wantBeforeCalls || afterCalls.Load() != wantAfterCalls || faulted.Load() != (mode == "before" || mode == "after") || after.Generation != before.Generation+wantDelta || after.SafetyTransitionsUsed != before.SafetyTransitionsUsed+wantDelta || after.NormalTransitionsUsed != before.NormalTransitionsUsed || after.DenyLatched != (mode != "before") || mode != "before" && after.FailureCode != "cancelled" || mode == "before" && after.FailureCode != "none" {
				t.Fatal("first-denial committed-or-not facts or exact event counters wrong")
			}
			wantState := "running"
			if generation {
				wantState = "finishing"
			}
			phase = after.PhaseRecords.Workload
			if after.State != wantState || phase == nil || phase.ResourceIdentity == nil || *phase.ResourceIdentity != identity || phase.StartReturned || after.PhaseRecords.Checker != nil || after.ReaderCloseConfirmed || after.CollectorCloseConfirmed || after.OwnershipCloseConfirmed || after.ReaderCloseUncertain || after.CollectorCloseUncertain || after.OwnershipCloseUncertain || after.SerialClear() || !after.AccountingUnresolved || after.ComputeCleanupObserved || after.Compute.ReservedCPUSeconds != 30 || after.Compute.ChargedCPUSeconds != 0 || after.Compute.Provenance != "reserved" {
				t.Fatal("target fabricated callback, global close, settlement or identity")
			}
			wantInspects, wantStops := int32(0), int32(0)
			if positive || generation {
				wantInspects = 1
			}
			if positive {
				wantStops = 1
			}
			if probe.creates.Load() != 1 || probe.starts.Load() != 1 || probe.inspects.Load() != wantInspects || probe.stops.Load() != wantStops || probe.removes.Load() != 0 || wantInspects != 0 && (!probe.inspectEntryLive.Load() || !probe.inspectReturnLive.Load()) {
				t.Fatal("parent provider continuation or actual Inspect return liveness wrong")
			}
			blocked.mu.Lock()
			resource = blocked.resources[phase.OwnerName]
			blocked.mu.Unlock()
			if !resource.Exists || resource.Identity != identity || resource.Stopped != positive {
				t.Fatal("actual original resource Stop state wrong")
			}
			select {
			case <-blocked.stopped:
				if !positive {
					t.Fatal("negative invocation actually stopped original resource")
				}
			default:
				if positive {
					t.Fatal("positive sibling did not observe actual delegated Stop")
				}
			}
			if positive {
				select {
				case stopped := <-probe.stopIdentity:
					if stopped != identity {
						t.Fatal("positive Stop targeted a different resource")
					}
				default:
					t.Fatal("actual positive Stop identity missing")
				}
			}
			// All observations above precede watcher/Start release and cleanup.
		})
	}
}
