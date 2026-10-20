package broker

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
)

type executionBlockedProvider struct {
	*executionTestProvider
	started, stopped, release chan struct{}
	stopOnce                  sync.Once
}

func (p *executionBlockedProvider) Start(ctx context.Context, id x.DockerResourceIdentity, input io.Reader, prior []byte, out, stderr io.Writer) (assessmentCompletion, error) {
	p.mu.Lock()
	p.starts++
	p.active++
	if p.active > p.maxActive {
		p.maxActive = p.active
	}
	p.mu.Unlock()
	defer func() { p.mu.Lock(); p.active--; p.mu.Unlock() }()
	start := time.Now().UTC()
	close(p.started)
	select {
	case <-p.stopped:
	case <-ctx.Done():
		return assessmentCompletion{}, ctx.Err()
	}
	// Stop is observed, but the running callback/reader has not yet drained.
	select {
	case <-p.release:
	case <-ctx.Done():
		return assessmentCompletion{}, ctx.Err()
	}
	finish := time.Now().UTC()
	return assessmentCompletion{ExitCode: 137, StartedAt: &start, FinishedAt: &finish}, nil
}
func (p *executionBlockedProvider) Stop(ctx context.Context, id x.DockerResourceIdentity) error {
	if e := p.executionTestProvider.Stop(ctx, id); e != nil {
		return e
	}
	p.stopOnce.Do(func() { close(p.stopped) })
	return nil
}

// Watch must see a running owned resource; the ordinary fixture creates an
// already-stopped resource to keep its immediate synthetic phase minimal.
func (p *executionBlockedProvider) Create(ctx context.Context, r assessmentPhaseRequest) (x.DockerResourceIdentity, error) {
	id, e := p.executionTestProvider.Create(ctx, r)
	if e != nil {
		return id, e
	}
	p.mu.Lock()
	obs := p.resources[r.OwnerName]
	obs.Stopped = false
	p.resources[r.OwnerName] = obs
	p.mu.Unlock()
	return id, nil
}
func TestExecutionWatcherStopsActiveDeniedOrExpiredAttempt(t *testing.T) {
	for _, mode := range []string{"slot-cancel", "attempt-expiry", "actual-wall-rollback"} {
		t.Run(mode, func(t *testing.T) {
			f := newExecutionFixture(t)
			p := &executionBlockedProvider{executionTestProvider: f.p, started: make(chan struct{}), stopped: make(chan struct{}), release: make(chan struct{})}
			f.b.assessment.provider = p
			var wallShift, tickShift atomic.Int64
			clock := f.b.assessment.owned.clock
			f.b.assessment.owned.clock = func() (ownedClock, error) {
				n, e := clock()
				n.Wall = n.Wall.Add(time.Duration(wallShift.Load()))
				n.TickMS += tickShift.Load()
				return n, e
			}
			status := f.seal(t)
			c := executionControl(f, status, 0)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var once sync.Once
			defer once.Do(func() { close(p.release) })
			type answer struct {
				r x.Result
				e error
			}
			done := make(chan answer, 1)
			drained := make(chan struct{})
			go func() { defer close(drained); r, e := f.b.RunAssessmentOnce(ctx, c); done <- answer{r, e} }()
			t.Cleanup(func() {
				cancel()
				once.Do(func() { close(p.release) })
				select {
				case <-drained:
				case <-time.After(4 * time.Second):
					t.Error("running owner did not drain during test cleanup")
				}
			})
			select {
			case <-p.started:
			case result := <-done:
				scope, a, ae := f.b.assessmentAttempt(c.Binding, *c.Slot)
				t.Fatalf("run returned before Start: err=%v read=%v scope=%s/%s attempt=%s/%s", result.e, ae, scope.State, scope.Reason, a.State, a.FailureCode)
			case <-time.After(2 * time.Second):
				scope, a, ae := f.b.assessmentAttempt(c.Binding, *c.Slot)
				t.Fatalf("Start barrier not reached: read=%v scope=%s/%s attempt=%s/%s", ae, scope.State, scope.Reason, a.State, a.FailureCode)
			}
			switch mode {
			case "slot-cancel":
				reason := "cancelled"
				cc := x.Control{Schema: x.ControlSchema, Operation: "cancel", Binding: c.Binding, Slot: c.Slot, Reason: &reason}
				if _, e := f.b.AssessmentData(ctx, cc, nil); !errors.Is(e, x.ErrHeld) {
					t.Fatal("active lease must remain held", e)
				}
			case "attempt-expiry":
				wallShift.Store(int64(31 * time.Second))
				tickShift.Store(31000)
			case "actual-wall-rollback":
				wallShift.Store(int64(-time.Second))
			}
			executionWait(t, p.stopped)
			scope, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
			if e != nil || !a.DenyLatched || a.ReaderCloseConfirmed || a.CollectorCloseConfirmed || !a.AccountingUnresolved || a.Compute.ChargedCPUSeconds != 0 || a.PhaseRecords.Checker != nil {
				t.Fatal("stop fabricated global drain/accounting", e)
			}
			if mode != "actual-wall-rollback" && scope.DenyLatched {
				t.Fatal("slot-only denial widened scope")
			}
			if mode == "actual-wall-rollback" && scope.Reason != "clock_rollback" {
				t.Fatal("real rollback was hidden", scope.Reason)
			}
			once.Do(func() { close(p.release) })
			var result answer
			select {
			case result = <-done:
			case <-ctx.Done():
				t.Fatal("running owner did not drain")
			}
			if result.e == nil {
				t.Fatal("cancelled/expired phase reported success")
			}
			_, a, e = f.b.assessmentAttempt(c.Binding, *c.Slot)
			if e != nil || !a.ReaderCloseConfirmed || !a.CollectorCloseConfirmed || a.PhaseRecords.Checker != nil {
				t.Fatal("final reader/collector proof missing", e)
			}
			p.mu.Lock()
			starts := p.starts
			active := p.active
			p.mu.Unlock()
			if starts != 1 || active != 0 {
				t.Fatal("extra phase or active reader", starts, active)
			}
			if mode != "actual-wall-rollback" && (a.AccountingUnresolved || a.Compute.ChargedCPUSeconds != 30) {
				t.Fatal("definitive stopped/callback-drained accounting", a.Compute)
			}
		})
	}
}

type executionOverflowProvider struct {
	*executionTestProvider
	cancellation error
}

func (p *executionOverflowProvider) Start(ctx context.Context, id x.DockerResourceIdentity, input io.Reader, prior []byte, out, stderr io.Writer) (assessmentCompletion, error) {
	p.mu.Lock()
	p.starts++
	p.mu.Unlock()
	oversized := make([]byte, 65537)
	for i := range oversized {
		oversized[i] = 'x'
	}
	_, writeError := out.Write(oversized)
	clear(oversized)
	if writeError == nil {
		return assessmentCompletion{}, errors.New("synthetic output unexpectedly accepted")
	}
	// The callback must cancel this context. Waiting for the absolute deadline is
	// a distinct result, not an equivalent overflow cancellation mechanism.
	<-ctx.Done()
	p.cancellation = ctx.Err()
	return assessmentCompletion{}, ctx.Err()
}
func TestExecutionOutputOverflowCancelsBlockedStart(t *testing.T) {
	f := newExecutionFixture(t)
	p := &executionOverflowProvider{executionTestProvider: f.p}
	f.b.assessment.provider = p
	status := f.seal(t)
	c := executionControl(f, status, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r, e := f.b.RunAssessmentOnce(ctx, c)
	if e == nil {
		t.Fatal("overflow succeeded")
	}
	if !errors.Is(p.cancellation, context.Canceled) {
		t.Fatalf("overflow did not cancel callback context: %v", p.cancellation)
	}
	_, a, ae := f.b.assessmentAttempt(c.Binding, *c.Slot)
	if ae != nil || !a.DenyLatched || a.FailureCode != "output_limit" || a.PhaseRecords.Checker != nil || a.Compute.ChargedCPUSeconds != 0 || !a.AccountingUnresolved || p.starts != 1 {
		t.Fatal("overflow failure lost held charge/no-checker semantics", ae, r.Code)
	}
}
