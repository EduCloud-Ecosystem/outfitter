package broker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
)

type executionContextPoint struct {
	Point                  string
	Context                string
	ElapsedMS, RemainingMS int64
}
type executionContextTrace struct {
	mu     sync.Mutex
	start  time.Time
	points []executionContextPoint
}

func newExecutionContextTrace() *executionContextTrace {
	return &executionContextTrace{start: time.Now()}
}
func (d *executionContextTrace) record(ctx context.Context, point string) {
	if d == nil {
		return
	}
	switch point {
	case "run_entry", "run_return", "create_entry", "create_return", "start_entry", "barrier_before_read", "barrier_after_read":
	default:
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.points) < 8 {
		d.points = append(d.points, executionContextPoint{point, executionProcessErrorClass(ctx.Err()), executionSessionDiagnosticMS(time.Since(d.start).Milliseconds()), executionSessionRemaining(ctx)})
	}
}
func (d *executionContextTrace) log(t *testing.T) {
	if d == nil {
		return
	}
	d.mu.Lock()
	points := append([]executionContextPoint(nil), d.points...)
	d.mu.Unlock()
	t.Logf("fixed callback context points: %+v", points)
}

// Fixed expiry observations do not isolate fsync from journal wait/read/write.
var executionExpiryStages = [...]string{
	"release", "start_return", "stop_return", "inspect_return", "remove_return", "transport_close_return",
	"watch_snapshot", "phase_safety_before", "phase_safety_after", "attempt_deny_before", "attempt_deny_after",
	"finish_after", "reader_close_after", "collector_close_after", "settle_after", "run_return",
}

type executionExpiryRow struct {
	Stage       string `json:"stage"`
	Count       int    `json:"count"`
	CountCapped bool   `json:"count_capped"`
	LastMS      int64  `json:"last_ms"`
	TimeCapped  bool   `json:"time_capped"`
	Caller      string `json:"caller"`
	Callback    string `json:"callback"`
	Error       string `json:"error"`
}
type executionExpiryTrace struct {
	mu                     sync.Mutex
	started                time.Time
	caller, callback       context.Context
	enabled                bool
	rows                   [len(executionExpiryStages)]executionExpiryRow
	snapshots, loggedBytes int
}

func executionExpiryContext(ctx context.Context) string {
	if ctx == nil {
		return "unavailable"
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return "canceled"
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "deadline"
	}
	return "none"
}
func executionExpiryError(e error) string {
	switch {
	case e == nil:
		return "none"
	case errors.Is(e, context.Canceled):
		return "canceled"
	case errors.Is(e, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(e, x.ErrDenied):
		return "denied"
	case errors.Is(e, x.ErrHeld):
		return "held"
	default:
		return "other"
	}
}
func (d *executionExpiryTrace) bind(started time.Time, caller context.Context) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.started, d.caller = started, caller
}
func (d *executionExpiryTrace) bindCallback(ctx context.Context) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.callback = ctx
}
func (d *executionExpiryTrace) record(stage string, e error) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.started.IsZero() {
		return
	}
	if stage == "release" {
		d.enabled = true
	}
	if !d.enabled && stage != "start_return" && stage != "stop_return" {
		return
	}
	for i, name := range executionExpiryStages {
		if stage != name {
			continue
		}
		r := &d.rows[i]
		r.Stage = name
		if r.Count < 32 {
			r.Count++
		} else {
			r.CountCapped = true
		}
		ms := time.Since(d.started).Milliseconds()
		r.TimeCapped = ms < 0 || ms > 125000
		if ms < 0 {
			ms = 0
		}
		if ms > 125000 {
			ms = 125000
		}
		r.LastMS, r.Caller, r.Callback, r.Error = ms, executionExpiryContext(d.caller), executionExpiryContext(d.callback), executionExpiryError(e)
		return
	}
}
func (d *executionExpiryTrace) hook(point string) error {
	if d == nil {
		return nil
	}
	switch point {
	case "watch-before-snapshot":
		d.record("watch_snapshot", nil)
	case "before-journal:" + string(x.AttemptPhaseSafety):
		d.record("phase_safety_before", nil)
	case "after-journal:" + string(x.AttemptPhaseSafety):
		d.record("phase_safety_after", nil)
	case "before-journal:" + string(x.AttemptDeny):
		d.record("attempt_deny_before", nil)
	case "after-journal:" + string(x.AttemptDeny):
		d.record("attempt_deny_after", nil)
	case "after-journal:" + string(x.AttemptFinish):
		d.record("finish_after", nil)
	case "after-journal:" + string(x.AttemptReaderClosed):
		d.record("reader_close_after", nil)
	case "after-journal:" + string(x.AttemptCollectorClosed):
		d.record("collector_close_after", nil)
	case "after-journal:" + string(x.AttemptSettle):
		d.record("settle_after", nil)
	}
	return nil
}
func (d *executionExpiryTrace) log(t *testing.T, point string) {
	if d == nil {
		return
	}
	if point != "before_fatal" && point != "after_cleanup" {
		t.Error("invalid fixed expiry snapshot")
		return
	}
	d.mu.Lock()
	if d.snapshots >= 2 {
		d.mu.Unlock()
		t.Error("fixed expiry snapshot bound exceeded")
		return
	}
	rows := make([]executionExpiryRow, 0, len(d.rows))
	for _, row := range d.rows {
		if row.Count > 0 {
			rows = append(rows, row)
		}
	}
	raw, e := json.Marshal(struct {
		Point string               `json:"point"`
		Rows  []executionExpiryRow `json:"rows"`
	}{point, rows})
	// Reserve testing's fixed log prefix as well as payload bytes.
	size := len(raw) + 128
	if e != nil || d.loggedBytes+size > 8192 {
		d.mu.Unlock()
		t.Error("fixed expiry trace byte bound exceeded")
		return
	}
	d.snapshots++
	d.loggedBytes += size
	d.mu.Unlock()
	t.Logf("fixed expiry trace: %s", raw)
}

type executionExpiryTransport struct {
	io.Closer
	trace *executionExpiryTrace
}

func (t *executionExpiryTransport) Close() error {
	e := t.Closer.Close()
	t.trace.record("transport_close_return", e)
	return e
}

// These synthetic positive controls separate bounded readiness from active
// assertions. The original Run/provider context has one absolute 20s deadline;
// no phase, gate or assertion renews it or the unchanged 30s reservation.
type executionActiveEntry struct {
	ctx context.Context
	at  time.Time
}

func executionActiveDeadline(t *testing.T, f *executionFixture, c x.Control, caller context.Context, started time.Time, entry executionActiveEntry) time.Time {
	t.Helper()
	_, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
	reserved, parseErr := time.Parse("2006-01-02T15:04:05Z", a.Deadline)
	callerDeadline, ok := caller.Deadline()
	now := time.Now()
	active := entry.at.Add(5 * time.Second)
	for _, cap := range []time.Time{started.Add(15 * time.Second), callerDeadline.Add(-4 * time.Second)} {
		if cap.Before(active) {
			active = cap
		}
	}
	if e != nil || parseErr != nil || !ok || caller.Err() != nil || entry.ctx.Err() != nil || entry.at.After(started.Add(10*time.Second)) || !now.Before(active) || callerDeadline.Sub(now) < 9*time.Second || reserved.Sub(now) < 9*time.Second {
		t.Fatal("active control lacks actual live Start and bounded caller/reservation budget")
	}
	return active
}

func executionReadyTimer(started time.Time) *time.Timer {
	return time.NewTimer(time.Until(started.Add(10 * time.Second)))
}

type executionBlockedProvider struct {
	*executionTestProvider
	started          chan executionActiveEntry
	stopped, release chan struct{}
	stopOnce         sync.Once
	diagnostic       *executionContextTrace
	expiry           *executionExpiryTrace
}

func (p *executionBlockedProvider) Start(ctx context.Context, id x.DockerResourceIdentity, input io.Reader, prior []byte, out, stderr io.Writer) (completion assessmentCompletion, err error) {
	p.expiry.bindCallback(ctx)
	defer func() { p.expiry.record("start_return", err) }()
	p.diagnostic.record(ctx, "start_entry")
	p.mu.Lock()
	p.starts++
	p.active++
	if p.active > p.maxActive {
		p.maxActive = p.active
	}
	p.mu.Unlock()
	defer func() { p.mu.Lock(); p.active--; p.mu.Unlock() }()
	start := time.Now().UTC()
	p.started <- executionActiveEntry{ctx, time.Now()}
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
func (p *executionBlockedProvider) Stop(ctx context.Context, id x.DockerResourceIdentity) (err error) {
	defer func() { p.expiry.record("stop_return", err) }()
	if e := p.executionTestProvider.Stop(ctx, id); e != nil {
		return e
	}
	p.stopOnce.Do(func() { close(p.stopped) })
	return nil
}

func (p *executionBlockedProvider) Inspect(ctx context.Context, request assessmentPhaseRequest) (assessmentResourceObservation, error) {
	observation, e := p.executionTestProvider.Inspect(ctx, request)
	p.expiry.record("inspect_return", e)
	return observation, e
}
func (p *executionBlockedProvider) Remove(ctx context.Context, id x.DockerResourceIdentity) error {
	e := p.executionTestProvider.Remove(ctx, id)
	p.expiry.record("remove_return", e)
	return e
}

// Watch must see a running owned resource; the ordinary fixture creates an
// already-stopped resource to keep its immediate synthetic phase minimal.
func (p *executionBlockedProvider) Create(ctx context.Context, r assessmentPhaseRequest) (x.DockerResourceIdentity, error) {
	p.diagnostic.record(ctx, "create_entry")
	id, e := p.executionTestProvider.Create(ctx, r)
	p.diagnostic.record(ctx, "create_return")
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
			p := &executionBlockedProvider{executionTestProvider: f.p, started: make(chan executionActiveEntry, 1), stopped: make(chan struct{}), release: make(chan struct{})}
			f.b.assessment.provider = p
			if mode == "attempt-expiry" || mode == "slot-cancel" {
				p.expiry = &executionExpiryTrace{}
				// Before seal: observe the actual copied watcher without parking it.
				f.b.assessment.owned.fault = p.expiry.hook
				originalTransport := f.b.assessment.transport
				f.b.assessment.transport = func(ctx context.Context, scope x.ExecutionScope, phase string) (io.Closer, error) {
					transport, e := originalTransport(ctx, scope, phase)
					if transport != nil {
						transport = &executionExpiryTransport{transport, p.expiry}
					}
					return transport, e
				}
				// LIFO follows the original-owner join ATTEMPT, not proof of drainage.
				t.Cleanup(func() { p.expiry.log(t, "after_cleanup") })
			}
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
			started := time.Now()
			ctx, cancel := context.WithDeadline(context.Background(), started.Add(20*time.Second))
			defer cancel()
			p.expiry.bind(started, ctx)
			p.diagnostic = newExecutionContextTrace()
			defer p.diagnostic.log(t)
			var once sync.Once
			defer once.Do(func() { close(p.release) })
			type answer struct {
				r x.Result
				e error
			}
			done := make(chan answer, 1)
			drained := make(chan struct{})
			go func() {
				defer close(drained)
				p.diagnostic.record(ctx, "run_entry")
				r, e := f.b.RunAssessmentOnce(ctx, c)
				p.diagnostic.record(ctx, "run_return")
				p.expiry.record("run_return", e)
				done <- answer{r, e}
			}()
			t.Cleanup(func() {
				cancel()
				once.Do(func() { close(p.release) })
				select {
				case <-drained:
				case <-time.After(4 * time.Second):
					t.Error("running owner did not drain during test cleanup")
				}
			})
			readyTimer := executionReadyTimer(started)
			defer readyTimer.Stop()
			var entry executionActiveEntry
			select {
			case entry = <-p.started:
			case result := <-done:
				scope, a, ae := f.b.assessmentAttempt(c.Binding, *c.Slot)
				t.Fatalf("run returned before Start: err=%v read=%v scope=%s/%s attempt=%s/%s", result.e, ae, scope.State, scope.Reason, a.State, a.FailureCode)
			case <-readyTimer.C:
				p.diagnostic.record(ctx, "barrier_before_read")
				scope, a, ae := f.b.assessmentAttempt(c.Binding, *c.Slot)
				p.diagnostic.record(ctx, "barrier_after_read")
				t.Fatalf("Start barrier not reached: read=%v scope=%s/%s attempt=%s/%s", ae, scope.State, scope.Reason, a.State, a.FailureCode)
			}
			activeDeadline := executionActiveDeadline(t, f, c, ctx, started, entry)
			activeCtx, activeCancel := context.WithDeadline(ctx, activeDeadline)
			defer activeCancel()
			switch mode {
			case "slot-cancel":
				reason := "cancelled"
				cc := x.Control{Schema: x.ControlSchema, Operation: "cancel", Binding: c.Binding, Slot: c.Slot, Reason: &reason}
				if _, e := f.b.AssessmentData(activeCtx, cc, nil); !errors.Is(e, x.ErrHeld) {
					t.Fatal("active lease must remain held", e)
				}
			case "attempt-expiry":
				wallShift.Store(int64(31 * time.Second))
				tickShift.Store(31000)
			case "actual-wall-rollback":
				wallShift.Store(int64(-time.Second))
			}
			select {
			case <-p.stopped:
			case <-activeCtx.Done():
				t.Fatal("actual watcher stop missed bounded active deadline")
			}
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
			if activeCtx.Err() != nil {
				t.Fatal("active stop assertions exceeded original active deadline")
			}
			p.expiry.record("release", nil)
			once.Do(func() { close(p.release) })
			drainDeadline := time.Now().Add(4 * time.Second)
			if cap := started.Add(19 * time.Second); cap.Before(drainDeadline) {
				drainDeadline = cap
			}
			drainTimer := time.NewTimer(time.Until(drainDeadline))
			defer drainTimer.Stop()
			var result answer
			select {
			case result = <-done:
			case <-drainTimer.C:
				p.expiry.log(t, "before_fatal")
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
	cancellation               error
	diagnostic                 *executionContextTrace
	started                    chan executionActiveEntry
	release                    chan struct{}
	writeCanceledSynchronously bool
}

func (p *executionOverflowProvider) Create(ctx context.Context, r assessmentPhaseRequest) (x.DockerResourceIdentity, error) {
	p.diagnostic.record(ctx, "create_entry")
	id, e := p.executionTestProvider.Create(ctx, r)
	p.diagnostic.record(ctx, "create_return")
	return id, e
}

func (p *executionOverflowProvider) Start(ctx context.Context, id x.DockerResourceIdentity, input io.Reader, prior []byte, out, stderr io.Writer) (assessmentCompletion, error) {
	p.diagnostic.record(ctx, "start_entry")
	p.started <- executionActiveEntry{ctx, time.Now()}
	select {
	case <-p.release:
	case <-ctx.Done():
		return assessmentCompletion{}, ctx.Err()
	}
	if ctx.Err() != nil {
		return assessmentCompletion{}, ctx.Err()
	}
	p.mu.Lock()
	p.starts++
	p.mu.Unlock()
	oversized := make([]byte, 65537)
	for i := range oversized {
		oversized[i] = 'x'
	}
	_, writeError := out.Write(oversized)
	p.writeCanceledSynchronously = errors.Is(ctx.Err(), context.Canceled)
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
	p := &executionOverflowProvider{executionTestProvider: f.p, started: make(chan executionActiveEntry, 1), release: make(chan struct{})}
	f.b.assessment.provider = p
	status := f.seal(t)
	c := executionControl(f, status, 0)
	started := time.Now()
	ctx, cancel := context.WithDeadline(context.Background(), started.Add(20*time.Second))
	defer cancel()
	p.diagnostic = newExecutionContextTrace()
	defer p.diagnostic.log(t)
	type answer struct {
		r x.Result
		e error
	}
	done := make(chan answer, 1)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		p.diagnostic.record(ctx, "run_entry")
		r, e := f.b.RunAssessmentOnce(ctx, c)
		p.diagnostic.record(ctx, "run_return")
		done <- answer{r, e}
	}()
	var releaseOnce sync.Once
	t.Cleanup(func() {
		cancel()
		releaseOnce.Do(func() { close(p.release) })
		select {
		case <-drained:
		case <-time.After(4 * time.Second):
			t.Error("overflow original owner did not drain")
		}
	})
	readyTimer := executionReadyTimer(started)
	defer readyTimer.Stop()
	var entry executionActiveEntry
	select {
	case entry = <-p.started:
	case <-done:
		t.Fatal("overflow owner returned before actual Start")
	case <-readyTimer.C:
		t.Fatal("overflow actual Start missed bounded setup deadline")
	}
	activeDeadline := executionActiveDeadline(t, f, c, ctx, started, entry)
	releaseOnce.Do(func() { close(p.release) })
	activeTimer := time.NewTimer(time.Until(activeDeadline))
	defer activeTimer.Stop()
	var got answer
	select {
	case got = <-done:
	case <-activeTimer.C:
		t.Fatal("overflow cancellation/finalization missed bounded active deadline")
	}
	if got.e == nil {
		t.Fatal("overflow succeeded")
	}
	if !p.writeCanceledSynchronously || !errors.Is(p.cancellation, context.Canceled) {
		t.Fatal("overflow did not synchronously cancel actual callback context")
	}
	_, a, ae := f.b.assessmentAttempt(c.Binding, *c.Slot)
	if ae != nil || !a.DenyLatched || a.FailureCode != "output_limit" || a.PhaseRecords.Checker != nil || a.Compute.ChargedCPUSeconds != 0 || !a.AccountingUnresolved || p.starts != 1 {
		t.Fatal("overflow failure lost held charge/no-checker semantics", ae, got.r.Code)
	}
}
