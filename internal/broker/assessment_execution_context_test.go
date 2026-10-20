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

// These callbacks deliberately return their already-successful result after
// caller cancellation. The core must decide whether a later effect is allowed;
// a cooperative provider must not hide the missing boundary in this control.
type executionContextGate struct {
	entered, release       chan struct{}
	enterOnce, releaseOnce sync.Once
}

func (g *executionContextGate) wait(ctx context.Context) {
	g.enterOnce.Do(func() { close(g.entered) })
	select {
	case <-g.release:
	case <-ctx.Done():
	}
}
func (g *executionContextGate) unblock() { g.releaseOnce.Do(func() { close(g.release) }) }

type executionContextStorage struct {
	assessmentStorage
	gate *executionContextGate
}

func (s executionContextStorage) BeforePhase(ctx context.Context, scope x.ExecutionScope, phase string) error {
	if e := s.assessmentStorage.BeforePhase(ctx, scope, phase); e != nil {
		return e
	}
	s.gate.wait(ctx)
	return nil
}

type executionContextProvider struct {
	*executionTestProvider
	gate              *executionContextGate
	afterCreate       bool
	creates, starts   int
	startContext      string
	trace             *executionContextTrace
	expiry            *executionExpiryTrace
	created, removed  x.DockerResourceIdentity
	inspects, removes int
}

func (p *executionContextProvider) Create(ctx context.Context, request assessmentPhaseRequest) (x.DockerResourceIdentity, error) {
	p.expiry.bindCallback(ctx)
	p.trace.record(ctx, "create_entry")
	p.creates++
	id, e := p.executionTestProvider.Create(ctx, request)
	if e == nil {
		p.mu.Lock()
		p.created = id
		p.mu.Unlock()
	}
	if e == nil && p.afterCreate {
		p.gate.wait(ctx)
	}
	p.trace.record(ctx, "create_return")
	return id, e
}
func (p *executionContextProvider) Start(ctx context.Context, id x.DockerResourceIdentity, input io.Reader, prior []byte, out, stderr io.Writer) (assessmentCompletion, error) {
	p.trace.record(ctx, "start_entry")
	p.starts++
	p.startContext = executionProcessErrorClass(ctx.Err())
	if e := ctx.Err(); e != nil {
		return assessmentCompletion{}, e
	}
	return assessmentCompletion{}, x.ErrHeld
}

func (p *executionContextProvider) Inspect(ctx context.Context, request assessmentPhaseRequest) (assessmentResourceObservation, error) {
	p.mu.Lock()
	p.inspects++
	p.mu.Unlock()
	obs, e := p.executionTestProvider.Inspect(ctx, request)
	p.expiry.record("inspect_return", e)
	return obs, e
}
func (p *executionContextProvider) Remove(ctx context.Context, id x.DockerResourceIdentity) error {
	e := p.executionTestProvider.Remove(ctx, id)
	p.expiry.record("remove_return", e)
	if e == nil {
		p.mu.Lock()
		p.removes++
		p.removed = id
		p.mu.Unlock()
	}
	return e
}

func TestExecutionCallerCancelSuppressesLaterProviderEffects(t *testing.T) {
	for _, point := range []string{"before-create", "after-create-before-start"} {
		t.Run(point, func(t *testing.T) {
			f := newExecutionFixture(t)
			var expiry *executionExpiryTrace
			if point == "after-create-before-start" {
				expiry = &executionExpiryTrace{}
				// Before seal: observe the actual copied watcher without parking it.
				f.b.assessment.owned.fault = expiry.hook
				originalTransport := f.b.assessment.transport
				f.b.assessment.transport = func(ctx context.Context, scope x.ExecutionScope, phase string) (io.Closer, error) {
					transport, e := originalTransport(ctx, scope, phase)
					if transport != nil {
						transport = &executionExpiryTransport{transport, expiry}
					}
					return transport, e
				}
				// LIFO: owner join attempt, snapshot, then fixture teardown. The
				// after_cleanup label does not prove a timed-out owner drained.
				t.Cleanup(func() { expiry.log(t, "after_cleanup") })
			}
			status := f.seal(t)
			gate := &executionContextGate{entered: make(chan struct{}), release: make(chan struct{})}
			p := &executionContextProvider{executionTestProvider: f.p, gate: gate, afterCreate: point == "after-create-before-start", trace: newExecutionContextTrace(), expiry: expiry, startContext: "none"}
			f.b.assessment.provider = p
			if !p.afterCreate {
				f.b.assessment.storage = executionContextStorage{f.b.assessment.storage, gate}
			}
			c := executionControl(f, status, 0)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			expiry.bind(time.Now(), ctx)
			type answer struct {
				result x.Result
				err    error
			}
			done := make(chan answer, 1)
			drained := make(chan struct{})
			go func() {
				defer close(drained)
				p.trace.record(ctx, "run_entry")
				r, e := f.b.RunAssessmentOnce(ctx, c)
				p.trace.record(ctx, "run_return")
				expiry.record("run_return", e)
				done <- answer{r, e}
			}()
			t.Cleanup(func() {
				cancel()
				gate.unblock()
				select {
				case <-drained:
				case <-time.After(4 * time.Second):
					t.Error("original context-control owner did not drain")
				}
			})
			select {
			case <-gate.entered:
			case <-done:
				p.trace.log(t)
				t.Fatal("run returned before deterministic cancellation gate")
			case <-time.After(2 * time.Second):
				p.trace.log(t)
				t.Fatal("deterministic cancellation gate not reached")
			}
			_, before, beforeErr := f.b.assessmentAttempt(c.Binding, *c.Slot)
			reservedDeadline, _ := time.Parse("2006-01-02T15:04:05Z", before.Deadline)
			if beforeErr != nil || ctx.Err() != nil || !reservedDeadline.After(time.Now()) {
				p.trace.log(t)
				t.Fatal("late cancel gate did not retain a live caller and reservation")
			}
			// Enable before cancel: the gate may return on ctx.Done before unblock.
			expiry.record("release", nil)
			cancel()
			gate.unblock()
			var got answer
			select {
			case got = <-done:
			case <-time.After(4 * time.Second):
				p.trace.log(t)
				expiry.log(t, "before_fatal")
				t.Fatal("original owner did not drain after caller cancellation")
			}
			p.trace.log(t)
			_, a, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
			deadline, _ := time.Parse("2006-01-02T15:04:05Z", a.Deadline)
			t.Logf("late cancel control: creates=%d starts=%d start_context=%s result_error=%s caller_context=%s reserved_deadline_future=%t attempt_read=%s deny=%t serial_clear=%t unresolved=%t reserved=%d charged=%d checker_present=%t", p.creates, p.starts, p.startContext, executionProcessErrorClass(got.err), executionProcessErrorClass(ctx.Err()), deadline.After(time.Now()), executionProcessErrorClass(e), a.DenyLatched, a.SerialClear(), a.AccountingUnresolved, a.Compute.ReservedCPUSeconds, a.Compute.ChargedCPUSeconds, a.PhaseRecords.Checker != nil)
			wantCreates := 0
			if p.afterCreate {
				wantCreates = 1
			}
			// The original red assertions are retained: count actual callback entries,
			// rather than relying on a cooperative provider to reject cancellation.
			if p.creates != wantCreates || p.starts != 0 {
				t.Errorf("provider effect after caller cancellation: creates=%d starts=%d", p.creates, p.starts)
			}
			if e != nil || got.err == nil || !a.DenyLatched || a.PhaseRecords.Checker != nil || a.Compute.ReservedCPUSeconds != 30 || p.starts > 0 && (a.SerialClear() || !a.AccountingUnresolved || a.Compute.ChargedCPUSeconds != 0) {
				t.Error("late cancellation fabricated success or ambiguity clearance")
			}
			if p.afterCreate {
				p.mu.Lock()
				created, removed, inspects, removes := p.created, p.removed, p.inspects, p.removes
				resource := p.resources[x.ResourceName(c.Binding.BrokerAuthorityID, c.Slot.AttemptID, "workload")]
				p.mu.Unlock()
				phase := a.PhaseRecords.Workload
				if phase == nil || !phase.CreateReturned || phase.ResourceIdentity == nil || *phase.ResourceIdentity != created || created.ContainerID == "" || phase.StartClaimed || !phase.StoppedObserved || !phase.RemovalObserved || resource.Exists || resource.Identity != created || removed != created || inspects == 0 || removes != 1 {
					t.Error("known Create identity or actual exact cleanup was lost after cancellation")
				}
			}
		})
	}
}

// Existing after-journal faults run outside the Store lock. Cancellation here
// proves that a durable claim cannot be converted to a fake return/close just
// because the final callback gate suppresses dispatch.
func TestExecutionCallerCancelAfterClaimRetainsUncertainty(t *testing.T) {
	for _, target := range []int32{1, 2, 3} {
		name := map[int32]string{1: "transport-claimed", 2: "create-claimed", 3: "start-claimed"}[target]
		t.Run(name, func(t *testing.T) {
			f := newExecutionFixture(t)
			status := f.seal(t)
			gate := &executionContextGate{entered: make(chan struct{}), release: make(chan struct{})}
			p := &executionContextProvider{executionTestProvider: f.p, trace: newExecutionContextTrace(), startContext: "none"}
			f.b.assessment.provider = p
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			var calls atomic.Int32
			f.b.assessment.owned.fault = func(point string) error {
				if point == "after-journal:"+string(x.AttemptPhaseForward) && calls.Add(1) == target {
					gate.wait(ctx)
				}
				return nil
			}
			c := executionControl(f, status, 0)
			done := make(chan error, 1)
			drained := make(chan struct{})
			go func() {
				defer close(drained)
				_, e := f.b.RunAssessmentOnce(ctx, c)
				done <- e
			}()
			t.Cleanup(func() {
				cancel()
				gate.unblock()
				select {
				case <-drained:
				case <-time.After(4 * time.Second):
					t.Error("post-claim original owner did not drain")
				}
			})
			select {
			case <-gate.entered:
			case <-done:
				t.Fatal("post-claim run returned before gate")
			case <-ctx.Done():
				t.Fatal("post-claim gate not reached while original caller live")
			}
			_, before, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
			deadline, _ := time.Parse("2006-01-02T15:04:05Z", before.Deadline)
			phase := before.PhaseRecords.Workload
			if e != nil || ctx.Err() != nil || !deadline.After(time.Now()) || phase == nil || !phase.TransportStarted || target >= 2 && !phase.CreateClaimed || target == 3 && (!phase.StartClaimed || !phase.CreateReturned || phase.ResourceIdentity == nil) {
				t.Fatal("post-claim live caller/reservation/claim precondition missing")
			}
			cancel()
			gate.unblock()
			select {
			case e = <-done:
			case <-time.After(4 * time.Second):
				t.Fatal("post-claim original owner did not return")
			}
			_, a, readErr := f.b.assessmentAttempt(c.Binding, *c.Slot)
			phase = a.PhaseRecords.Workload
			wantCreates := 0
			if target == 3 {
				wantCreates = 1
			}
			if e == nil || readErr != nil || phase == nil || p.creates != wantCreates || p.starts != 0 || !a.DenyLatched || !a.AccountingUnresolved || a.SerialClear() || a.ReaderCloseConfirmed || a.CollectorCloseConfirmed || a.Compute.ReservedCPUSeconds != 30 || a.Compute.ChargedCPUSeconds != 0 || a.PhaseRecords.Checker != nil {
				t.Fatal("post-claim suppression fabricated dispatch, global close or settlement")
			}
			switch target {
			case 1:
				if !phase.TransportStarted || phase.TransportCloseConfirmed || phase.CreateClaimed {
					t.Error("skipped constructor fabricated close")
				}
			case 2:
				if !phase.CreateClaimed || phase.CreateReturned || phase.ResourceIdentity != nil || phase.StartClaimed {
					t.Error("skipped Create fabricated return or identity")
				}
			case 3:
				p.mu.Lock()
				created, removed, removes := p.created, p.removed, p.removes
				p.mu.Unlock()
				if !phase.StartClaimed || phase.StartReturned || !phase.CreateReturned || phase.ResourceIdentity == nil || *phase.ResourceIdentity != created || removed != created || removes != 1 || !phase.RemovalObserved {
					t.Error("skipped Start lost exact cleanup or fabricated return")
				}
			}
		})
	}
}

// Count the actual original factory and delegated Close. A downstream factory
// call is indirect evidence of OpenInput success, never a raw FD/syscall proof.
type executionContextCollector struct {
	assessmentCollector
	closes *atomic.Int32
}

func (c *executionContextCollector) Close() error {
	e := c.assessmentCollector.Close()
	c.closes.Add(1)
	return e
}

func TestExecutionCallerCancelReaderCollectorBeforeDispatch(t *testing.T) {
	for _, point := range []string{"reader-after-claim", "collector-after-claim"} {
		t.Run(point, func(t *testing.T) {
			f := newExecutionFixture(t)
			status := f.seal(t)
			c := executionControl(f, status, 0)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			gate := &executionContextGate{entered: make(chan struct{}), release: make(chan struct{})}
			event := x.AttemptReader
			if point == "collector-after-claim" {
				event = x.AttemptCollector
			}
			var entered atomic.Bool
			f.b.assessment.owned.fault = func(hook string) error {
				if hook == "after-journal:"+string(event) && entered.CompareAndSwap(false, true) {
					gate.wait(ctx)
				}
				return nil
			}
			var factories, closes atomic.Int32
			originalFactory := f.b.assessment.collector
			f.b.assessment.collector = func(ctx context.Context, scope x.ExecutionScope) (assessmentCollector, error) {
				factories.Add(1)
				collector, e := originalFactory(ctx, scope)
				if collector != nil {
					collector = &executionContextCollector{collector, &closes}
				}
				return collector, e
			}
			p := &executionContextProvider{executionTestProvider: f.p, trace: newExecutionContextTrace(), startContext: "none"}
			f.b.assessment.provider = p
			done := make(chan error, 1)
			drained := make(chan struct{})
			go func() {
				defer close(drained)
				_, e := f.b.RunAssessmentOnce(ctx, c)
				done <- e
			}()
			t.Cleanup(func() {
				cancel()
				gate.unblock()
				select {
				case <-drained:
				case <-time.After(4 * time.Second):
					t.Error("reader/collector original owner did not drain")
				}
			})
			select {
			case <-gate.entered:
			case <-done:
				t.Fatal("reader/collector run returned before exact gate")
			case <-time.After(2 * time.Second):
				t.Fatal("reader/collector exact gate not reached")
			}
			_, before, e := f.b.assessmentAttempt(c.Binding, *c.Slot)
			deadline, _ := time.Parse("2006-01-02T15:04:05Z", before.Deadline)
			if e != nil || ctx.Err() != nil || !deadline.After(time.Now()) || !before.ReaderStarted || point == "collector-after-claim" && !before.CollectorStarted {
				t.Fatal("reader/collector claim lacks live caller and future reservation")
			}
			cancel()
			gate.unblock()
			var runErr error
			select {
			case runErr = <-done:
			case <-time.After(4 * time.Second):
				t.Fatal("reader/collector original owner did not return")
			}
			_, a, readErr := f.b.assessmentAttempt(c.Binding, *c.Slot)
			calls, closeCalls := factories.Load(), closes.Load()
			t.Logf("reader/collector observed: factory_calls=%d delegated_closes=%d run_error=%s read_error=%s reader_started=%t reader_closed=%t collector_started=%t collector_closed=%t ownership_closed=%t deny=%t unresolved=%t serial_clear=%t reserved=%d charged=%d creates=%d starts=%d phases_present=%t", calls, closeCalls, executionExpiryError(runErr), executionExpiryError(readErr), a.ReaderStarted, a.ReaderCloseConfirmed, a.CollectorStarted, a.CollectorCloseConfirmed, a.OwnershipCloseConfirmed, a.DenyLatched, a.AccountingUnresolved, a.SerialClear(), a.Compute.ReservedCPUSeconds, a.Compute.ChargedCPUSeconds, p.creates, p.starts, a.PhaseRecords.Workload != nil || a.PhaseRecords.Checker != nil)
			if calls != 0 {
				t.Errorf("collector factory dispatched after known caller cancellation: calls=%d", calls)
			}
			if readErr != nil || runErr == nil || !a.DenyLatched || a.Compute.ReservedCPUSeconds != 30 || p.creates != 0 || p.starts != 0 || a.PhaseRecords.Workload != nil || a.PhaseRecords.Checker != nil {
				t.Error("reader/collector cancellation fabricated success or provider dispatch")
			}
			// In the unfixed red, actual returned handles can truthfully close and
			// settle. Apply skipped-construction proof requirements only when the
			// factory was actually suppressed, not to those real closure facts.
			if calls == 0 {
				if !a.AccountingUnresolved || a.SerialClear() || a.Compute.ChargedCPUSeconds != 0 || a.CollectorCloseConfirmed || closeCalls != 0 {
					t.Error("skipped claimed construction fabricated close or settlement")
				}
				if point == "reader-after-claim" && (a.ReaderCloseConfirmed || a.CollectorStarted) {
					t.Error("skipped reader construction fabricated reader close or later claim")
				}
				if point == "collector-after-claim" && !a.ReaderCloseConfirmed {
					t.Error("actual returned reader lost its checked close")
				}
			}
		})
	}
}
