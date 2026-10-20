package broker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
)

// This helper is compiled only into the test executable. It is a tiny synthetic
// phase, not Docker, a recipe implementation, a sampler, or an enforceable quota.
func TestExecutionPrivatePhaseHelper(t *testing.T) {
	if os.Getenv("OUTFITTER_PRIVATE_EXECUTION_PHASE") != "1" {
		return
	}
	metadata := os.NewFile(3, "private-phase-control")
	prior := os.NewFile(4, "private-phase-prior")
	if metadata == nil || prior == nil {
		os.Exit(71)
	}
	raw, e := io.ReadAll(io.LimitReader(metadata, 32769))
	if e != nil || len(raw) > 32768 {
		os.Exit(72)
	}
	if metadata.Close() != nil {
		os.Exit(73)
	}
	var request assessmentPhaseRequest
	if e = json.Unmarshal(raw, &request); e != nil || request.Selection.Method != "synthetic_fixture" || request.Selection.Seed != 11 || request.Selection.Slot != request.Slot || request.Limits.CPUCountPerAttempt != 1 || request.Limits.OutputBudgetBytes != 65536 || request.Deadline.Before(time.Now()) {
		os.Exit(74)
	}
	input, e := io.ReadAll(io.LimitReader(os.Stdin, 1025))
	if e != nil || string(input) != "PRIVATE_INPUT_POISON\n" {
		os.Exit(75)
	}
	previous, e := io.ReadAll(io.LimitReader(prior, 1025))
	if e != nil || prior.Close() != nil {
		os.Exit(76)
	}
	clear(input)
	// The cancellation control alone supplies these test-only inherited pipes.
	// Readiness follows actual child input validation; the gate cannot finish
	// normally while the parent retains its writer. Cancellation must reap us.
	if os.Getenv("OUTFITTER_PRIVATE_EXECUTION_BLOCK") == "1" {
		ready := os.NewFile(5, "private-phase-ready")
		gate := os.NewFile(6, "private-phase-gate")
		if request.Phase != "workload" || ready == nil || gate == nil {
			os.Exit(82)
		}
		if _, e = ready.Write([]byte{1}); e != nil || ready.Close() != nil {
			os.Exit(83)
		}
		var release [1]byte
		_, _ = io.ReadFull(gate, release[:])
		_ = gate.Close()
		os.Exit(84)
	}
	switch request.Phase {
	case "workload":
		if len(previous) != 0 {
			os.Exit(77)
		}
		_, e = io.WriteString(os.Stdout, "PRIVATE_OUTPUT_POISON")
	case "checker":
		if string(previous) != "PRIVATE_OUTPUT_POISON" || request.Code.RecipeID != "independent-checker" {
			os.Exit(78)
		}
		_, e = io.WriteString(os.Stdout, `{"quality":"passed"}`)
	default:
		os.Exit(79)
	}
	clear(previous)
	if e != nil {
		os.Exit(80)
	}
	if _, e = io.WriteString(os.Stderr, "PRIVATE_STDERR_POISON"); e != nil {
		os.Exit(81)
	}
	os.Exit(0)
}

type executionProcessProvider struct {
	*executionTestProvider
	observed      []assessmentCompletion
	diagnostics   []executionProcessDiagnostic
	cancelAtReady context.CancelFunc
}

// Diagnostics contain only closed error classes, phase names and durations;
// never provider error text, resource identities, paths, or captured content.
type executionProcessDiagnostic struct {
	Phase                                                                      string
	Started                                                                    bool
	ChildReady                                                                 bool
	ElapsedMS, WaitMS                                                          int64
	DeadlineRemainingBeforeMS, DeadlineRemainingAfterMS                        int64
	ContextBefore, ContextAfter, StartError, WaitError, PipeError, ResultError string
	ExitCode                                                                   int
}

func executionProcessErrorClass(e error) string {
	switch {
	case e == nil:
		return "none"
	case errors.Is(e, context.Canceled):
		return "canceled"
	case errors.Is(e, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(e, exec.ErrWaitDelay):
		return "wait_delay"
	case errors.Is(e, io.ErrShortWrite):
		return "short_write"
	}
	var exit *exec.ExitError
	if errors.As(e, &exit) {
		return "exit"
	}
	var path *os.PathError
	if errors.As(e, &path) {
		return "os"
	}
	return "other"
}

func (p *executionProcessProvider) Start(ctx context.Context, id x.DockerResourceIdentity, input io.Reader, prior []byte, out, errOut io.Writer) (completion assessmentCompletion, resultErr error) {
	begin := time.Now()
	diagnostic := executionProcessDiagnostic{Phase: id.Phase, ContextBefore: executionProcessErrorClass(ctx.Err()), ExitCode: -1}
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		diagnostic.DeadlineRemainingBeforeMS = time.Until(deadline).Milliseconds()
	}
	defer func() {
		diagnostic.ElapsedMS = time.Since(begin).Milliseconds()
		diagnostic.ContextAfter = executionProcessErrorClass(ctx.Err())
		diagnostic.ResultError = executionProcessErrorClass(resultErr)
		if hasDeadline {
			diagnostic.DeadlineRemainingAfterMS = time.Until(deadline).Milliseconds()
		}
		p.mu.Lock()
		p.diagnostics = append(p.diagnostics, diagnostic)
		p.mu.Unlock()
	}()
	var request assessmentPhaseRequest
	p.mu.Lock()
	for _, r := range p.requests {
		if r.Slot.AttemptID == id.AttemptID && r.Phase == id.Phase {
			request = r
		}
	}
	if request.Phase == "" {
		p.mu.Unlock()
		return assessmentCompletion{}, errors.New("unbound synthetic resource")
	}
	p.starts++
	p.active++
	if p.active > p.maxActive {
		p.maxActive = p.active
	}
	p.mu.Unlock()
	defer func() { p.mu.Lock(); p.active--; p.mu.Unlock() }()
	metaRead, metaWrite, e := os.Pipe()
	if e != nil {
		return assessmentCompletion{}, e
	}
	defer metaRead.Close()
	defer metaWrite.Close()
	priorRead, priorWrite, e := os.Pipe()
	if e != nil {
		return assessmentCompletion{}, e
	}
	defer priorRead.Close()
	defer priorWrite.Close()
	raw, e := json.Marshal(request)
	if e != nil || len(raw) > 32768 {
		return assessmentCompletion{}, errors.New("bounded synthetic phase metadata")
	}
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExecutionPrivatePhaseHelper$")
	cmd.Env = []string{"OUTFITTER_PRIVATE_EXECUTION_PHASE=1", "GOMAXPROCS=2"}
	cmd.ExtraFiles = []*os.File{metaRead, priorRead}
	var readyRead, readyWrite, gateRead, gateWrite *os.File
	if p.cancelAtReady != nil {
		readyRead, readyWrite, e = os.Pipe()
		if e != nil {
			return assessmentCompletion{}, e
		}
		defer readyRead.Close()
		defer readyWrite.Close()
		gateRead, gateWrite, e = os.Pipe()
		if e != nil {
			return assessmentCompletion{}, e
		}
		defer gateRead.Close()
		defer gateWrite.Close()
		cmd.Env = append(cmd.Env, "OUTFITTER_PRIVATE_EXECUTION_BLOCK=1")
		cmd.ExtraFiles = append(cmd.ExtraFiles, readyWrite, gateRead)
	}
	cmd.Stdin = input
	cmd.Stdout = out
	cmd.Stderr = errOut
	cmd.WaitDelay = 250 * time.Millisecond
	started := time.Now().UTC()
	if e = cmd.Start(); e != nil {
		diagnostic.StartError = executionProcessErrorClass(e)
		return assessmentCompletion{}, e
	}
	diagnostic.Started = true
	// Parent copies are closed immediately after the actual inherited handles
	// exist. Independent child exit/Wait and exact pipe completion are required.
	parentCloseErr := metaRead.Close()
	if e = priorRead.Close(); parentCloseErr == nil {
		parentCloseErr = e
	}
	if p.cancelAtReady != nil {
		if e = readyWrite.Close(); parentCloseErr == nil {
			parentCloseErr = e
		}
		if e = gateRead.Close(); parentCloseErr == nil {
			parentCloseErr = e
		}
	}
	writes := make(chan error, 2)
	write := func(f *os.File, b []byte) {
		n, e := f.Write(b)
		ce := f.Close()
		if e == nil && ce != nil {
			e = ce
		}
		if e == nil && n != len(b) {
			e = io.ErrShortWrite
		}
		writes <- e
	}
	go write(metaWrite, raw)
	go write(priorWrite, prior)
	if p.cancelAtReady != nil {
		var ready [1]byte
		_, e = io.ReadFull(readyRead, ready[:])
		if e == nil && ready[0] == 1 {
			diagnostic.ChildReady = true
			p.cancelAtReady()
		} else if parentCloseErr == nil {
			parentCloseErr = errors.New("synthetic child readiness failed")
		}
	}
	waitBegin := time.Now()
	waitErr := cmd.Wait()
	diagnostic.WaitMS = time.Since(waitBegin).Milliseconds()
	diagnostic.WaitError = executionProcessErrorClass(waitErr)
	diagnostic.ExitCode = cmd.ProcessState.ExitCode()
	finished := time.Now().UTC()
	for i := 0; i < 2; i++ {
		select {
		case e := <-writes:
			if e != nil && parentCloseErr == nil {
				parentCloseErr = e
			}
		case <-ctx.Done():
			if parentCloseErr == nil {
				parentCloseErr = ctx.Err()
			}
		}
	}
	diagnostic.PipeError = executionProcessErrorClass(parentCloseErr)
	observed := assessmentCompletion{ExitCode: int64(cmd.ProcessState.ExitCode()), StartedAt: &started, FinishedAt: &finished}
	p.mu.Lock()
	v := p.resources[request.OwnerName]
	v.Stopped = true
	p.resources[request.OwnerName] = v
	p.observed = append(p.observed, observed)
	p.mu.Unlock()
	if waitErr != nil {
		return observed, waitErr
	}
	if parentCloseErr != nil {
		return observed, parentCloseErr
	}
	return observed, nil
}
func TestExecutionSeparateSequentialPhaseProcesses(t *testing.T) {
	f := newExecutionFixture(t)
	s := f.seal(t)
	p := &executionProcessProvider{executionTestProvider: f.p}
	f.b.assessment.provider = p
	// Journal validation under -race plus two actual child exits exceeded the
	// original five-second caller context (retained red evidence). Leave bounded
	// headroom within the unchanged immutable 30-second reservation; cancellation
	// behavior is checked separately at an actual child readiness gate below.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c := executionControl(f, s, 0)
	r, e := f.b.RunAssessmentOnce(ctx, c)
	p.mu.Lock()
	diagnostics := append([]executionProcessDiagnostic(nil), p.diagnostics...)
	starts, active, maxActive := p.starts, p.active, p.maxActive
	p.mu.Unlock()
	t.Logf("synthetic process diagnostics: ctx=%s starts=%d active=%d max_active=%d phases=%+v", executionProcessErrorClass(ctx.Err()), starts, active, maxActive, diagnostics)
	if e != nil || r.Code != "OK" || r.Disposition != "new_attempt" || r.Attempt == nil || r.Attempt.Outcome != "succeeded" {
		scope, attempt, readErr := f.b.assessmentAttempt(c.Binding, *c.Slot)
		// Read the actual durable state, but emit only enums, counters and flags.
		t.Logf("synthetic process state: read=%s scope=%s reason=%s attempt=%s failure=%s charge=%d readers=%t/%t collector=%t/%t ownership=%t/%t", executionProcessErrorClass(readErr), scope.State, scope.Reason, attempt.State, attempt.FailureCode, attempt.Compute.ChargedCPUSeconds, attempt.ReaderCloseConfirmed, attempt.ReaderCloseUncertain, attempt.CollectorCloseConfirmed, attempt.CollectorCloseUncertain, attempt.OwnershipCloseConfirmed, attempt.OwnershipCloseUncertain)
		t.Fatal("separate phases", r.Code, r.Disposition, executionProcessErrorClass(e))
	}
	if len(p.observed) != 2 || p.starts != 2 || p.maxActive != 1 {
		t.Fatal("phase count/overlap", p.starts, p.maxActive)
	}
	if p.observed[0].FinishedAt == nil || p.observed[1].StartedAt == nil || p.observed[1].StartedAt.Before(*p.observed[0].FinishedAt) {
		t.Fatal("checker started before workload Wait")
	}
	if r.Attempt.Phases.Workload.StartedAt == nil || r.Attempt.Phases.Workload.FinishedAt == nil || *r.Attempt.Phases.Workload.StartedAt == *r.Attempt.Phases.Workload.FinishedAt {
		t.Fatal("phase timing was replaced with a post-return timestamp")
	}
	raw, e := x.EncodeReply(r)
	if e != nil {
		t.Fatal(e)
	}
	finalScope, finalAttempt, ae := f.b.assessmentAttempt(c.Binding, *c.Slot)
	if ae != nil {
		t.Fatal(ae)
	}
	if _, e = x.DecodeResult(raw, c, finalScope, &finalAttempt); e != nil {
		t.Fatal("reply correlation", e)
	}
	for _, poison := range []string{"PRIVATE_INPUT_POISON", "PRIVATE_OUTPUT_POISON", "PRIVATE_STDERR_POISON"} {
		if strings.Contains(string(raw), poison) {
			t.Fatal("raw result content in public reply")
		}
	}
	if len(p.resources) != 2 {
		t.Fatal("missing permanent resource observations")
	}
	for _, observation := range p.resources {
		if observation.Exists || !observation.Stopped || observation.Identity.AttemptID != c.Slot.AttemptID || observation.Identity.ScopeID != c.Binding.ScopeID || observation.Identity.BrokerOwner != c.Binding.BrokerAuthorityID {
			t.Fatal("owned synthetic resource was not exactly removed")
		}
	}
}

func TestExecutionActualPhaseProcessCancellationKeepsAmbiguousStartHeld(t *testing.T) {
	f := newExecutionFixture(t)
	s := f.seal(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	p := &executionProcessProvider{executionTestProvider: f.p, cancelAtReady: cancel}
	f.b.assessment.provider = p
	c := executionControl(f, s, 0)
	r, e := f.b.RunAssessmentOnce(ctx, c)
	p.mu.Lock()
	diagnostics := append([]executionProcessDiagnostic(nil), p.diagnostics...)
	starts, active, maxActive := p.starts, p.active, p.maxActive
	observed := append([]assessmentCompletion(nil), p.observed...)
	resources := make([]assessmentResourceObservation, 0, len(p.resources))
	for _, resource := range p.resources {
		resources = append(resources, resource)
	}
	p.mu.Unlock()
	t.Logf("actual child cancellation diagnostics: ctx=%s starts=%d active=%d max_active=%d phases=%+v", executionProcessErrorClass(ctx.Err()), starts, active, maxActive, diagnostics)
	if !errors.Is(ctx.Err(), context.Canceled) || !errors.Is(e, x.ErrHeld) {
		t.Fatal("actual child gate did not cancel the original run", executionProcessErrorClass(ctx.Err()), executionProcessErrorClass(e))
	}
	if starts != 1 || active != 0 || maxActive != 1 || len(diagnostics) != 1 || !diagnostics[0].Started || !diagnostics[0].ChildReady || diagnostics[0].ContextAfter != "canceled" || diagnostics[0].WaitError != "exit" || len(observed) != 1 || observed[0].FinishedAt == nil || observed[0].ExitCode == 0 {
		t.Fatal("cancellation lacked an actual ready child and definitive failed Wait")
	}
	_, a, ae := f.b.assessmentAttempt(c.Binding, *c.Slot)
	if ae != nil || r.Code != "HELD" || a.State != "held" || !a.DenyLatched || a.FailureCode != "execution_failed" || a.SerialClear() || !a.AccountingUnresolved || a.PhaseRecords.Checker != nil || a.PhaseRecords.Workload == nil || !a.PhaseRecords.Workload.StartClaimed || a.PhaseRecords.Workload.StartReturned || a.ReaderCloseConfirmed || a.CollectorCloseConfirmed {
		t.Fatal("actual child cancellation fabricated callback return/global finality", executionProcessErrorClass(ae))
	}
	if len(resources) != 1 || resources[0].Exists || !resources[0].Stopped {
		t.Fatal("exact canceled child resource was not removed")
	}
	// Actual Wait/reap is a local fact; it cannot retrofit the missing successful
	// Start return or grant a distinct slot admission through public metadata.
	currentScope, _, scopeErr := f.b.assessmentRead(c.Binding)
	usage, usageErr := f.b.Store.Usage(c.Binding.ProjectID)
	if scopeErr != nil || currentScope.State != "sealed" || currentScope.DenyLatched || !currentScope.TransientOwnershipClosed(false) || usageErr != nil || usage.Budget != 60 || usage.Reserved != 30 || usage.Unknown != 30 || usage.Charged != 0 || usage.Remaining != 30 {
		t.Fatal("next-slot negative lacks fresh scope and sufficient unspent budget")
	}
	second := executionControl(f, currentScope.Status("status", "OK", "observed_current", nil), 1)
	if _, nextErr := f.b.RunAssessmentOnce(context.Background(), second); nextErr == nil {
		t.Fatal("canceled ambiguous start admitted another planned slot")
	}
	if e := f.b.Store.View(func(d ledger.Data) error {
		if len(d.AssessmentExecution.Attempts) != 1 {
			t.Error("refused next slot created another permanent attempt")
		}
		return nil
	}); e != nil {
		t.Fatal("next-slot journal read failed")
	}
	afterUsage, usageErr := f.b.Store.Usage(c.Binding.ProjectID)
	if usageErr != nil || afterUsage != usage {
		t.Fatal("refused next slot changed reservation or charge")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.starts != 1 {
		t.Fatal("refused next slot started another child")
	}
}
