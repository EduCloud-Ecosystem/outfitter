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
	observed []assessmentCompletion
}

func (p *executionProcessProvider) Start(ctx context.Context, id x.DockerResourceIdentity, input io.Reader, prior []byte, out, errOut io.Writer) (assessmentCompletion, error) {
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
	cmd.Stdin = input
	cmd.Stdout = out
	cmd.Stderr = errOut
	cmd.WaitDelay = 250 * time.Millisecond
	started := time.Now().UTC()
	if e = cmd.Start(); e != nil {
		return assessmentCompletion{}, e
	}
	// Parent copies are closed immediately after the actual inherited handles
	// exist. Independent child exit/Wait and exact pipe completion are required.
	parentCloseErr := metaRead.Close()
	if e = priorRead.Close(); parentCloseErr == nil {
		parentCloseErr = e
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
	waitErr := cmd.Wait()
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
	// This finite overall test context is smaller than the immutable synthetic
	// 30-second reservation. No real allocation is requested or consumed.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c := executionControl(f, s, 0)
	r, e := f.b.RunAssessmentOnce(ctx, c)
	if e != nil || r.Code != "OK" || r.Disposition != "new_attempt" || r.Attempt == nil || r.Attempt.Outcome != "succeeded" {
		t.Fatal("separate phases", r.Code, r.Disposition, e)
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
