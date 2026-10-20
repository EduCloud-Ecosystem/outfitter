package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
)

// This separate test fixture leaves the accepted process helper unchanged.
// Its actual phase child checks every open descriptor in a bounded inventory
// against the original session pipe identities, including the watch pipe.
type executionSessionPhaseProvider struct {
	*executionTestProvider
	forbidden   []x.FilesystemIdentity
	diagnostics []executionSessionPhaseTrace
}

type executionSessionPhaseDiagnostic struct {
	Phase             string `json:"phase"`
	StartEntered      bool   `json:"start_entered"`
	CmdStarted        bool   `json:"cmd_started"`
	ExitKnown         bool   `json:"exit_known"`
	ExitCode          int    `json:"exit_code"`
	ElapsedMS         int64  `json:"elapsed_ms"`
	WaitMS            int64  `json:"wait_ms"`
	RemainingBeforeMS int64  `json:"remaining_before_ms"`
	RemainingAfterMS  int64  `json:"remaining_after_ms"`
	ContextBefore     string `json:"context_before"`
	ContextAfter      string `json:"context_after"`
	StartError        string `json:"start_error"`
	WaitError         string `json:"wait_error"`
	PipeError         string `json:"pipe_error"`
}
type executionSessionPhaseTrace struct {
	attemptID  string // internal selection only, never serialized or logged
	diagnostic executionSessionPhaseDiagnostic
}

func executionSessionDiagnosticMS(n int64) int64 {
	if n < -125000 {
		return -125000
	}
	if n > 125000 {
		return 125000
	}
	return n
}

func executionSessionRemaining(ctx context.Context) int64 {
	if d, ok := ctx.Deadline(); ok {
		return executionSessionDiagnosticMS(time.Until(d).Milliseconds())
	}
	return 0
}

func (p *executionSessionPhaseProvider) phaseDiagnostics(attempt string) []executionSessionPhaseDiagnostic {
	p.mu.Lock()
	defer p.mu.Unlock()
	rows := make([]executionSessionPhaseDiagnostic, 0, 2)
	for _, row := range p.diagnostics {
		if row.attemptID == attempt && len(rows) < 2 {
			rows = append(rows, row.diagnostic)
		}
	}
	return rows
}

type executionSessionPhaseEnvelope struct {
	Request   assessmentPhaseRequest `json:"request"`
	Forbidden []x.FilesystemIdentity `json:"forbidden"`
}

func newExecutionSessionPhaseProvider(files [9]*os.File) (*executionSessionPhaseProvider, error) {
	p := &executionSessionPhaseProvider{executionTestProvider: &executionTestProvider{resources: map[string]assessmentResourceObservation{}}}
	for _, f := range files {
		id, e := openedIdentity(f, "pipe")
		if e != nil {
			return nil, e
		}
		p.forbidden = append(p.forbidden, id)
	}
	return p, nil
}

// Older fixtures combined all these refusals into exit73. These private
// branch codes reuse the bounded ExitCode diagnostic; they grant no authority.
const (
	executionSessionPhaseMetadataDecode = 83 + iota
	executionSessionPhaseForbiddenCount
	executionSessionPhaseForbiddenIdentity
	executionSessionPhaseInventoryOpen
	executionSessionPhaseInventoryRead
	executionSessionPhaseInventoryClose
	executionSessionPhaseInventoryOverflow
	executionSessionPhaseEntryName
	executionSessionPhaseFstat
	executionSessionPhaseFlagQuery
	executionSessionPhaseNotCloexec
	executionSessionPhaseForbiddenMatch
)

func executionSessionPhaseFDCheck(forbidden []x.FilesystemIdentity) int {
	if len(forbidden) != 9 {
		return executionSessionPhaseForbiddenCount
	}
	for _, id := range forbidden {
		if !id.Valid() {
			return executionSessionPhaseForbiddenIdentity
		}
	}
	directory, e := os.Open("/dev/fd")
	if e != nil {
		return executionSessionPhaseInventoryOpen
	}
	entries, e := directory.ReadDir(129)
	closeErr := directory.Close()
	if e != nil && e != io.EOF {
		return executionSessionPhaseInventoryRead
	}
	if closeErr != nil {
		return executionSessionPhaseInventoryClose
	}
	if len(entries) > 128 {
		return executionSessionPhaseInventoryOverflow
	}
	for _, entry := range entries {
		fd, e := strconv.ParseUint(entry.Name(), 10, 20)
		if e != nil {
			return executionSessionPhaseEntryName
		}
		var st syscall.Stat_t
		e = syscall.Fstat(int(fd), &st)
		// ReadDir has already closed its own temporary directory descriptor.
		if e == syscall.EBADF {
			continue
		}
		if e != nil {
			return executionSessionPhaseFstat
		}
		// Only stdio and the reviewed metadata/prior remaps may survive exec
		// without CLOEXEC. Any extra descriptor created by this Go child runtime
		// must itself be private; arbitrary inherited extras are refused.
		if fd >= 5 {
			flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFD, 0)
			if errno != 0 {
				return executionSessionPhaseFlagQuery
			}
			if flags&syscall.FD_CLOEXEC == 0 {
				return executionSessionPhaseNotCloexec
			}
		}
		id := x.FilesystemIdentity{Device: strconv.FormatUint(uint64(st.Dev), 10), Inode: strconv.FormatUint(uint64(st.Ino), 10)}
		for _, bad := range forbidden {
			if id == bad {
				return executionSessionPhaseForbiddenMatch
			}
		}
	}
	return 0
}

func executionSessionPhaseEntry() int {
	metadata, prior := os.NewFile(3, "private-session-phase-metadata"), os.NewFile(4, "private-session-phase-prior")
	if metadata == nil || prior == nil {
		if metadata != nil {
			_ = metadata.Close()
		}
		if prior != nil {
			_ = prior.Close()
		}
		return 71
	}
	raw, e := io.ReadAll(io.LimitReader(metadata, 32769))
	closeErr := metadata.Close()
	if e != nil || closeErr != nil || len(raw) > 32768 {
		_ = prior.Close()
		return 72
	}
	var envelope executionSessionPhaseEnvelope
	if x.DecodeClosed(raw, &envelope, 32768) != nil {
		_ = prior.Close()
		return executionSessionPhaseMetadataDecode
	}
	if code := executionSessionPhaseFDCheck(envelope.Forbidden); code != 0 {
		_ = prior.Close()
		return code
	}
	clear(raw)
	r := envelope.Request
	if r.Selection.Method != "synthetic_fixture" || r.Selection.Seed != 11 || r.Selection.Slot != r.Slot || r.Limits.CPUCountPerAttempt != 1 || r.Limits.OutputBudgetBytes != 65536 || !r.Deadline.After(time.Now()) {
		_ = prior.Close()
		return 74
	}
	input, e := io.ReadAll(io.LimitReader(os.Stdin, 1025))
	if e != nil || !bytes.Equal(input, executionSessionInputSource) {
		_ = prior.Close()
		clear(input)
		return 75
	}
	previous, e := io.ReadAll(io.LimitReader(prior, 1025))
	closeErr = prior.Close()
	clear(input)
	if e != nil || closeErr != nil {
		clear(previous)
		return 76
	}
	var output string
	switch r.Phase {
	case "workload":
		if len(previous) != 0 {
			clear(previous)
			return 77
		}
		output = "PRIVATE_OUTPUT_POISON"
	case "checker":
		if string(previous) != "PRIVATE_OUTPUT_POISON" || r.Code.RecipeID != "independent-checker" {
			clear(previous)
			return 78
		}
		output = `{"quality":"passed"}`
	default:
		clear(previous)
		return 79
	}
	clear(previous)
	if _, e = io.WriteString(os.Stdout, output); e != nil {
		return 80
	}
	if _, e = io.WriteString(os.Stderr, "PRIVATE_STDERR_POISON"); e != nil {
		return 81
	}
	return 0
}

func TestExecutionPrivateSessionPhaseHelper(t *testing.T) {
	if os.Getenv("OUTFITTER_PRIVATE_SESSION_PHASE") != "1" {
		return
	}
	// Entry closes all explicit metadata/prior handles before this selector exit.
	code := executionSessionPhaseEntry()
	for _, f := range []*os.File{os.Stdin, os.Stdout, os.Stderr} {
		if e := f.Close(); e != nil {
			code = 82
		}
	}
	os.Exit(code)
}

func (p *executionSessionPhaseProvider) Start(ctx context.Context, id x.DockerResourceIdentity, input io.Reader, prior []byte, out, errOut io.Writer) (completion assessmentCompletion, resultErr error) {
	p.mu.Lock()
	var request assessmentPhaseRequest
	for _, r := range p.requests {
		if r.Slot.AttemptID == id.AttemptID && r.Phase == id.Phase {
			request = r
		}
	}
	if request.Phase == "" {
		p.mu.Unlock()
		return completion, executionSessionProtocolError
	}
	entered := time.Now()
	diagnostic := executionSessionPhaseDiagnostic{Phase: request.Phase, StartEntered: true, ContextBefore: executionProcessErrorClass(ctx.Err()), ContextAfter: "none", StartError: "none", WaitError: "none", PipeError: "none", RemainingBeforeMS: executionSessionRemaining(ctx)}
	defer func() {
		diagnostic.ElapsedMS = executionSessionDiagnosticMS(time.Since(entered).Milliseconds())
		diagnostic.ContextAfter = executionProcessErrorClass(ctx.Err())
		diagnostic.RemainingAfterMS = executionSessionRemaining(ctx)
		p.mu.Lock()
		if len(p.diagnostics) < 4 {
			p.diagnostics = append(p.diagnostics, executionSessionPhaseTrace{request.Slot.AttemptID, diagnostic})
		}
		p.mu.Unlock()
	}()
	p.starts++
	p.active++
	if p.active > p.maxActive {
		p.maxActive = p.active
	}
	p.mu.Unlock()
	defer func() { p.mu.Lock(); p.active--; p.mu.Unlock() }()
	var handles []*os.File
	closed := map[*os.File]bool{}
	var closeMu sync.Mutex
	closeFile := func(f *os.File) error {
		closeMu.Lock()
		defer closeMu.Unlock()
		if f == nil || closed[f] {
			return nil
		}
		closed[f] = true
		return f.Close()
	}
	defer func() {
		for _, f := range handles {
			if e := closeFile(f); e != nil {
				diagnostic.PipeError = executionProcessErrorClass(e)
				resultErr = executionSessionProtocolError
			}
		}
	}()
	pipe := func() (*os.File, *os.File, error) {
		r, w, e := os.Pipe()
		if r != nil {
			handles = append(handles, r)
		}
		if w != nil {
			handles = append(handles, w)
		}
		return r, w, e
	}
	metaRead, metaWrite, e := pipe()
	if e != nil {
		return completion, e
	}
	priorRead, priorWrite, e := pipe()
	if e != nil {
		return completion, e
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return completion, executionSessionProtocolError
	}
	for _, f := range []*os.File{metaWrite, priorWrite} {
		if e = f.SetWriteDeadline(deadline); e != nil {
			return completion, e
		}
	}
	raw, e := json.Marshal(executionSessionPhaseEnvelope{Request: request, Forbidden: p.forbidden})
	if e != nil || len(raw) > 32768 {
		return completion, executionSessionProtocolError
	}
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExecutionPrivateSessionPhaseHelper$")
	cmd.Env = []string{"OUTFITTER_PRIVATE_SESSION_PHASE=1", "GOMAXPROCS=2"}
	cmd.ExtraFiles = []*os.File{metaRead, priorRead}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = input, out, errOut
	cmd.WaitDelay = 250 * time.Millisecond
	started := time.Now().UTC()
	if e = cmd.Start(); e != nil {
		diagnostic.StartError = executionProcessErrorClass(e)
		clear(raw)
		return completion, e
	}
	diagnostic.CmdStarted = true
	parentErr := closeFile(metaRead)
	if e = closeFile(priorRead); parentErr == nil {
		parentErr = e
	}
	writes := make(chan error, 2)
	write := func(f *os.File, b []byte) {
		e := executionSessionWrite(f, b)
		ce := closeFile(f)
		if e == nil {
			e = ce
		}
		writes <- e
	}
	go write(metaWrite, raw)
	go write(priorWrite, prior)
	waitStarted := time.Now()
	waitErr := cmd.Wait()
	diagnostic.WaitMS = executionSessionDiagnosticMS(time.Since(waitStarted).Milliseconds())
	diagnostic.WaitError = executionProcessErrorClass(waitErr)
	if cmd.ProcessState != nil {
		diagnostic.ExitKnown, diagnostic.ExitCode = true, cmd.ProcessState.ExitCode()
	}
	finished := time.Now().UTC()
	for i := 0; i < 2; i++ {
		if e = <-writes; e != nil {
			parentErr = executionSessionProtocolError
		}
	}
	clear(raw)
	diagnostic.PipeError = executionProcessErrorClass(parentErr)
	completion = assessmentCompletion{ExitCode: int64(cmd.ProcessState.ExitCode()), StartedAt: &started, FinishedAt: &finished}
	p.mu.Lock()
	observation := p.resources[request.OwnerName]
	observation.Stopped = true
	p.resources[request.OwnerName] = observation
	p.mu.Unlock()
	if waitErr != nil {
		return completion, waitErr
	}
	if parentErr != nil {
		return completion, parentErr
	}
	if cmd.ProcessState.ExitCode() != 0 {
		return completion, errors.New("synthetic phase failed")
	}
	return completion, nil
}
