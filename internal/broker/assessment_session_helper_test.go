package broker

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/assessment"
	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

//go:embed testdata/execution-session-a2-manifest.json
var executionSessionManifestSource []byte

//go:embed testdata/execution-session-a2-input.txt
var executionSessionInputSource []byte

// Config is a private fixture bootstrap file, never a production capability or
// an HTTP payload. The prebuilt test executable and this fixture are pinned by
// the driver; no compiler or installer is invoked from connected tests.
type executionSessionConfig struct {
	StateRoot           string                `json:"state_root"`
	DataRoot            string                `json:"data_root"`
	WaypointAuthorityID string                `json:"waypoint_authority_id"`
	ScopeID             string                `json:"scope_id"`
	OwnerEpoch          string                `json:"owner_epoch"`
	ScopeExpiresAt      string                `json:"scope_expires_at"`
	AuthorizationExpiry string                `json:"authorization_expiry"`
	ProjectExpiry       string                `json:"project_expiry"`
	PolicyExpiry        string                `json:"policy_expiry"`
	SessionDeadline     string                `json:"session_deadline"`
	Manifest            assessment.ManifestV2 `json:"manifest"`
}

func executionSessionExpiry(now time.Time, caps ...time.Time) time.Time {
	end := now.Add(30 * time.Minute)
	for _, cap := range caps {
		if cap.Before(end) {
			end = cap
		}
	}
	return end.UTC().Truncate(time.Second)
}

// Bind once before launch. Narrow the real fixture policy rather than making
// its clock unavailable after expiry; original denied cleanup still needs it.
func (c *executionSessionConfig) bindSessionDeadline(now time.Time) error {
	auth, e1 := owneddata.Time(c.AuthorizationExpiry, false)
	project, e2 := owneddata.Time(c.ProjectExpiry, false)
	policy, e3 := owneddata.Time(c.PolicyExpiry, false)
	manifest, e4 := owneddata.Time(c.Manifest.ExpiresAt, false)
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
		return executionSessionProtocolError
	}
	deadline := now.Add(executionSessionLifetime).UTC().Truncate(time.Microsecond)
	policy = executionSessionMinDeadline(policy, deadline).Truncate(time.Second)
	scope := executionSessionExpiry(now, auth, project, policy, manifest)
	if !scope.After(now) {
		return executionSessionProtocolError
	}
	c.SessionDeadline = owneddata.Stamp(deadline)
	c.PolicyExpiry = policy.Format(owneddata.SecondUTC)
	c.ScopeExpiresAt = scope.Format(owneddata.SecondUTC)
	return nil
}

func (c executionSessionConfig) grants(now time.Time) (ownedPolicy, time.Time, error) {
	var p ownedPolicy
	auth, e1 := owneddata.Time(c.AuthorizationExpiry, false)
	project, e2 := owneddata.Time(c.ProjectExpiry, false)
	policy, e3 := owneddata.Time(c.PolicyExpiry, false)
	manifest, e4 := owneddata.Time(c.Manifest.ExpiresAt, false)
	scope, e5 := owneddata.Time(c.ScopeExpiresAt, false)
	session, e6 := owneddata.Time(c.SessionDeadline, true)
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || e6 != nil || policy.After(session) || session.After(now.Add(executionSessionLifetime)) || scope.After(executionSessionExpiry(now, auth, project, policy, manifest)) {
		return p, scope, executionSessionProtocolError
	}
	p = ownedPolicy{MaxUploadBytes: 1024, ProjectEnd: project, AuthorizationExpiry: auth, PolicyExpiry: policy}
	return p, scope, nil
}

func executionSessionConfigRead(path string) (executionSessionConfig, error) {
	var c executionSessionConfig
	f, e := os.Open(path)
	if e != nil {
		return c, e
	}
	info, statErr := f.Stat()
	raw, readErr := io.ReadAll(io.LimitReader(f, 131073))
	closeErr := f.Close()
	if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || readErr != nil || closeErr != nil || len(raw) > 131072 || x.DecodeClosed(raw, &c, 131072) != nil {
		return c, executionSessionProtocolError
	}
	if len(c.StateRoot) > 1024 || len(c.DataRoot) > 1024 || !owneddata.ID(c.WaypointAuthorityID) || !owneddata.ID(c.ScopeID) || !owneddata.ID(c.OwnerEpoch) {
		return c, executionSessionProtocolError
	}
	var expected assessment.ManifestV2
	if x.DecodeClosed(executionSessionManifestSource, &expected, 65536) != nil {
		return c, executionSessionProtocolError
	}
	expected.ProjectID, expected.ExpiresAt = c.Manifest.ProjectID, c.Manifest.ExpiresAt
	if !reflect.DeepEqual(expected, c.Manifest) {
		return c, executionSessionProtocolError
	}
	encoded, e := json.Marshal(c.Manifest)
	if e != nil {
		return c, e
	}
	if _, e = assessment.DecodeManifestV2(encoded); e != nil {
		return c, e
	}
	now := time.Now().UTC()
	_, scope, e := c.grants(now)
	if e != nil || !scope.After(now) {
		return c, executionSessionProtocolError
	}
	return c, nil
}

type executionSessionOwner struct {
	broker            *Broker
	phaseProvider     *executionSessionPhaseProvider
	diagnosticObjects int
	config            executionSessionConfig
	binding           x.ScopeBinding
	files             [9]*os.File
	root, stateRoot   *os.Root
	transport         *executionSessionTransport
	context           context.Context
	cancel            context.CancelFunc
	watchResult       error
	watchReturned     chan struct{}
	cleanupOrder      *executionSessionCleanupOrder
	mu                sync.Mutex
	watchLaunched     bool
	setupLent         bool
	prepareFailed     bool
	prepared          bool
	primaryDone       bool
	finalStatus       bool
	safetyUsed        map[string]int
}

// The only diagnostic carrier is the existing private owner stderr. These two
// closed records contain no strings derived from paths, errors or phase output.
type executionSessionDiagnostic struct {
	Schema         string                            `json:"schema"`
	AttemptOrdinal int                               `json:"attempt_ordinal"`
	Phases         []executionSessionPhaseDiagnostic `json:"phases"`
}

const executionSessionDiagnosticSchema = "outfitter.private-session-diagnostic/v1"

func executionSessionDiagnosticClass(s string) bool {
	switch s {
	case "none", "canceled", "deadline", "wait_delay", "short_write", "exit", "os", "other":
		return true
	}
	return false
}

func executionSessionDiagnosticContext(s string) bool {
	return s == "none" || s == "canceled" || s == "deadline"
}

func (d executionSessionDiagnostic) valid() bool {
	if d.Schema != executionSessionDiagnosticSchema || d.AttemptOrdinal < 1 || d.AttemptOrdinal > 2 || d.Phases == nil || len(d.Phases) > 2 {
		return false
	}
	for i, p := range d.Phases {
		if p.Phase != []string{"workload", "checker"}[i] || !p.StartEntered || p.CmdStarted && !p.StartEntered || p.ExitKnown && !p.CmdStarted || p.ExitCode < -1 || p.ExitCode > 255 || !p.ExitKnown && p.ExitCode != 0 || p.ElapsedMS < 0 || p.ElapsedMS > 125000 || p.WaitMS < 0 || p.WaitMS > 125000 || p.RemainingBeforeMS < -125000 || p.RemainingBeforeMS > 125000 || p.RemainingAfterMS < -125000 || p.RemainingAfterMS > 125000 || !executionSessionDiagnosticContext(p.ContextBefore) || !executionSessionDiagnosticContext(p.ContextAfter) || !executionSessionDiagnosticClass(p.StartError) || !executionSessionDiagnosticClass(p.WaitError) || !executionSessionDiagnosticClass(p.PipeError) {
			return false
		}
	}
	return true
}

func executionSessionReadDiagnostics(input io.Reader, accept func(executionSessionDiagnostic)) error {
	// Limit the entire raw stream, not just individual parsed lines. Invalid
	// bytes are discarded and never passed to accept or a diagnostic formatter.
	limited := io.LimitReader(input, 65536)
	r := bufio.NewReaderSize(limited, 8193)
	total, count := 0, 0
	for {
		line, e := r.ReadSlice('\n')
		total += len(line)
		if e == io.EOF && len(line) == 0 && total < 65536 {
			return nil
		}
		var d executionSessionDiagnostic
		if e != nil || total >= 65536 || len(line) > 8192 || count >= 2 || len(line) < 2 || owneddata.ClosedJSON(line[:len(line)-1], &d, 8192) != nil || !d.valid() || d.AttemptOrdinal != count+1 {
			clear(line)
			return executionSessionProtocolError
		}
		count++
		accept(d)
	}
}

func (s *executionSessionOwner) emitPhaseDiagnostic(ctx context.Context, c x.Control) {
	if c.Slot == nil || s.phaseProvider == nil || s.diagnosticObjects >= 2 {
		return
	}
	ordinal := 0
	for i, slot := range s.config.Manifest.Reservations {
		if slot == *c.Slot {
			ordinal = i + 1
		}
	}
	d := executionSessionDiagnostic{executionSessionDiagnosticSchema, ordinal, s.phaseProvider.phaseDiagnostics(c.Slot.AttemptID)}
	if !d.valid() {
		return
	}
	raw, e := json.Marshal(d)
	if e != nil || len(raw)+1 > 8192 {
		return
	}
	s.diagnosticObjects++
	deadline, ok := ctx.Deadline()
	if !ok || s.files[2].SetWriteDeadline(executionSessionMinDeadline(deadline, s.transport.deadline)) != nil || ctx.Err() != nil || s.transport.context.Err() != nil {
		return
	}
	// This is outside all authority locks; failure never changes a public reply
	// or creates an ownership fact. Parent can report diagnostic_absent instead.
	_ = executionSessionWrite(s.files[2], append(raw, '\n'))
}

// These gates exist only in the explicit negative child fixture. Both existing
// hook sites execute outside Store/operation locks. No hook supplies close proof.
type executionSessionCleanupOrder struct {
	snapshotRelease, cleanupEntered, cleanupRelease chan struct{}
	snapshotOnce, enteredOnce, releaseOnce          sync.Once
}

func newExecutionSessionCleanupOrder() *executionSessionCleanupOrder {
	return &executionSessionCleanupOrder{snapshotRelease: make(chan struct{}), cleanupEntered: make(chan struct{}), cleanupRelease: make(chan struct{})}
}

func (g *executionSessionCleanupOrder) release() {
	g.snapshotOnce.Do(func() { close(g.snapshotRelease) })
	g.releaseOnce.Do(func() { close(g.cleanupRelease) })
}

type executionSessionCleanupStorage struct {
	assessmentStorage
	gate *executionSessionCleanupOrder
	ctx  context.Context
}

func (s executionSessionCleanupStorage) Remove(ctx context.Context, scope x.ExecutionScope) error {
	s.gate.enteredOnce.Do(func() { close(s.gate.cleanupEntered) })
	select {
	case <-s.ctx.Done():
		return x.ErrHeld
	case <-s.gate.cleanupRelease:
	}
	if s.ctx.Err() != nil {
		return x.ErrHeld
	}
	return s.assessmentStorage.Remove(ctx, scope)
}

func executionSessionAdoptFiles() ([9]*os.File, error) {
	var files [9]*os.File
	var firstErr error
	// No aliases for stdio; its Go wrappers must already be pollable at startup.
	files[0], files[1], files[2] = os.Stdin, os.Stdout, os.Stderr
	for fd := 3; fd < len(files); fd++ {
		if e := syscall.SetNonblock(fd, true); e != nil && firstErr == nil {
			firstErr = e
		}
		files[fd] = os.NewFile(uintptr(fd), "private-session-endpoint")
		if files[fd] == nil && firstErr == nil {
			firstErr = executionSessionProtocolError
		}
	}
	// Collect every known inherited endpoint before returning any construction
	// error. Each nonnil wrapper has one checked close path; no raw close retry.
	if firstErr != nil {
		return files, firstErr
	}
	for _, f := range files {
		if e := executionSessionPrivatePipe(f); e != nil {
			return files, e
		}
	}
	return files, nil
}

func executionSessionBindings(c executionSessionConfig) (x.CapabilityMapping, x.ScopeBinding) {
	w := x.CodeBinding{RecipeID: c.Manifest.Workload.RecipeID, CodeSHA256: c.Manifest.Workload.RecipeSHA256, EnvironmentID: c.Manifest.Workload.EnvironmentID, Image: c.Manifest.Workload.Image, InterpreterID: "python", InterpreterVersion: "3.12+fixture", InterpreterIdentitySHA256: strings.Repeat("e", 64)}
	checker := w
	checker.RecipeID, checker.CodeSHA256 = "independent-checker", strings.Repeat("f", 64)
	impl := x.ImplementationBinding{ID: "synthetic-fixture", Version: 1, SourceSHA256: strings.Repeat("9", 64)}
	capability := x.CapabilityMapping{Schema: x.CapabilitySchema, ID: "test-capability", Version: 1, ExecutionProfile: c.Manifest.ExecutionProfile, Architecture: c.Manifest.Hardware.Architecture, Workload: w, Checker: checker, WorkloadArgv: []string{"fixture", "workload"}, CheckerArgv: []string{"fixture", "checker"}, Provider: impl, Storage: impl, Collector: impl, Quality: impl, Limits: x.LimitsFor(c.Manifest), PhaseOrder: []string{"workload", "checker"}, MaxRunningResources: 1, DataClassifications: []string{"synthetic"}}
	binding := x.ScopeBinding{BrokerAuthorityID: strings.Repeat("5", 32), WaypointAuthorityID: c.WaypointAuthorityID, ScopeID: c.ScopeID, AssessmentID: c.Manifest.AssessmentID, ProjectID: c.Manifest.ProjectID, ManifestSchema: c.Manifest.Schema, ManifestSHA256: x.Digest(c.Manifest), ExecutionProfile: c.Manifest.ExecutionProfile, Capability: x.CapabilityRef{ID: capability.ID, Version: 1, SHA256: x.Digest(capability)}, Workload: w, Checker: checker}
	return capability, binding
}

func newExecutionSessionOwner(c executionSessionConfig, files [9]*os.File, cleanupOrder bool) (*executionSessionOwner, error) {
	deadline, e := owneddata.Time(c.SessionDeadline, true)
	if e != nil || !deadline.After(time.Now()) {
		return nil, executionSessionProtocolError
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	s := &executionSessionOwner{config: c, files: files, context: ctx, cancel: cancel, watchReturned: make(chan struct{}), safetyUsed: map[string]int{}}
	if cleanupOrder {
		s.cleanupOrder = newExecutionSessionCleanupOrder()
	}
	s.root, e = os.OpenRoot(c.DataRoot)
	if e != nil {
		return s, e
	}
	s.stateRoot, e = os.OpenRoot(c.StateRoot)
	if e != nil {
		return s, e
	}
	store, e := ledger.Open(c.StateRoot, false)
	if e != nil {
		return s, e
	}
	e = store.View(func(d ledger.Data) error {
		if d.Version != 7 || d.Authority == nil || d.Authority.ID != strings.Repeat("5", 32) || d.AssessmentExecution == nil || d.AssessmentExecution.Identity.WaypointAuthorityID != c.WaypointAuthorityID || len(d.AssessmentExecution.Scopes) != 0 || len(d.AssessmentExecution.Attempts) != 0 || len(d.Runs) != 0 || len(d.NativeRuns) != 0 || d.Projects[c.Manifest.ProjectID].Budget != 60 {
			return executionSessionProtocolError
		}
		return nil
	})
	if e != nil {
		return s, e
	}
	identity := func(f *os.File) (x.FilesystemIdentity, error) {
		st, e := f.Stat()
		if e != nil {
			return x.FilesystemIdentity{}, e
		}
		return ledger.FileIdentity(st)
	}
	f, e := s.root.Open(".")
	if e != nil {
		return s, e
	}
	rootID, identityErr := identity(f)
	closeErr := f.Close()
	if identityErr != nil || closeErr != nil {
		return s, executionSessionProtocolError
	}
	start := time.Now()
	o := &ownedBoundary{root: s.root, stateRoot: s.stateRoot, id: strings.Repeat("6", 32), rootIdentity: rootID, ownerRead: files[3], watchRead: files[4], watchWrite: files[5]}
	o.clock = func() (ownedClock, error) {
		return ownedClock{Wall: time.Now().UTC(), TickMS: time.Since(start).Milliseconds(), Epoch: strings.Repeat("9", 32)}, nil
	}
	o.policy = func(project, epoch string) (ownedPolicy, error) {
		if project != c.Manifest.ProjectID || epoch != c.OwnerEpoch {
			return ownedPolicy{}, executionSessionProtocolError
		}
		p, _, e := c.grants(time.Now().UTC())
		return p, e
	}
	capability, binding := executionSessionBindings(c)
	s.binding = binding
	p, e := newExecutionSessionPhaseProvider(files)
	if e != nil {
		return s, e
	}
	s.phaseProvider = p
	b := New(store, nil)
	a := &assessmentBoundary{owned: o, capability: capability, provider: p, storage: executionTestStorage{}, collector: func(context.Context, x.ExecutionScope) (assessmentCollector, error) {
		return &executionTestCollector{}, nil
	}, transport: func(context.Context, x.ExecutionScope, string) (io.Closer, error) {
		r, w, e := os.Pipe()
		if e != nil {
			return nil, e
		}
		return &executionTestTransport{r, w}, nil
	}, checkQuality: func(raw []byte) (string, error) {
		if !bytes.Equal(raw, []byte(`{"quality":"passed"}`)) {
			return "unknown", x.ErrInvalid
		}
		return "passed", nil
	}}
	b.assessment = a
	s.broker = b
	a.spawnWatch = func(owner *ledger.AssessmentOperation) error {
		childOwned, childBoundary := *o, *a
		childBoundary.owned, childBoundary.watchOwner = &childOwned, owner
		if g := s.cleanupOrder; g != nil {
			prior := childOwned.fault
			childOwned.fault = func(point string) error {
				if point == "watch-before-snapshot" {
					select {
					case <-s.transport.context.Done():
						return x.ErrHeld
					case <-g.snapshotRelease:
					}
					if s.transport.context.Err() != nil {
						return x.ErrHeld
					}
				}
				if prior != nil {
					return prior(point)
				}
				return nil
			}
			childBoundary.storage = executionSessionCleanupStorage{assessmentStorage: childBoundary.storage, gate: g, ctx: s.transport.context}
		}
		child := New(store, nil)
		child.assessment = &childBoundary
		e := owner.LaunchWatch(func(*ledger.AssessmentOperation) {
			result := child.AssessmentWatch(ctx, s.binding)
			s.mu.Lock()
			s.watchResult = result
			s.mu.Unlock()
			close(s.watchReturned)
		})
		if e == nil {
			s.mu.Lock()
			s.watchLaunched = true
			s.mu.Unlock()
		}
		return e
	}
	s.transport, e = newExecutionSessionTransport(ctx, files[0], files[1], files[7], files[8], files[6], deadline)
	if e != nil {
		return s, e
	}
	s.transport.validate, s.transport.dispatch, s.transport.finish = s.validate, s.dispatch, s.finished
	s.transport.afterReply = func(c x.Control, e error) error {
		if c.Operation == "prepare" && e != nil {
			return executionSessionProtocolError
		}
		return nil
	}
	return s, nil
}

func (s *executionSessionOwner) validate(lane byte, sequence uint64, c x.Control) error {
	if c.Binding != s.binding {
		return executionSessionProtocolError
	}
	if lane == 'P' {
		steps := []string{"prepare", "ingest", "seal", "execute", "execute"}
		if sequence < 1 || sequence > 5 || c.Operation != steps[sequence-1] {
			return executionSessionProtocolError
		}
		if c.Operation == "prepare" && (c.Manifest == nil || !reflect.DeepEqual(*c.Manifest, s.config.Manifest) || c.ExpiresAt == nil || *c.ExpiresAt != s.config.ScopeExpiresAt || c.AdmittingOwnerEpoch == nil || *c.AdmittingOwnerEpoch != s.config.OwnerEpoch || c.DeclaredUploadCeilingBytes == nil || *c.DeclaredUploadCeilingBytes != 1024) {
			return executionSessionProtocolError
		}
		if c.Operation == "ingest" && (c.ContentLength == nil || *c.ContentLength != int64(len(executionSessionInputSource))) {
			return executionSessionProtocolError
		}
		if c.Operation == "execute" && (c.Slot == nil || *c.Slot != s.config.Manifest.Reservations[sequence-4]) {
			return executionSessionProtocolError
		}
		return nil
	}
	if lane != 'S' || c.Operation != "cancel" && c.Operation != "reconcile" && c.Operation != "status" {
		return executionSessionProtocolError
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.prepared {
		return executionSessionProtocolError
	}
	key := c.Operation + ":scope"
	maximum := 1
	if c.Operation == "reconcile" {
		maximum = 2
	}
	if c.Slot != nil {
		if c.Operation == "status" || *c.Slot != s.config.Manifest.Reservations[0] && *c.Slot != s.config.Manifest.Reservations[1] {
			return executionSessionProtocolError
		}
		key = c.Operation + ":" + c.Slot.AttemptID
	}
	s.safetyUsed[key]++
	if s.safetyUsed[key] > maximum {
		return executionSessionProtocolError
	}
	return nil
}

func (s *executionSessionOwner) dispatch(ctx context.Context, c x.Control) (any, error) {
	if c.Operation == "reconcile" && c.Slot == nil {
		if e := s.beforeScopeReconcile(ctx); e != nil {
			return nil, e
		}
	}
	if c.Operation == "execute" {
		r, e := s.broker.RunAssessmentOnce(ctx, c)
		s.emitPhaseDiagnostic(ctx, c)
		s.mu.Lock()
		if c.Slot != nil && *c.Slot == s.config.Manifest.Reservations[1] {
			s.primaryDone = true
		}
		s.mu.Unlock()
		return r, e
	}
	var body io.Reader
	if c.Operation == "ingest" {
		deadline, _ := ctx.Deadline()
		if e := s.transport.readDeadline(s.files[6], deadline); e != nil {
			return nil, e
		}
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		body = s.files[6]
	}
	if c.Operation == "prepare" {
		s.mu.Lock()
		s.setupLent = true
		s.mu.Unlock()
	}
	r, e := s.broker.AssessmentData(ctx, c, body)
	s.mu.Lock()
	if c.Operation == "prepare" {
		s.prepared, s.prepareFailed = e == nil, e != nil
	}
	if c.Operation == "status" && c.Slot == nil && e == nil {
		s.finalStatus = true
	}
	s.mu.Unlock()
	return r, e
}

func (s *executionSessionOwner) beforeScopeReconcile(ctx context.Context) error {
	if ctx.Err() != nil || s.context.Err() != nil {
		return x.ErrHeld
	}
	scope, _, e := s.broker.assessmentRead(s.binding)
	if e != nil || ctx.Err() != nil || s.context.Err() != nil {
		return x.ErrHeld
	}
	if !scope.DenyLatched {
		// A healthy reconcile must not wait for its intentionally live watcher.
		return nil
	}
	if g := s.cleanupOrder; g != nil {
		// validate already spent this distinct opportunity; no poll/retry can
		// bypass the fixed count. Never hold s.mu while joining either owner.
		s.mu.Lock()
		first := s.safetyUsed["reconcile:scope"] == 1
		s.mu.Unlock()
		if first {
			g.snapshotOnce.Do(func() { close(g.snapshotRelease) })
			select {
			case <-ctx.Done():
				return x.ErrHeld
			case <-s.context.Done():
				return x.ErrHeld
			case <-g.cleanupEntered:
				// Intentionally call the unchanged Broker while the exact
				// original cleanup is held. HELD must remain a real error.
				if ctx.Err() != nil || s.context.Err() != nil {
					return x.ErrHeld
				}
				return nil
			}
		}
		g.release()
	}
	s.mu.Lock()
	launched := s.watchLaunched
	s.mu.Unlock()
	if !launched {
		return x.ErrHeld
	}
	select {
	case <-ctx.Done():
		return x.ErrHeld
	case <-s.context.Done():
		return x.ErrHeld
	case <-s.watchReturned:
	}
	if ctx.Err() != nil || s.context.Err() != nil {
		return x.ErrHeld
	}
	return nil
}

func (s *executionSessionOwner) finished(lane byte) bool {
	s.mu.Lock()
	ready := s.primaryDone
	if lane == 'S' {
		ready = s.finalStatus
	}
	s.mu.Unlock()
	if lane == 'P' && ready {
		return true
	}
	if lane == 'S' && !ready {
		return false
	}
	// A known cancellation can end the remaining primary plan only after the
	// original owners have actually drained. This is transport termination, not
	// a replacement receipt or new cleanup/admission authority.
	return s.checkedDrained()
}

func (s *executionSessionOwner) checkedDrained() bool {
	if s.broker == nil {
		return false
	}
	e := s.broker.Store.View(func(d ledger.Data) error {
		if d.AssessmentExecution == nil {
			return x.ErrHeld
		}
		scope, ok := d.AssessmentExecution.Scopes[s.binding.ScopeID]
		if !ok || scope.Binding() != s.binding || !scope.DenyLatched || !scope.CleanupOwnershipCloseConfirmed || !scope.TransientOwnershipClosed(true) || scope.ContentCloseUncertain || scope.WriterStarted && !scope.ContentCloseConfirmed {
			return x.ErrHeld
		}
		for _, a := range d.AssessmentExecution.Attempts {
			if a.Binding.ScopeID == scope.ScopeID && !a.SerialClear() {
				return x.ErrHeld
			}
		}
		return nil
	})
	return e == nil
}

func (s *executionSessionOwner) close() error {
	if s == nil {
		return nil
	}
	if s.transport != nil {
		s.transport.stopTimer()
	}
	s.cancel()
	if s.cleanupOrder != nil {
		s.cleanupOrder.release()
	}
	s.mu.Lock()
	watch, lent, failed := s.watchLaunched, s.setupLent, s.prepareFailed
	s.mu.Unlock()
	var result error
	if failed {
		result = executionSessionProtocolError
	}
	borrowedDrained := true
	if watch {
		select {
		case <-s.watchReturned:
			s.mu.Lock()
			e := s.watchResult
			s.mu.Unlock()
			// Expected denial also returns HELD; only the same Store's actual
			// checked original-owner state can distinguish it from uncertainty.
			if !s.checkedDrained() || e != nil && e != x.ErrHeld {
				result = executionSessionProtocolError
			}
		case <-time.After(2 * time.Second):
			// Do not close borrowed roots beneath an undrained original owner.
			borrowedDrained = false
			result = executionSessionProtocolError
		}
	}
	for i, f := range s.files {
		if f == nil || i == 5 && lent || !borrowedDrained && (i == 3 || i == 4) {
			continue
		}
		if e := f.Close(); e != nil {
			result = executionSessionProtocolError
		}
	}
	if borrowedDrained && s.root != nil {
		if e := s.root.Close(); e != nil {
			result = executionSessionProtocolError
		}
	}
	if borrowedDrained && s.stateRoot != nil {
		if e := s.stateRoot.Close(); e != nil {
			result = executionSessionProtocolError
		}
	}
	// Lent FD5 is never retried: the original prepare owns its close path.
	// On failed/ambiguous handoff its remaining liability is explicit, not cleared
	// by process exit. A successful actual Broker call has its original contract.
	return result
}

func executionSessionHelperEntry() int {
	files, e := executionSessionAdoptFiles()
	if e != nil {
		return executionSessionExitCode(e, executionSessionCloseFiles(files))
	}
	config, e := executionSessionConfigRead(os.Getenv("OUTFITTER_PRIVATE_EXECUTION_SESSION_CONFIG"))
	if e != nil {
		return executionSessionExitCode(e, executionSessionCloseFiles(files))
	}
	mode := os.Getenv("OUTFITTER_PRIVATE_EXECUTION_SESSION_CAUSAL")
	if mode != "" && mode != "cleanup-order" {
		return executionSessionExitCode(executionSessionProtocolError, executionSessionCloseFiles(files))
	}
	s, e := newExecutionSessionOwner(config, files, mode == "cleanup-order")
	if e != nil {
		if s != nil {
			return executionSessionExitCode(e, s.close())
		}
		return executionSessionExitCode(e, executionSessionCloseFiles(files))
	}
	runErr := s.transport.run()
	closeErr := s.close()
	return executionSessionExitCode(runErr, closeErr)
}

func executionSessionCloseFiles(files [9]*os.File) error {
	var result error
	for _, f := range files {
		if f != nil {
			if e := f.Close(); e != nil {
				result = executionSessionProtocolError
			}
		}
	}
	return result
}

func executionSessionExitCode(operation, cleanup error) int {
	if operation != nil || cleanup != nil {
		return 2
	}
	return 0
}

func TestExecutionPrivateSessionOwnerHelper(t *testing.T) {
	if os.Getenv("OUTFITTER_PRIVATE_EXECUTION_SESSION") != "1" {
		return
	}
	// Entry returns only AFTER all permitted checked teardown attempts. There
	// are no deferred test cleanups skipped by this exit and no harness footer.
	code := executionSessionHelperEntry()
	os.Exit(code)
}
