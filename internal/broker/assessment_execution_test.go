package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/assessment"
	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

// Positive construction is deliberately confined to _test.go. These are
// synthetic provider/collector/storage observations, not engine acceptance.
type executionTestProvider struct {
	mu                        sync.Mutex
	requests                  []assessmentPhaseRequest
	resources                 map[string]assessmentResourceObservation
	active, maxActive, starts int
}

func (p *executionTestProvider) Create(ctx context.Context, r assessmentPhaseRequest) (x.DockerResourceIdentity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ctx.Err() != nil {
		return x.DockerResourceIdentity{}, ctx.Err()
	}
	p.requests = append(p.requests, r)
	id := x.DockerResourceIdentity{ContainerID: fmt.Sprintf("%064x", len(p.requests)), BrokerOwner: r.Binding.BrokerAuthorityID, AttemptID: r.Slot.AttemptID, Phase: r.Phase, ScopeID: r.Binding.ScopeID, CapabilitySHA256: r.Binding.Capability.SHA256}
	p.resources[r.OwnerName] = assessmentResourceObservation{Identity: id, Exists: true, Stopped: true}
	return id, nil
}
func (p *executionTestProvider) Start(ctx context.Context, id x.DockerResourceIdentity, in io.Reader, prior []byte, out, stderr io.Writer) (assessmentCompletion, error) {
	p.mu.Lock()
	p.active++
	p.starts++
	if p.active > p.maxActive {
		p.maxActive = p.active
	}
	p.mu.Unlock()
	defer func() { p.mu.Lock(); p.active--; p.mu.Unlock() }()
	start := time.Now().UTC()
	raw, e := io.ReadAll(io.LimitReader(in, 1025))
	if e != nil || len(raw) > 1024 {
		return assessmentCompletion{}, x.ErrDenied
	}
	if !bytes.Contains(raw, []byte("PRIVATE_INPUT_POISON")) {
		return assessmentCompletion{}, x.ErrInvalid
	}
	if _, e = stderr.Write([]byte("PRIVATE_STDERR_POISON")); e != nil {
		return assessmentCompletion{}, e
	}
	if id.Phase == "workload" {
		_, e = out.Write([]byte("PRIVATE_OUTPUT_POISON"))
	} else {
		if string(prior) != "PRIVATE_OUTPUT_POISON" {
			return assessmentCompletion{}, x.ErrInvalid
		}
		_, e = out.Write([]byte(`{"quality":"passed"}`))
	}
	finish := time.Now().UTC()
	return assessmentCompletion{ExitCode: 0, StartedAt: &start, FinishedAt: &finish}, e
}
func (p *executionTestProvider) Inspect(ctx context.Context, r assessmentPhaseRequest) (assessmentResourceObservation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ctx.Err() != nil {
		return assessmentResourceObservation{}, ctx.Err()
	}
	return p.resources[r.OwnerName], nil
}
func (p *executionTestProvider) Stop(ctx context.Context, id x.DockerResourceIdentity) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, v := range p.resources {
		if v.Identity == id {
			v.Stopped = true
			p.resources[k] = v
			return nil
		}
	}
	return x.ErrHeld
}
func (p *executionTestProvider) Remove(ctx context.Context, id x.DockerResourceIdentity) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, v := range p.resources {
		if v.Identity == id && v.Stopped {
			v.Exists = false
			p.resources[k] = v
			return nil
		}
	}
	return x.ErrHeld
}

type executionTestStorage struct{}

func (executionTestStorage) Admit(context.Context, x.ExecutionScope) error               { return nil }
func (executionTestStorage) BeforePhase(context.Context, x.ExecutionScope, string) error { return nil }
func (executionTestStorage) AfterPhase(context.Context, x.ExecutionScope, string) error  { return nil }
func (executionTestStorage) Remove(context.Context, x.ExecutionScope) error              { return nil }

type executionTestCollector struct{}

func (*executionTestCollector) Observe(context.Context, x.DockerResourceIdentity) (x.MetricSet, error) {
	return x.UnknownMetrics(), nil
}
func (*executionTestCollector) Finish(context.Context) (x.MetricSet, error) {
	return x.UnknownMetrics(), nil
}
func (*executionTestCollector) Close() error { return nil }

type executionTestTransport struct{ r, w *os.File }

func (t *executionTestTransport) Close() error {
	a := t.r.Close()
	b := t.w.Close()
	if a != nil {
		return a
	}
	return b
}

type executionFixture struct {
	b          *Broker
	p          *executionTestProvider
	c          x.Control
	ownerWrite *os.File
	watchDone  chan error
}

func newExecutionFixture(t *testing.T) *executionFixture {
	t.Helper()
	base := t.TempDir()
	state := filepath.Join(base, "state")
	data := filepath.Join(base, "data")
	for _, p := range []string{state, data} {
		if e := os.Mkdir(p, 0700); e != nil {
			t.Fatal(e)
		}
	}
	state, _ = filepath.EvalSymlinks(state)
	data, _ = filepath.EvalSymlinks(data)
	idAt := func(path string) x.FilesystemIdentity {
		st, e := os.Stat(path)
		if e != nil {
			t.Fatal(e)
		}
		id, e := ledger.FileIdentity(st)
		if e != nil {
			t.Fatal(e)
		}
		return id
	}
	for _, name := range []string{"initialized", "ledger.lock", "execution.lock"} {
		if e := os.WriteFile(filepath.Join(state, name), nil, 0600); e != nil {
			t.Fatal(e)
		}
	}
	i := x.AuthorityIdentity{WaypointAuthorityID: strings.Repeat("4", 32), Directory: idAt(state), JournalLease: idAt(filepath.Join(state, "ledger.lock")), ExecutionLease: idAt(filepath.Join(state, "execution.lock"))}
	project := strings.Repeat("2", 32)
	d := ledger.Data{Version: 7, Owner: strings.Repeat("f", 32), Authority: &ledger.Authority{ID: strings.Repeat("5", 32), StatePath: state, Admission: "active"}, Projects: map[string]ledger.Project{project: {ID: project, Budget: 60}}, Runs: map[string]provider.Record{}, AssessmentExecution: &x.Authority{Schema: x.AuthoritySchema, Identity: i, Scopes: map[string]x.ExecutionScope{}, Attempts: map[string]x.AssessmentAttempt{}}}
	raw, _ := json.Marshal(d)
	if e := os.WriteFile(filepath.Join(state, "ledger.json"), raw, 0600); e != nil {
		t.Fatal(e)
	}
	store, e := ledger.Open(state, false)
	if e != nil {
		t.Fatal(e)
	}
	raw, e = os.ReadFile("../assessment/testdata/v2/benchmark-manifest.canonical.json")
	if e != nil {
		t.Fatal(e)
	}
	m, e := assessment.DecodeManifestV2(raw)
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now().UTC().Truncate(time.Second)
	m.AssessmentID = strings.Repeat("1", 32)
	m.ProjectID = project
	m.ExpiresAt = now.Add(time.Hour).Format(owneddata.SecondUTC)
	m.Design.FittingSizes = []int64{1}
	m.Design.HeldOutSize = 2
	m.Design.Repetitions = 1
	m.Design.Seeds = []int64{11}
	m.Design.SmallerDesign = &assessment.SmallerDesign{ApprovedByUser: true, Reason: "operator_selected"}
	m.Ceilings = assessment.CeilingsV2{MaxAttempts: 2, CPUSeconds: 60, CPUCountPerAttempt: 1, MemoryBytes: 256 << 20, InputBytes: 1024, InputRows: 2, ScratchBytes: 128 << 10, OutputBytes: 65536, TimeoutSecondsPerAttempt: 30}
	m.Reservations = []assessment.ReservationBinding{{AttemptID: strings.Repeat("a", 32), StageID: "analysis", SampleRole: "fitting", SampleSize: 1, Repetition: 1, ReservationID: strings.Repeat("b", 32)}, {AttemptID: strings.Repeat("c", 32), StageID: "analysis", SampleRole: "held_out", SampleSize: 2, Repetition: 1, ReservationID: strings.Repeat("d", 32)}}
	w := x.CodeBinding{RecipeID: m.Workload.RecipeID, CodeSHA256: m.Workload.RecipeSHA256, EnvironmentID: m.Workload.EnvironmentID, Image: m.Workload.Image, InterpreterID: "python", InterpreterVersion: "3.12+fixture", InterpreterIdentitySHA256: strings.Repeat("e", 64)}
	checker := w
	checker.RecipeID = "independent-checker"
	checker.CodeSHA256 = strings.Repeat("f", 64)
	impl := x.ImplementationBinding{ID: "synthetic-fixture", Version: 1, SourceSHA256: strings.Repeat("9", 64)}
	capability := x.CapabilityMapping{Schema: x.CapabilitySchema, ID: "test-capability", Version: 1, ExecutionProfile: m.ExecutionProfile, Architecture: m.Hardware.Architecture, Workload: w, Checker: checker, WorkloadArgv: []string{"fixture", "workload"}, CheckerArgv: []string{"fixture", "checker"}, Provider: impl, Storage: impl, Collector: impl, Quality: impl, Limits: x.LimitsFor(m), PhaseOrder: []string{"workload", "checker"}, MaxRunningResources: 1, DataClassifications: []string{"synthetic"}}
	root, e := os.OpenRoot(data)
	if e != nil {
		t.Fatal(e)
	}
	stateRoot, e := os.OpenRoot(state)
	if e != nil {
		t.Fatal(e)
	}
	ownerRead, ownerWrite, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	watchRead, watchWrite, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	start := time.Now()
	o := &ownedBoundary{root: root, stateRoot: stateRoot, id: strings.Repeat("6", 32), rootIdentity: idAt(data), ownerRead: ownerRead, watchRead: watchRead, watchWrite: watchWrite, clock: func() (ownedClock, error) {
		return ownedClock{Wall: time.Now().UTC(), TickMS: time.Since(start).Milliseconds(), Epoch: strings.Repeat("9", 32)}, nil
	}, policy: func(string, string) (ownedPolicy, error) {
		expiry := now.Add(time.Hour)
		return ownedPolicy{MaxUploadBytes: 1024, ProjectEnd: expiry, AuthorizationExpiry: expiry, PolicyExpiry: expiry}, nil
	}}
	p := &executionTestProvider{resources: map[string]assessmentResourceObservation{}}
	b := New(store, nil)
	a := &assessmentBoundary{owned: o, capability: capability, provider: p, storage: executionTestStorage{}, collector: func(context.Context, x.ExecutionScope) (assessmentCollector, error) {
		return &executionTestCollector{}, nil
	}, transport: func(context.Context, x.ExecutionScope, string) (io.Closer, error) {
		r, w, e := os.Pipe()
		if e != nil {
			return nil, e
		}
		return &executionTestTransport{r, w}, nil
	}, checkQuality: func(v []byte) (string, error) {
		if string(v) != `{"quality":"passed"}` {
			return "unknown", x.ErrInvalid
		}
		return "passed", nil
	}}
	f := &executionFixture{b: b, p: p, ownerWrite: ownerWrite, watchDone: make(chan error, 1)}
	b.assessment = a
	watchCtx, stop := context.WithCancel(context.Background())
	a.spawnWatch = func(watchOwner *ledger.AssessmentOperation) error {
		childOwned := *o
		childBoundary := *a
		childBoundary.owned = &childOwned
		childBoundary.watchOwner = watchOwner
		child := New(store, nil)
		child.assessment = &childBoundary
		return watchOwner.LaunchWatch(func(_ *ledger.AssessmentOperation) { f.watchDone <- child.AssessmentWatch(watchCtx, f.c.Binding) })
	}
	ownerEpoch := strings.Repeat("8", 32)
	ceiling := int64(1024)
	expires := now.Add(30 * time.Minute).Format(owneddata.SecondUTC)
	f.c = x.Control{Schema: x.ControlSchema, Operation: "prepare", Binding: x.ScopeBinding{BrokerAuthorityID: d.Authority.ID, WaypointAuthorityID: i.WaypointAuthorityID, ScopeID: strings.Repeat("3", 32), AssessmentID: m.AssessmentID, ProjectID: m.ProjectID, ManifestSchema: m.Schema, ManifestSHA256: x.Digest(m), ExecutionProfile: m.ExecutionProfile, Capability: x.CapabilityRef{ID: capability.ID, Version: capability.Version, SHA256: x.Digest(capability)}, Workload: w, Checker: checker}, AdmittingOwnerEpoch: &ownerEpoch, Manifest: &m, DeclaredUploadCeilingBytes: &ceiling, ExpiresAt: &expires}
	t.Cleanup(func() {
		stop()
		_ = ownerWrite.Close()
		select {
		case <-f.watchDone:
		case <-time.After(time.Second):
		}
		_ = ownerRead.Close()
		_ = watchRead.Close()
		_ = watchWrite.Close()
		_ = root.Close()
		_ = stateRoot.Close()
	})
	if e := f.c.Validate(); e != nil {
		t.Fatal("synthetic test control invalid", e)
	}
	return f
}
func (f *executionFixture) seal(t *testing.T) x.Status {
	t.Helper()
	s, e := f.b.AssessmentData(context.Background(), f.c, nil)
	if e != nil {
		t.Fatal("prepare", e)
	}
	body := []byte("PRIVATE_INPUT_POISON\n")
	length := int64(len(body))
	gen := s.Generation
	epoch := s.WriterEpoch
	c := x.Control{Schema: x.ControlSchema, Operation: "ingest", Binding: f.c.Binding, ExpectedGeneration: &gen, AdmittingOwnerEpoch: f.c.AdmittingOwnerEpoch, WriterEpoch: &epoch, ContentLength: &length}
	s, e = f.b.AssessmentData(context.Background(), c, bytes.NewReader(body))
	if e != nil {
		t.Fatal("ingest", e)
	}
	gen = s.Generation
	c.Operation = "seal"
	c.ContentLength = nil
	s, e = f.b.AssessmentData(context.Background(), c, nil)
	if e != nil {
		current, _, readErr := f.b.assessmentRead(f.c.Binding)
		t.Fatalf("seal %v read=%v state=%s reason=%s generation=%d expected=%d close=%v complete=%v", e, readErr, current.State, current.Reason, current.Generation, *c.ExpectedGeneration, current.ContentCloseConfirmed, current.UploadComplete)
	}
	return s
}
func TestExecutionConnectedTwoPhaseOnceAndNonretention(t *testing.T) {
	f := newExecutionFixture(t)
	s := f.seal(t)
	generation := s.Generation
	nonce := strings.Repeat("e", 32)
	slot := f.c.Manifest.Reservations[0]
	c := x.Control{Schema: x.ControlSchema, Operation: "execute", Binding: f.c.Binding, ExpectedGeneration: &generation, AdmittingOwnerEpoch: f.c.AdmittingOwnerEpoch, Slot: &slot, HandoffNonce: &nonce}
	r, e := f.b.RunAssessmentOnce(context.Background(), c)
	if e != nil {
		t.Fatal("execute", e, r)
	}
	if r.Code != "OK" || r.Attempt.Outcome != "succeeded" || r.Compute.ReservedCPUSeconds != 30 || r.Compute.ChargedCPUSeconds != 30 || r.AccountingUnresolved || r.Trust != x.Untrusted() {
		t.Fatalf("wrong final result: %+v", r)
	}
	if f.p.starts != 2 || f.p.maxActive != 1 || len(f.p.requests) != 2 || f.p.requests[0].Selection.Method != "synthetic_fixture" || f.p.requests[0].Selection.Seed != 11 || f.p.requests[0].Selection.Slot != slot || f.p.requests[1].Selection != f.p.requests[0].Selection {
		t.Fatal("phase/selection binding", f.p.requests)
	}
	if _, e = f.b.RunAssessmentOnce(context.Background(), c); e != nil {
		t.Fatal("lookup", e)
	}
	if f.p.starts != 2 {
		t.Fatal("replayed permanent slot")
	}
	u, e := f.b.Store.Usage(c.Binding.ProjectID)
	if e != nil || u.Budget != 60 || u.Charged != 30 || u.Reserved != 0 || u.Unknown != 0 || u.Remaining != 30 {
		t.Fatal(u, e)
	}
	if e = filepath.WalkDir(f.b.Store.Dir, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			if d.Name() == "outputs" || d.Name() == "snapshots" {
				t.Error("retained diagnostic directory")
			}
			return nil
		}
		raw, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		for _, poison := range []string{"PRIVATE_INPUT_POISON", "PRIVATE_OUTPUT_POISON", "PRIVATE_STDERR_POISON"} {
			if bytes.Contains(raw, []byte(poison)) {
				t.Error("retained content", path)
			}
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
}
