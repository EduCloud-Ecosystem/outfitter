package assessmentexec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/EduCloud-Ecosystem/outfitter/internal/assessment"
)

func executionFixture(t *testing.T) ExecutionScope {
	t.Helper()
	raw, e := os.ReadFile("../assessment/testdata/v2/benchmark-manifest.canonical.json")
	if e != nil {
		t.Fatal(e)
	}
	m, e := assessment.DecodeManifestV2(raw)
	if e != nil {
		t.Fatal(e)
	}
	m.AssessmentID = strings.Repeat("1", 32)
	m.ProjectID = strings.Repeat("2", 32)
	m.Reservations = []Slot{}
	for i, n := range append(append([]int64{}, m.Design.FittingSizes...), m.Design.HeldOutSize) {
		role := "fitting"
		if i == len(m.Design.FittingSizes) {
			role = "held_out"
		}
		for r := int64(1); r <= m.Design.Repetitions; r++ {
			k := len(m.Reservations)
			m.Reservations = append(m.Reservations, Slot{AttemptID: fmt.Sprintf("%032x", 100+k), StageID: "analysis", SampleRole: role, SampleSize: n, Repetition: r, ReservationID: fmt.Sprintf("%032x", 200+k)})
		}
	}
	w := CodeBinding{RecipeID: m.Workload.RecipeID, CodeSHA256: m.Workload.RecipeSHA256, EnvironmentID: m.Workload.EnvironmentID, Image: m.Workload.Image, InterpreterID: "python", InterpreterVersion: "3.12+review", InterpreterIdentitySHA256: strings.Repeat("c", 64)}
	checker := w
	checker.RecipeID = "independent-checker"
	checker.CodeSHA256 = strings.Repeat("d", 64)
	impl := ImplementationBinding{ID: "synthetic-test", Version: 1, SourceSHA256: strings.Repeat("e", 64)}
	c := CapabilityMapping{Schema: CapabilitySchema, ID: "test-capability", Version: 1, ExecutionProfile: m.ExecutionProfile, Architecture: m.Hardware.Architecture, Workload: w, Checker: checker, WorkloadArgv: []string{"python", "/reviewed/recipe.py"}, CheckerArgv: []string{"python", "/reviewed/checker.py"}, Provider: impl, Storage: impl, Collector: impl, Quality: impl, Limits: LimitsFor(m), PhaseOrder: []string{"workload", "checker"}, MaxRunningResources: 1, DataClassifications: []string{"synthetic"}}
	f := FilesystemIdentity{Device: "1", Inode: "1"}
	return ExecutionScope{Schema: ScopeSchema, ScopeID: strings.Repeat("3", 32), AssessmentID: m.AssessmentID, ProjectID: m.ProjectID, WaypointAuthorityID: strings.Repeat("4", 32), BrokerAuthorityID: strings.Repeat("5", 32), ManifestSHA256: Digest(m), ManifestExpiresAt: m.ExpiresAt, BoundaryID: strings.Repeat("6", 32), CreatedAt: "2026-10-20T00:00:00.000000Z", ExpiresAt: "2026-10-20T02:00:00Z", WallHighWater: "2026-10-20T00:00:00.000000Z", Generation: 1, State: "preparing", Reason: "none", WriterEpoch: strings.Repeat("7", 32), Assurance: "synthetic", Observations: []ScopeObservation{}, DeclaredUploadCeilingBytes: m.Ceilings.InputBytes, AdmittingOwnerEpoch: strings.Repeat("8", 32), ClockEpoch: strings.Repeat("9", 32), DeadlineTickMS: 7200000, TickHighWaterMS: 0, FilesystemIdentity: ExecutionFilesystemBinding{Root: f, OwnerRead: f, WatchRead: f, WatchWrite: f}, Manifest: m, ManifestSchema: m.Schema, ExecutionProfile: m.ExecutionProfile, Capability: c, CapabilitySHA256: Digest(c), Limits: c.Limits, Slots: Clone(m.Reservations)}
}

func TestExecutionClosedControlAndTokens(t *testing.T) {
	s := executionFixture(t)
	if e := s.Validate(); e != nil {
		t.Fatal(e)
	}
	c := Control{Schema: ControlSchema, Operation: "prepare", Binding: s.Binding(), AdmittingOwnerEpoch: &s.AdmittingOwnerEpoch, Manifest: &s.Manifest, DeclaredUploadCeilingBytes: &s.DeclaredUploadCeilingBytes, ExpiresAt: &s.ExpiresAt}
	raw, _ := json.Marshal(c)
	if _, e := DecodeControl(raw, "prepare"); e != nil {
		t.Fatal(e)
	}
	for name, mutate := range map[string]func([]byte) []byte{
		"duplicate": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"operation":"prepare"`), []byte(`"operation":"prepare","operation":"prepare"`), 1)
		},
		"nested-duplicate": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
		},
		"negative-zero": func(b []byte) []byte { return bytes.Replace(b, []byte(`"gpu_count":0`), []byte(`"gpu_count":-0`), 1) },
		"exponent":      func(b []byte) []byte { return bytes.Replace(b, []byte(`"version":1`), []byte(`"version":1e0`), 1) },
		"missing-null":  func(b []byte) []byte { return bytes.Replace(b, []byte(`,"reason":null`), nil, 1) },
		"unknown":       func(b []byte) []byte { return append([]byte(`{"enabler":true,`), b[1:]...) },
	} {
		t.Run(name, func(t *testing.T) {
			b := mutate(raw)
			if bytes.Equal(b, raw) {
				t.Fatal("mutation missed target")
			}
			if _, e := DecodeControl(b, "prepare"); e == nil {
				t.Fatal("accepted malformed control")
			}
		})
	}
	if _, e := DecodeControl(raw, "execute"); e == nil {
		t.Fatal("operation selected by body")
	}
	for _, bad := range []string{"3:12", strings.Repeat("a", 65), ""} {
		v := s.Capability.Workload
		v.InterpreterVersion = bad
		if v.Validate() == nil {
			t.Fatal("bad version", bad)
		}
	}
	for _, bad := range []string{"python.3", "python:3", strings.Repeat("a", 81)} {
		v := s.Capability.Workload
		v.InterpreterID = bad
		if v.Validate() == nil {
			t.Fatal("bad code ID")
		}
	}
	for _, bad := range []string{"profile.name", "profile:name", strings.Repeat("a", 65)} {
		v := s.Capability
		v.ID = bad
		if v.Validate() == nil {
			t.Fatal("bad profile ID")
		}
	}
}

func TestExecutionScopeEventsPermanentAndSaturated(t *testing.T) {
	s := executionFixture(t)
	changed := s
	changed.WallHighWater = "2026-10-20T00:00:01.000000Z"
	changed.Generation++
	changed.NormalTransitionsUsed++
	if ScopeTransition(s, changed, false) == nil {
		t.Fatal("clock-only write consumed normal budget")
	}
	denied, e := AdvanceScope(s, ScopeDeny, s.WallHighWater, 0, func(v *ExecutionScope) {
		v.DenyLatched = true
		v.ExecutionDenyLatched = true
		v.State = "cleaning"
		v.Reason = "cancelled"
	})
	if e != nil {
		t.Fatal(e)
	}
	for i, reason := range []string{"none", "writer_active", "reader_active", "collector_active", "owner_mismatch", "io_uncertain", "lease_uncertain", "record_uncertain", "clock_uncertain"} {
		o := ScopeObservation{Schema: ScopeObservationSchema, ObservationID: fmt.Sprintf("%032x", i+400), ScopeID: s.ScopeID, ObservedAt: s.CreatedAt, Method: "synthetic_execution_scope_v1", Assurance: "synthetic", NewAdmission: "denied", WritersDrained: "unknown", ReadersDrained: "unknown", CollectorDrained: "unknown", ScopeAccessRevocation: "unknown", OwnedRemoval: "unknown", Reason: reason}
		denied, e = AddScopeObservation(denied, o)
		if e != nil {
			t.Fatal(i, e)
		}
	}
	if !denied.ObservationsSaturated || len(denied.Observations) != 8 || denied.SafetyTransitionsUsed != 10 {
		t.Fatalf("unexpected finite observation accounting: %+v", denied)
	}
	before, _ := json.Marshal(denied)
	for i := 0; i < 100; i++ {
		o := denied.Observations[i%8]
		o.ObservationID = fmt.Sprintf("%032x", i+500)
		o.ObservedAt = "2026-10-20T00:01:00.000000Z"
		denied, e = AddScopeObservation(denied, o)
		if e != nil {
			t.Fatal(e)
		}
	}
	after, _ := json.Marshal(denied)
	if !bytes.Equal(before, after) {
		t.Fatal("saturated observations mutated authority")
	}
	_, e = AdvanceScope(denied, ScopeUnresolved, denied.WallHighWater, 0, func(v *ExecutionScope) { v.State = "cleanup_unresolved" })
	if e != nil {
		t.Fatal("safety after saturation:", e)
	}
	for name, mutate := range map[string]func(*ExecutionScope){"plan-shrink": func(v *ExecutionScope) { v.Slots = v.Slots[:len(v.Slots)-1] }, "rebind": func(v *ExecutionScope) { v.ProjectID = strings.Repeat("a", 32) }, "counter-reset": func(v *ExecutionScope) { v.SafetyTransitionsUsed = 0 }, "undeniable": func(v *ExecutionScope) { v.DenyLatched = false }} {
		t.Run(name, func(t *testing.T) {
			bad := Clone(denied)
			mutate(&bad)
			bad.Generation++
			bad.SafetyTransitionsUsed++
			if ScopeTransition(denied, bad, false) == nil {
				t.Fatal("accepted permanent-state mutation")
			}
		})
	}
}

func maximumIndependentScope(t *testing.T) ExecutionScope {
	s := executionFixture(t)
	maxIdentity := FilesystemIdentity{Device: "18446744073709551615", Inode: "18446744073709551615"}
	s.FilesystemIdentity = ExecutionFilesystemBinding{Root: maxIdentity, Scope: &maxIdentity, WriterLease: &maxIdentity, WatchLease: &maxIdentity, Content: &maxIdentity, OwnerRead: maxIdentity, WatchRead: maxIdentity, WatchWrite: maxIdentity, ExecutionLease: &maxIdentity, WorkRoot: &maxIdentity, OutputRoot: &maxIdentity}
	s.WatchStarted = true
	watch := strings.Repeat("a", 32)
	s.WatchEpoch = &watch
	s.WriterStarted = true
	s.PrepareOwnershipCloseConfirmed = true
	s.WriterOwnershipCloseConfirmed = true
	s.SealStarted = true
	s.SealOwnershipCloseConfirmed = true
	s.WatchOwnershipCloseConfirmed = true
	s.CleanupOwnershipCloseConfirmed = true
	epoch := strings.Repeat("a", 32)
	s.CleanupOwnerEpoch = &epoch
	s.LeaseCloseUncertain = true
	s.UploadComplete = true
	s.ContentCloseConfirmed = true
	s.ContentCloseUncertain = true
	s.DenyLatched = true
	s.ExecutionDenyLatched = true
	s.State = "cleanup_unresolved"
	s.Reason = "incomplete_upload"
	length := s.DeclaredUploadCeilingBytes
	s.SealedLengthBytes = &length
	s.Generation = ScopeGenerationMax
	s.NormalTransitionsUsed = ScopeNormalCapacity
	s.SafetyTransitionsUsed = ScopeSafetyCapacity
	s.ObservationsSaturated = true
	s.Capability.ID = strings.Repeat("a", 64)
	s.Capability.Version = 9007199254740991
	s.Capability.Workload.EnvironmentID = strings.Repeat("a", 80)
	s.Capability.Workload.InterpreterID = strings.Repeat("a", 80)
	s.Capability.Workload.InterpreterVersion = strings.Repeat("+", 64)
	s.Capability.Workload.Image = strings.Repeat("a", 440) + "@sha256:" + strings.Repeat("a", 64)
	s.Capability.Checker = s.Capability.Workload
	s.Capability.Checker.RecipeID = strings.Repeat("b", 80)
	s.Capability.WorkloadArgv = []string{}
	s.Capability.CheckerArgv = []string{}
	for i := 0; i < 16; i++ {
		s.Capability.WorkloadArgv = append(s.Capability.WorkloadArgv, strings.Repeat("<", 128))
		s.Capability.CheckerArgv = append(s.Capability.CheckerArgv, strings.Repeat("&", 128))
	}
	for _, p := range []*ImplementationBinding{&s.Capability.Provider, &s.Capability.Storage, &s.Capability.Collector, &s.Capability.Quality} {
		p.ID = strings.Repeat("i", 64)
		p.Version = 9007199254740991
	}
	s.Manifest.Workload.EnvironmentID = s.Capability.Workload.EnvironmentID
	s.Manifest.Workload.Image = s.Capability.Workload.Image
	s.ManifestSHA256 = Digest(s.Manifest)
	s.CapabilitySHA256 = Digest(s.Capability)
	for i, reason := range []string{"none", "writer_active", "reader_active", "collector_active", "owner_mismatch", "io_uncertain", "lease_uncertain", "record_uncertain"} {
		s.Observations = append(s.Observations, ScopeObservation{Schema: ScopeObservationSchema, ObservationID: fmt.Sprintf("%032x", 700+i), ScopeID: s.ScopeID, ScopeGeneration: 81, ObservedAt: s.CreatedAt, Method: "synthetic_execution_scope_v1", Assurance: "synthetic", NewAdmission: "denied", WritersDrained: "unknown", ReadersDrained: "unknown", CollectorDrained: "unknown", ScopeAccessRevocation: "unknown", OwnedRemoval: "unknown", Reason: reason})
	}
	if e := s.Validate(); e != nil {
		t.Fatal(e)
	}
	return s
}

func TestExecutionMaximumScopeIndependentSerialization(t *testing.T) {
	s := maximumIndependentScope(t)
	raw, e := json.Marshal(s)
	if e != nil || len(raw) > MaxUnitBytes {
		t.Fatal("maximum escaped scope exceeds unit", len(raw), e)
	}
	t.Logf("independent longest scope bytes=%d ceiling=%d", len(raw), MaxUnitBytes)
}

func TestExecutionClaimedCreateRequiresResourceIdentity(t *testing.T) {
	s := executionFixture(t)
	a := AssessmentAttempt{Schema: AttemptSchema, Binding: s.Binding(), Slot: s.Slots[0], HandoffNonce: strings.Repeat("a", 32), ClaimEpoch: strings.Repeat("b", 32), ClaimedAt: s.CreatedAt, Deadline: "2026-10-20T00:30:00Z", ClockEpoch: s.ClockEpoch, DeadlineTickMS: 1800000, Generation: 1, State: "terminal", Outcome: "unknown", FailureCode: "none", Compute: Compute{ReservedCPUSeconds: 1800, ChargedCPUSeconds: 1800, Provenance: "estimated_upper_bound"}, ComputeCleanupObserved: true, Metrics: UnknownMeasurements(), Quality: Quality{Status: "unknown", Source: "none", Checker: s.Capability.Checker}, CleanupObservations: []AttemptCleanupObservation{}, WallHighWater: s.CreatedAt}
	a.BindingDigest = AttemptBindingDigest(a.Binding, a.Slot, a.HandoffNonce)
	a.PhaseRecords.Workload = &PhaseRecord{Phase: "workload", OwnerName: ResourceName(a.Binding.BrokerAuthorityID, a.Slot.AttemptID, "workload"), CreateClaimed: true, CreateReturned: true, StoppedObserved: true, RemovalObserved: true}
	if a.EffectsClosed() || a.SerialClear() {
		t.Error("claimed creation without exact resource identity was treated as final")
	}
	if a.Validate(s) == nil {
		t.Error("accepted cleanup/settlement without resource identity")
	}
}
