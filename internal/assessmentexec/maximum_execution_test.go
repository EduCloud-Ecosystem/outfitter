package assessmentexec

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// This is independently built from field maxima, not a production padding or
// "maximum" constructor. Both uncertainty and prior full-charge forms remain
// representable without erasing old observations or enlarging a unit.
func maximumIndependentAttempt(t *testing.T, s ExecutionScope) AssessmentAttempt {
	t.Helper()
	a := AssessmentAttempt{Schema: AttemptSchema, Binding: s.Binding(), Slot: s.Slots[len(s.Slots)-1], HandoffNonce: strings.Repeat("f", 32), ClaimEpoch: strings.Repeat("e", 32), ClaimedAt: s.CreatedAt, Deadline: "2026-10-20T00:30:00Z", ClockEpoch: s.ClockEpoch, DeadlineTickMS: 1800000, Generation: AttemptGenerationMax, NormalTransitionsUsed: AttemptNormalCapacity, SafetyTransitionsUsed: AttemptSafetyCapacity, State: "terminal", Outcome: "unknown", DenyLatched: true, FailureCode: "measurement_unknown", Compute: Compute{ReservedCPUSeconds: 1800, ChargedCPUSeconds: 1800, Provenance: "estimated_upper_bound"}, ComputeCleanupObserved: true, ReaderStarted: true, ReaderCloseConfirmed: true, ReaderCloseUncertain: true, CollectorStarted: true, CollectorCloseConfirmed: true, CollectorCloseUncertain: true, Metrics: UnknownMeasurements(), Quality: Quality{Status: "passed", Source: "unverified_checker", Checker: s.Capability.Checker}, CleanupObservations: []AttemptCleanupObservation{}, ObservationsSaturated: true, WallHighWater: "9999-12-31T23:59:59.999999Z", TickHighWaterMS: 9007199254740991}
	a.OwnershipCloseConfirmed = true
	a.OwnershipCloseUncertain = true
	a.BindingDigest = AttemptBindingDigest(a.Binding, a.Slot, a.HandoffNonce)
	for i, name := range []string{"workload", "checker"} {
		exit := int64(255)
		at := "9999-12-31T23:59:59.999999Z"
		p := &PhaseRecord{Phase: name, OwnerName: ResourceName(a.Binding.BrokerAuthorityID, a.Slot.AttemptID, name), CreateClaimed: true, CreateReturned: true, CreateAmbiguous: true, StartClaimed: true, StartReturned: true, StartedObserved: true, StoppedObserved: true, RemovalObserved: true, ExitCode: &exit, StartedAt: &at, FinishedAt: &at, TransportStarted: true, TransportCloseConfirmed: true, TransportCloseUncertain: true, ResourceIdentity: &DockerResourceIdentity{ContainerID: strings.Repeat("d", 64), BrokerOwner: a.Binding.BrokerAuthorityID, AttemptID: a.Slot.AttemptID, Phase: name, ScopeID: s.ScopeID, CapabilitySHA256: s.CapabilitySHA256}}
		if i == 0 {
			a.PhaseRecords.Workload = p
		} else {
			a.PhaseRecords.Checker = p
		}
	}
	for _, set := range []*MetricSet{&a.Metrics.Workload, &a.Metrics.Checker, &a.Metrics.Attempt} {
		v := reflect.ValueOf(set).Elem()
		for i := 0; i < v.NumField(); i++ {
			m := v.Field(i).Interface().(Metric)
			if m.Availability != "unsupported" {
				max := int64(9007199254740991)
				m.Value = &max
				m.Availability = "available"
				m.Source = "unverified_collector"
				v.Field(i).Set(reflect.ValueOf(m))
			}
		}
	}
	digest := Digest(a.Metrics)
	a.MeasurementDigest = &digest
	for i, reason := range []string{"resource_active", "late_create_possible", "reader_active", "collector_active", "owner_mismatch", "io_uncertain", "record_uncertain", "clock_uncertain"} {
		a.CleanupObservations = append(a.CleanupObservations, AttemptCleanupObservation{Schema: AttemptObservationSchema, ObservationID: fmt.Sprintf("%032x", i+900), AttemptID: a.Slot.AttemptID, ScopeID: s.ScopeID, AttemptGeneration: AttemptGenerationMax, ObservedAt: a.WallHighWater, WorkloadRemoval: "observed_absent", CheckerRemoval: "observed_absent", ReadersDrained: "unknown", CollectorDrained: "unknown", AccountingUnresolved: false, Reason: reason, Assurance: "synthetic"})
	}
	if e := a.Validate(s); e != nil {
		t.Fatal("independent maximal attempt", e)
	}
	return a
}
func TestExecutionIndependentMaximumAttemptAndReplies(t *testing.T) {
	s := maximumIndependentScope(t)
	a := maximumIndependentAttempt(t, s)
	raw, e := json.Marshal(a)
	if e != nil || len(raw) > MaxUnitBytes {
		t.Fatal(len(raw), e)
	}
	t.Logf("independent longest attempt bytes=%d ceiling=%d", len(raw), MaxUnitBytes)
	forms := []any{s.Status("status", "HELD", "unknown", &a), s.Status("status", "HELD", "unknown", nil), Ready{Schema: ReadySchema, Binding: s.Binding(), Generation: ScopeGenerationMax, ExpiresAt: s.ExpiresAt, DeclaredUploadCeilingBytes: s.DeclaredUploadCeilingBytes, AdmittingOwnerEpoch: s.AdmittingOwnerEpoch, WriterEpoch: s.WriterEpoch, WatchEpoch: *s.WatchEpoch, ClockEpoch: s.ClockEpoch, DeadlineTickMS: s.DeadlineTickMS, Assurance: "synthetic"}, Error{Schema: ErrorSchema, Code: ErrCapacity.Error()}}
	receipts, e := Project(s, a)
	if e != nil {
		t.Fatal("unknown settled maximum projection", e)
	}
	hash := Digest(receipts)
	forms = append(forms, Result{Schema: ResultSchema, Operation: "execute", Code: "HELD", Disposition: "existing_attempt", Binding: s.Binding(), Slot: a.Slot, HandoffNonce: a.HandoffNonce, BindingDigest: a.BindingDigest, ScopeGeneration: s.Generation, Attempt: a.Public(), Compute: a.Compute, AccountingUnresolved: false, NormalizedReceipts: &receipts, NormalizedReceiptsSHA256: &hash, Trust: Untrusted()})
	for _, form := range forms {
		wire, e := EncodeReply(form)
		if e != nil || len(wire) > MaxReplyBytes {
			t.Fatalf("%T bytes=%d err=%v", form, len(wire), e)
		}
		t.Logf("max %T bytes=%d", form, len(wire))
	}
	// Held accounting is a distinct shape: no charge, no double count. It does
	// not become trusted merely because the separately projected JSON fits.
	a.State = "held"
	a.OwnershipCloseConfirmed = false
	a.OwnershipCloseUncertain = true
	a.AccountingUnresolved = true
	a.ComputeCleanupObserved = false
	a.Compute.ChargedCPUSeconds = 0
	a.Compute.Provenance = "reserved"
	for i := range a.CleanupObservations {
		a.CleanupObservations[i].AccountingUnresolved = true
	}
	if e := a.Validate(s); e != nil {
		t.Fatal("valid unconfirmed held maximum", e)
	}
	heldRaw, e := json.Marshal(a)
	if e != nil || len(heldRaw) > MaxUnitBytes {
		t.Fatal("held maximum serialization", len(heldRaw), e)
	}
	t.Logf("independent held attempt bytes=%d ceiling=%d", len(heldRaw), MaxUnitBytes)
	if _, e := Project(s, a); e != nil {
		t.Fatal("unknown held maximum projection", e)
	}
}
