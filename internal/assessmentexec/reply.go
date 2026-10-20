package assessmentexec

import (
	"bytes"

	"github.com/EduCloud-Ecosystem/outfitter/internal/assessment"
)

func validCompute(c Compute, unresolved bool) bool {
	if !uintn(c.ReservedCPUSeconds, 1, 1800) {
		return false
	}
	if unresolved {
		return c.ChargedCPUSeconds == 0 && c.Provenance == "reserved"
	}
	return c.ChargedCPUSeconds == c.ReservedCPUSeconds && c.Provenance == "estimated_upper_bound"
}
func (p PhasePublic) validate(phase string) bool {
	if p.Phase != phase || p.CreateClaimed && !p.TransportStarted || p.CreateReturned && !p.CreateClaimed || p.CreateAmbiguous && !p.CreateClaimed || p.StartClaimed && !p.CreateReturned || p.StartReturned && !p.StartClaimed || p.StartedObserved && !p.StartClaimed || (p.StoppedObserved || p.RemovalObserved) && !p.CreateClaimed || p.RemovalObserved && !p.StoppedObserved || (p.TransportCloseConfirmed || p.TransportCloseUncertain) && !p.TransportStarted {
		return false
	}
	return !(p.ExitCode != nil && (!uintn(*p.ExitCode, 0, 255) || !p.StartedObserved || p.FinishedAt == nil) || p.FinishedAt != nil && (p.ExitCode == nil || !stamp(*p.FinishedAt, true)) || p.StartedAt != nil && (!p.StartedObserved || !stamp(*p.StartedAt, true)) || p.StartedAt != nil && p.FinishedAt != nil && *p.FinishedAt < *p.StartedAt)
}
func (a AttemptPublic) validate(b ScopeBinding) bool {
	if !validSlot(a.Slot) || !id(a.HandoffNonce) || !id(a.ClaimEpoch) || a.BindingDigest != AttemptBindingDigest(b, a.Slot, a.HandoffNonce) || !stamp(a.ClaimedAt, true) || !stamp(a.Deadline, false) || !uintn(a.Generation, 1, AttemptGenerationMax) || !one(a.State, "claimed", "preparing", "running", "checking", "finishing", "held", "terminal") || !one(a.Outcome, "unknown", "succeeded", "failed", "cancelled") || !one(a.FailureCode, "none", "cancelled", "expired", "access_ended", "restart", "clock_rollback", "owner_mismatch", "io_uncertain", "execution_failed", "output_limit", "quality_failed", "measurement_unknown", "cleanup_unknown", "create_ambiguous") || !validCompute(a.Compute, a.AccountingUnresolved) || a.Metrics.Validate() != nil || a.Quality.Checker != b.Checker || !one(a.Quality.Status, "unknown", "passed", "failed") || a.Quality.Status == "unknown" && a.Quality.Source != "none" || a.Quality.Status != "unknown" && a.Quality.Source != "unverified_checker" {
		return false
	}
	if !a.AccountingUnresolved && (!a.ComputeCleanupObserved || !one(a.State, "held", "terminal")) {
		return false
	}
	if a.Phases.Workload != nil && !a.Phases.Workload.validate("workload") || a.Phases.Checker != nil && (!a.Phases.Checker.validate("checker") || a.Phases.Workload == nil) {
		return false
	}
	if a.Outcome == "succeeded" && (a.AccountingUnresolved || a.Quality.Status != "passed" || a.Phases.Workload == nil || a.Phases.Workload.ExitCode == nil || *a.Phases.Workload.ExitCode != 0 || a.Phases.Checker == nil || a.Phases.Checker.ExitCode == nil || *a.Phases.Checker.ExitCode != 0) {
		return false
	}
	if a.MeasurementDigest != nil && *a.MeasurementDigest != Digest(a.Metrics) {
		return false
	}
	if a.CleanupObservations == nil || len(a.CleanupObservations) > MaxObservations || a.ObservationsSaturated && len(a.CleanupObservations) != MaxObservations {
		return false
	}
	seen := map[string]bool{}
	ids := map[string]bool{}
	for _, o := range a.CleanupObservations {
		if o.Schema != AttemptObservationSchema || !id(o.ObservationID) || ids[o.ObservationID] || o.AttemptID != a.Slot.AttemptID || o.ScopeID != b.ScopeID || !uintn(o.AttemptGeneration, 1, a.Generation) || !stamp(o.ObservedAt, true) || !one(o.WorkloadRemoval, "observed_absent", "failed", "unknown", "not_created") || !one(o.CheckerRemoval, "observed_absent", "failed", "unknown", "not_created") || !one(o.ReadersDrained, "observed", "failed", "unknown") || !one(o.CollectorDrained, "observed", "failed", "unknown") || !one(o.Reason, "none", "resource_active", "late_create_possible", "reader_active", "collector_active", "owner_mismatch", "io_uncertain", "record_uncertain", "clock_uncertain") || o.Assurance != "synthetic" || o.CleanupTrusted || o.ActualResourceAccepted {
			return false
		}
		k := attemptObservationKey(o)
		if seen[k] {
			return false
		}
		seen[k] = true
		ids[o.ObservationID] = true
	}
	return true
}
func (s Status) Validate() error {
	if s.Schema != StatusSchema || !one(s.Operation, "prepare", "ingest", "seal", "status", "cancel", "reconcile") || !one(s.Code, "OK", "HELD") || s.Binding.Validate() != nil || !uintn(s.Generation, 1, ScopeGenerationMax) || !one(s.State, "preparing", "accepting", "sealed", "cleaning", "cleanup_unresolved") || !one(s.Reason, "none", "cancelled", "access_ended", "expired", "restart", "clock_rollback", "incomplete_upload", "io_uncertain", "owner_mismatch", "observations_full") || !stamp(s.ExpiresAt, false) || !uintn(s.DeclaredUploadCeilingBytes, 1, 5000000000) || !id(s.WriterEpoch) || s.WatchEpoch != nil && !id(*s.WatchEpoch) || !id(s.AdmittingOwnerEpoch) || !id(s.ClockEpoch) || !uintn(s.DeadlineTickMS, 1, 9007199254740991) || !one(s.Liveness, "observed_current", "unknown") || s.Trust != Untrusted() {
		return ErrInvalid
	}
	if s.DenyLatched != s.ExecutionDenyLatched || s.DenyLatched != (s.State == "cleaning" || s.State == "cleanup_unresolved") || s.DenyLatched != (s.Reason != "none") || s.DenyLatched && s.Code != "HELD" || s.ContentCloseUncertain && !s.DenyLatched || (s.ContentCloseConfirmed || s.ContentCloseUncertain || s.UploadComplete) && !s.WriterStarted || s.UploadComplete && (!s.ContentCloseConfirmed || s.SealedLengthBytes == nil) || s.SealedLengthBytes != nil && (!uintn(*s.SealedLengthBytes, 1, s.DeclaredUploadCeilingBytes) || !s.UploadComplete) || s.State == "sealed" && (!s.UploadComplete || s.ContentCloseUncertain) || one(s.State, "accepting", "sealed") && s.WatchEpoch == nil {
		return ErrInvalid
	}
	if s.Observations == nil || len(s.Observations) > MaxObservations || s.ObservationsSaturated && len(s.Observations) != MaxObservations {
		return ErrInvalid
	}
	seen := map[string]bool{}
	ids := map[string]bool{}
	for _, o := range s.Observations {
		if o.Schema != ScopeObservationSchema || !id(o.ObservationID) || ids[o.ObservationID] || o.ScopeID != s.Binding.ScopeID || !uintn(o.ScopeGeneration, 1, s.Generation) || !stamp(o.ObservedAt, true) || o.Method != "synthetic_execution_scope_v1" || o.Assurance != "synthetic" || o.NewAdmission != "denied" || !one(o.WritersDrained, "observed", "failed", "unknown") || !one(o.ReadersDrained, "observed", "failed", "unknown") || !one(o.CollectorDrained, "observed", "failed", "unknown") || !one(o.ScopeAccessRevocation, "observed", "failed", "unknown") || !one(o.OwnedRemoval, "observed_absent", "failed", "unknown") || !one(o.Reason, "none", "writer_active", "reader_active", "collector_active", "owner_mismatch", "io_uncertain", "lease_uncertain", "record_uncertain", "clock_uncertain") || o.CleanupTrusted || o.ActualResourceAccepted || o.OwnedRemoval == "observed_absent" && (o.WritersDrained != "observed" || o.ReadersDrained != "observed" || o.CollectorDrained != "observed" || o.ScopeAccessRevocation != "observed") {
			return ErrInvalid
		}
		k := scopeObservationKey(o)
		if seen[k] {
			return ErrInvalid
		}
		seen[k] = true
		ids[o.ObservationID] = true
	}
	if s.Attempt != nil && !s.Attempt.validate(s.Binding) {
		return ErrInvalid
	}
	return nil
}
func (r Ready) Validate() error {
	if r.Schema != ReadySchema || r.Binding.Validate() != nil || !uintn(r.Generation, 1, ScopeGenerationMax) || !stamp(r.ExpiresAt, false) || !uintn(r.DeclaredUploadCeilingBytes, 1, 5000000000) || !id(r.AdmittingOwnerEpoch) || !id(r.WriterEpoch) || !id(r.WatchEpoch) || !id(r.ClockEpoch) || !uintn(r.DeadlineTickMS, 1, 9007199254740991) || r.Assurance != "synthetic" {
		return ErrInvalid
	}
	return nil
}
func (r Result) Validate() error {
	if r.Schema != ResultSchema || r.Operation != "execute" || !one(r.Code, "OK", "NOT_ADMITTED", "HELD") || !one(r.Disposition, "new_attempt", "existing_attempt", "not_admitted") || r.Binding.Validate() != nil || !validSlot(r.Slot) || !id(r.HandoffNonce) || r.BindingDigest != AttemptBindingDigest(r.Binding, r.Slot, r.HandoffNonce) || !uintn(r.ScopeGeneration, 1, ScopeGenerationMax) || r.Trust != Untrusted() {
		return ErrInvalid
	}
	if r.Disposition == "not_admitted" {
		if r.Code != "NOT_ADMITTED" || r.Attempt != nil || r.Compute != (Compute{Provenance: "not_admitted"}) || r.AccountingUnresolved || r.NormalizedReceipts != nil || r.NormalizedReceiptsSHA256 != nil {
			return ErrInvalid
		}
		return nil
	}
	if r.Code == "NOT_ADMITTED" || r.Attempt == nil || !r.Attempt.validate(r.Binding) || r.Attempt.Slot != r.Slot || r.Attempt.HandoffNonce != r.HandoffNonce || r.Attempt.BindingDigest != r.BindingDigest || r.Attempt.Compute != r.Compute || r.Attempt.AccountingUnresolved != r.AccountingUnresolved {
		return ErrInvalid
	}
	if r.Code == "OK" && (r.Attempt.State != "terminal" || r.AccountingUnresolved || !r.Attempt.ComputeCleanupObserved) {
		return ErrInvalid
	}
	if r.NormalizedReceipts == nil {
		if r.Code != "HELD" || r.NormalizedReceiptsSHA256 != nil {
			return ErrInvalid
		}
		return nil
	}
	if r.NormalizedReceiptsSHA256 == nil || *r.NormalizedReceiptsSHA256 != Digest(r.NormalizedReceipts) {
		return ErrInvalid
	}
	set := r.NormalizedReceipts
	m, c := set.Measurement, set.Cleanup
	if set.Schema != assessment.SetSchemaV2 || m.Schema != assessment.MeasurementSchemaV2 || c.Schema != assessment.CleanupSchemaV2 || m.Binding != c.Binding || m.CleanupReceiptID != c.ReceiptID || m.CleanupReceiptSHA256 != Digest(c) || !m.Execution.Admitted || m.Execution.DiagnosticsAvailable || m.ReservationCeiling.CPUSeconds != r.Compute.ReservedCPUSeconds || m.ConservativeAccounting.ChargedCPUSeconds != r.Compute.ChargedCPUSeconds || m.ConservativeAccounting.Provenance != r.Compute.Provenance || m.ConservativeAccounting.UnresolvedLiability != r.AccountingUnresolved || !eq(m.MeasuredConsumption, r.Attempt.Metrics.Workload.Map()) || m.Quality.Status != r.Attempt.Quality.Status || m.Quality.Source != r.Attempt.Quality.Source {
		return ErrInvalid
	}
	b := m.Binding
	if b.ManifestSHA256 != r.Binding.ManifestSHA256 || b.ManifestSchema != r.Binding.ManifestSchema || b.ExecutionProfile != r.Binding.ExecutionProfile || b.AssessmentID != r.Binding.AssessmentID || b.ProjectID != r.Binding.ProjectID || b.AttemptID != r.Slot.AttemptID || b.ReservationID != r.Slot.ReservationID || b.StageID != r.Slot.StageID || b.SampleRole != r.Slot.SampleRole || b.SampleSize != r.Slot.SampleSize || b.Repetition != r.Slot.Repetition || b.EnvironmentID != r.Binding.Workload.EnvironmentID || b.Image != r.Binding.Workload.Image {
		return ErrInvalid
	}
	if m.Observation != (assessment.Observation{SourceKind: "synthetic_fixture"}) || c.Observation != m.Observation || c.Termination != "unknown" || c.AccessRevocation != "unknown" || c.Outcome != "unresolved" || !c.Quarantine || !c.ReuseDenied || c.VerifierID != nil || c.ObservedAt != nil || c.PriorReceiptSHA256 != nil {
		return ErrInvalid
	}
	return nil
}

// Decoding is a structural/correlation check only. Imported messages never
// construct a capability, release an accounting hold, or prove data cleanup.
func replyFrame(raw []byte) ([]byte, error) {
	if len(raw) < 2 || len(raw) > MaxReplyBytes || raw[len(raw)-1] != '\n' || bytes.ContainsAny(raw[:len(raw)-1], "\r\n") {
		return nil, ErrInvalid
	}
	return raw[:len(raw)-1], nil
}
func DecodeStatus(raw []byte, c Control) (Status, error) {
	var s Status
	body, e := replyFrame(raw)
	if e != nil || c.Validate() != nil || DecodeClosed(body, &s, MaxReplyBytes) != nil || s.Validate() != nil || s.Operation != c.Operation || s.Binding != c.Binding || c.Slot == nil && s.Attempt != nil || c.Slot != nil && (s.Attempt == nil || s.Attempt.Slot != *c.Slot) {
		return Status{}, ErrInvalid
	}
	return s, nil
}

// DecodeResult requires a locally trusted journal snapshot. A matching digest
// proves bytes only; it cannot replace exact manifest/attempt projection.
func DecodeResult(raw []byte, c Control, scope ExecutionScope, attempt *AssessmentAttempt) (Result, error) {
	var r Result
	body, e := replyFrame(raw)
	if e != nil || c.Validate() != nil || c.Operation != "execute" || DecodeClosed(body, &r, MaxReplyBytes) != nil || r.Validate() != nil || r.Binding != c.Binding || r.Slot != *c.Slot || r.HandoffNonce != *c.HandoffNonce {
		return Result{}, ErrInvalid
	}
	if scope.Validate() != nil || scope.Binding() != c.Binding || r.ScopeGeneration != scope.Generation {
		return Result{}, ErrInvalid
	}
	if r.Disposition == "not_admitted" {
		if attempt != nil {
			return Result{}, ErrInvalid
		}
		return r, nil
	}
	if attempt == nil || attempt.Validate(scope) != nil || !eq(r.Attempt, attempt.Public()) {
		return Result{}, ErrInvalid
	}
	if r.NormalizedReceipts != nil {
		expected, e := Project(scope, *attempt)
		if e != nil || !eq(*r.NormalizedReceipts, expected) {
			return Result{}, ErrInvalid
		}
	}
	return r, nil
}
