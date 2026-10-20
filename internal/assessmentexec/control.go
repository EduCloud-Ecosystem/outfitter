package assessmentexec

import (
	"encoding/json"

	"github.com/EduCloud-Ecosystem/outfitter/internal/assessment"
)

func DecodeControl(raw []byte, operation string) (Control, error) {
	var c Control
	if DecodeClosed(raw, &c, MaxControlBytes) != nil || c.Operation != operation || c.Validate() != nil {
		return Control{}, ErrInvalid
	}
	return c, nil
}
func (c Control) Validate() error {
	if c.Schema != ControlSchema || c.Binding.Validate() != nil {
		return ErrInvalid
	}
	base := Control{Schema: c.Schema, Operation: c.Operation, Binding: c.Binding}
	switch c.Operation {
	case "prepare":
		if c.Manifest == nil || c.AdmittingOwnerEpoch == nil || !id(*c.AdmittingOwnerEpoch) || c.DeclaredUploadCeilingBytes == nil || c.ExpiresAt == nil || !stamp(*c.ExpiresAt, false) {
			return ErrInvalid
		}
		raw, _ := json.Marshal(c.Manifest)
		m, e := assessment.DecodeManifestV2(raw)
		if e != nil || !CompletePlan(m) || !id(m.AssessmentID) || !id(m.ProjectID) || m.DataClassification != "synthetic" || Digest(m) != c.Binding.ManifestSHA256 || m.AssessmentID != c.Binding.AssessmentID || m.ProjectID != c.Binding.ProjectID || !eq(m.ExecutionProfile, c.Binding.ExecutionProfile) || !uintn(*c.DeclaredUploadCeilingBytes, 1, m.Ceilings.InputBytes) || *c.ExpiresAt > m.ExpiresAt {
			return ErrInvalid
		}
		w := m.Workload
		b := c.Binding.Workload
		if w.RecipeID != b.RecipeID || w.RecipeSHA256 != b.CodeSHA256 || w.EnvironmentID != b.EnvironmentID || w.Image != b.Image {
			return ErrInvalid
		}
		base.Manifest = c.Manifest
		base.AdmittingOwnerEpoch = c.AdmittingOwnerEpoch
		base.DeclaredUploadCeilingBytes = c.DeclaredUploadCeilingBytes
		base.ExpiresAt = c.ExpiresAt
	case "ingest", "seal", "execute":
		if c.ExpectedGeneration == nil || !uintn(*c.ExpectedGeneration, 1, ScopeGenerationMax) || c.AdmittingOwnerEpoch == nil || !id(*c.AdmittingOwnerEpoch) {
			return ErrInvalid
		}
		base.ExpectedGeneration = c.ExpectedGeneration
		base.AdmittingOwnerEpoch = c.AdmittingOwnerEpoch
		if c.Operation == "execute" {
			if c.Slot == nil || !validSlot(*c.Slot) || c.HandoffNonce == nil || !id(*c.HandoffNonce) {
				return ErrInvalid
			}
			base.Slot = c.Slot
			base.HandoffNonce = c.HandoffNonce
		} else {
			if c.WriterEpoch == nil || !id(*c.WriterEpoch) {
				return ErrInvalid
			}
			base.WriterEpoch = c.WriterEpoch
		}
		if c.Operation == "ingest" {
			if c.ContentLength == nil || !uintn(*c.ContentLength, 1, 5000000000) {
				return ErrInvalid
			}
			base.ContentLength = c.ContentLength
		}
	case "status", "cancel", "reconcile":
		if c.Slot != nil && !validSlot(*c.Slot) {
			return ErrInvalid
		}
		base.Slot = c.Slot
		if c.Operation == "cancel" {
			if c.Reason == nil || !one(*c.Reason, "cancelled", "access_ended") {
				return ErrInvalid
			}
			base.Reason = c.Reason
		}
	default:
		return ErrInvalid
	}
	if !eq(c, base) {
		return ErrInvalid
	}
	return nil
}

func Untrusted() Trust { return Trust{Assurance: "synthetic"} }
func phasePublic(p *PhaseRecord) *PhasePublic {
	if p == nil {
		return nil
	}
	return &PhasePublic{p.Phase, p.CreateClaimed, p.CreateReturned, p.CreateAmbiguous, p.StartClaimed, p.StartReturned, p.StartedObserved, p.StoppedObserved, p.RemovalObserved, p.ExitCode, p.StartedAt, p.FinishedAt, p.TransportStarted, p.TransportCloseConfirmed, p.TransportCloseUncertain}
}
func (a AssessmentAttempt) Public() *AttemptPublic {
	return &AttemptPublic{a.Slot, a.BindingDigest, a.HandoffNonce, a.ClaimEpoch, a.ClaimedAt, a.Deadline, a.Generation, a.State, a.Outcome, a.DenyLatched, a.FailureCode, a.Compute, a.AccountingUnresolved, a.ComputeCleanupObserved, PhasesPublic{phasePublic(a.PhaseRecords.Workload), phasePublic(a.PhaseRecords.Checker)}, a.Metrics, a.Quality, Clone(a.CleanupObservations), a.ObservationsSaturated, a.MeasurementDigest}
}
func (s ExecutionScope) Status(op, code, liveness string, a *AssessmentAttempt) Status {
	out := Status{Schema: StatusSchema, Operation: op, Code: code, Binding: s.Binding(), Generation: s.Generation, State: s.State, DenyLatched: s.DenyLatched, Reason: s.Reason, ExpiresAt: s.ExpiresAt, DeclaredUploadCeilingBytes: s.DeclaredUploadCeilingBytes, SealedLengthBytes: s.SealedLengthBytes, WriterEpoch: s.WriterEpoch, WatchEpoch: s.WatchEpoch, AdmittingOwnerEpoch: s.AdmittingOwnerEpoch, ClockEpoch: s.ClockEpoch, DeadlineTickMS: s.DeadlineTickMS, WriterStarted: s.WriterStarted, UploadComplete: s.UploadComplete, ContentCloseConfirmed: s.ContentCloseConfirmed, ContentCloseUncertain: s.ContentCloseUncertain, ExecutionDenyLatched: s.ExecutionDenyLatched, Liveness: liveness, Observations: Clone(s.Observations), Trust: Untrusted(), ObservationsSaturated: s.ObservationsSaturated}
	if a != nil {
		out.Attempt = a.Public()
	}
	return out
}

// EncodeReply is the only wire encoder: bounded canonical bytes plus one LF.
// Callers must supply typed projections, never an Authority, Scope or private
// attempt. No input byte, resource ID, raw diagnostic or filesystem path leaks.
func EncodeReply(v any) ([]byte, error) {
	switch x := v.(type) {
	case Status:
		if x.Validate() != nil {
			return nil, ErrInvalid
		}
	case Result:
		if x.Validate() != nil {
			return nil, ErrInvalid
		}
	case Ready:
		if x.Validate() != nil {
			return nil, ErrInvalid
		}
	case Error:
		if x.Schema != ErrorSchema || !one(x.Code, ErrInvalid.Error(), ErrDisabled.Error(), ErrDenied.Error(), ErrHeld.Error(), ErrCollision.Error(), ErrCapacity.Error(), ErrMissing.Error(), ErrStale.Error(), ErrBusy.Error()) {
			return nil, ErrInvalid
		}
	default:
		return nil, ErrInvalid
	}
	b, e := assessment.Canonical(v)
	if e != nil || len(b)+1 > MaxReplyBytes {
		return nil, ErrCapacity
	}
	return append(b, '\n'), nil
}
