package assessment

import (
	"errors"

	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

// ProjectLegacy retains only closed lifecycle/accounting metadata. It cannot
// infer measured use, authenticated provenance or deletion from legacy output.
func ProjectLegacy(m Manifest, binding Binding, cleanupID string, source LegacyReceipt) (Result, error) {
	if validateManifest(m) != nil || validateBinding(m, binding) != nil || ValidateLegacy(source) != nil || !provider.ValidID(cleanupID) || source.RequestID != binding.AttemptID || source.RequestID != binding.ReservationID || source.ProjectID != binding.ProjectID || source.Image != binding.Image {
		return Result{}, errors.New("LEGACY_BINDING_MISMATCH")
	}
	o := Observation{SourceKind: "legacy_projection"}
	deletion := "unknown"
	if !source.Admitted {
		deletion = "not_applicable"
	} else if source.CleanupVerified {
		deletion = "reported_complete"
	}
	c := CleanupReceipt{
		Schema: CleanupSchema, ReceiptID: cleanupID, Binding: binding, Observation: o,
		Method: CleanupMethod{"legacy_provider_assertion", 1}, Termination: "unknown", AccessRevocation: "unknown",
		ObservedScopes: []ObservedScope{
			{"execution_resource", binding.AttemptID, deletion},
			{"temporary_data", binding.AssessmentID, "unknown"},
			{"recovery_copies", binding.AssessmentID, "unknown"},
		},
		Outcome: "unresolved", Quarantine: true, ReuseDenied: true,
	}
	c.RemainingLiabilities = cleanupLiabilities(c, source.UnresolvedLiability)
	cleanupHash, e := hashCanonical(c)
	if e != nil {
		return Result{}, e
	}
	failure := "none"
	if !source.Admitted {
		failure = "not_admitted"
	} else if source.Status == "failed" {
		failure = "execution_failed"
	} else if oneOf(source.Status, "cancelled", "unknown") {
		failure = source.Status
	}
	metrics := make(map[string]Metric, len(metricUnits))
	for name, unit := range metricUnits {
		metrics[name] = Metric{Availability: "unsupported", Unit: unit, Scope: "whole_attempt", Source: "none"}
	}
	x := Execution{Status: source.Status, Admitted: source.Admitted, ExecutionStarted: source.ExecutionStarted, CancelRequested: source.CancelRequested, ExitCode: source.ExitCode, FailureCategory: failure, DiagnosticsAvailable: source.OutputError == ""}
	if source.Consumer != "" {
		x.Reused = &source.Reused
	}
	r := MeasurementReceipt{
		Schema: MeasurementSchema, Binding: binding, Observation: o, Execution: x, MeasuredConsumption: metrics,
		ReservationCeiling:     ReservationCeiling{source.Compute.Reserved, "legacy_broker_receipt"},
		ConservativeAccounting: Accounting{source.Compute.Charged, source.Compute.Provenance, source.UnresolvedLiability},
		ProviderReportedCharge: ProviderCharge{Availability: "unknown", Unit: "unknown", Source: "none"},
		Quality:                Quality{"unknown", "none"}, CleanupReceiptID: cleanupID, CleanupReceiptSHA256: cleanupHash,
	}
	return Validate(m, ReceiptSet{SetSchema, r, c})
}
