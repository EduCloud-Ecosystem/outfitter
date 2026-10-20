package assessment

import (
	"errors"
	"slices"

	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

func validObservationV2(o Observation) bool {
	return oneOf(o.SourceKind, "unverified_observation", "synthetic_fixture") && !o.Authenticated && !o.IndependentlyVerified
}

func DecodeReceiptsV2(raw []byte) (ReceiptSetV2, error) {
	var s ReceiptSetV2
	if decodeClosed(raw, &s, MaxReceiptBytes) != nil {
		return ReceiptSetV2{}, errors.New("INVALID_RECEIPT_SET")
	}
	return s, nil
}

// ValidateV2 is a pure structural check. Every imported statement stays
// unverified; no reservation, measurement authority, or cleanup trust is made.
func ValidateV2(m ManifestV2, s ReceiptSetV2) (ResultV2, error) {
	invalid := errors.New("INVALID_RECEIPT_SET")
	if validateManifestV2(m) != nil || s.Schema != SetSchemaV2 || s.Measurement.Schema != MeasurementSchemaV2 || s.Cleanup.Schema != CleanupSchemaV2 || validateBindingV2(m, s.Measurement.Binding) != nil || s.Measurement.Binding != s.Cleanup.Binding || !validObservationV2(s.Measurement.Observation) || !validObservationV2(s.Cleanup.Observation) {
		return ResultV2{}, invalid
	}
	r, c := s.Measurement, s.Cleanup
	if !provider.ValidID(c.ReceiptID) || r.CleanupReceiptID != c.ReceiptID || validateCleanupV2(c, r.ConservativeAccounting.UnresolvedLiability) != nil || validateMeasurementV2(m, r, c) != nil {
		return ResultV2{}, invalid
	}
	cleanupHash, e := hashCanonical(c)
	if e != nil || r.CleanupReceiptSHA256 != cleanupHash {
		return ResultV2{}, errors.New("CLEANUP_DIGEST_MISMATCH")
	}
	measurementHash, e := hashCanonical(r)
	if e != nil {
		return ResultV2{}, invalid
	}
	manifestHash, e := hashCanonical(m)
	if e != nil {
		return ResultV2{}, invalid
	}
	result := ResultV2{Schema: ResultSchemaV2, Validation: "valid_unverified", ManifestSHA256: manifestHash, MeasurementSHA256: measurementHash, CleanupSHA256: cleanupHash, Receipts: s}
	raw, e := Canonical(result)
	if e != nil || len(raw) > MaxOutputBytes {
		return ResultV2{}, errors.New("RECEIPT_OUTPUT_LIMIT")
	}
	return result, nil
}

func validateMetricsV2(metrics map[string]Metric) bool {
	if len(metrics) != len(metricUnits) {
		return false
	}
	for name, unit := range metricUnits {
		v, ok := metrics[name]
		if !ok || v.Unit != unit {
			return false
		}
		switch name {
		case "cpu_consumed":
			if !oneOf(v.Scope, "process", "sandbox") {
				return false
			}
		case "execution_duration":
			if !oneOf(v.Scope, "execution_only", "whole_attempt") {
				return false
			}
		default:
			if !oneOf(v.Scope, "whole_attempt", "process", "sandbox") {
				return false
			}
		}
		if oneOf(name, "gpu_allocated_duration", "gpu_peak_memory", "gpu_utilization") && v.Availability != "unsupported" {
			return false
		}
		switch v.Availability {
		case "available":
			if v.Value == nil || !between(*v.Value, 0, 9007199254740991) || v.Source != "unverified_collector" {
				return false
			}
		case "unknown", "unsupported":
			if v.Value != nil || v.Source != "none" {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func validateMeasurementV2(m ManifestV2, r MeasurementReceiptV2, c CleanupReceiptV2) error {
	invalid := errors.New("INVALID_MEASUREMENT")
	p := r.ProviderReportedCharge
	if !validateMetricsV2(r.MeasuredConsumption) || !oneOf(p.Availability, "unknown", "unsupported") || p.Value != nil || p.Unit != "unknown" || p.Source != "none" || r.ReservationCeiling.Source != "unverified_profile_reservation" {
		return invalid
	}
	x, a, reserved := r.Execution, r.ConservativeAccounting, r.ReservationCeiling.CPUSeconds
	active := oneOf(x.Status, "reserved", "creating", "running")
	if !active && !oneOf(x.Status, "succeeded", "failed", "cancelled", "unknown") || x.ExitCode != nil && !between(int64(*x.ExitCode), 0, 255) {
		return invalid
	}
	if !x.Admitted {
		if x.Status != "failed" || x.ExecutionStarted || x.CancelRequested || x.ExitCode != nil || reserved != 0 || a.ChargedCPUSeconds != 0 || a.Provenance != "not_admitted" || a.UnresolvedLiability || x.FailureCategory != "not_admitted" || c.ObservedScopes[0].Deletion != "not_applicable" {
			return invalid
		}
	} else {
		if reserved != m.Ceilings.CPUCountPerAttempt*m.Ceilings.TimeoutSecondsPerAttempt || !between(a.ChargedCPUSeconds, 0, reserved) || c.ObservedScopes[0].Deletion == "not_applicable" {
			return invalid
		}
		if active && (x.ExitCode != nil || c.ObservedScopes[0].Deletion == "reported_complete") || oneOf(x.Status, "reserved", "creating") && x.ExecutionStarted || x.ExitCode != nil && !x.ExecutionStarted {
			return invalid
		}
		if a.UnresolvedLiability {
			if a.ChargedCPUSeconds != 0 || a.Provenance != "reserved" {
				return invalid
			}
		} else if active || a.ChargedCPUSeconds != reserved || a.Provenance != "estimated_upper_bound" || c.ObservedScopes[0].Deletion != "reported_complete" {
			return invalid
		}
		if x.Status == "succeeded" && (!x.ExecutionStarted || x.ExitCode == nil || *x.ExitCode != 0 || a.UnresolvedLiability || x.CancelRequested) || x.Status == "running" && !x.ExecutionStarted {
			return invalid
		}
		if active || x.Status == "succeeded" {
			if x.FailureCategory != "none" {
				return invalid
			}
		} else if x.Status == "failed" {
			if !oneOf(x.FailureCategory, "execution_failed", "timeout", "out_of_memory") {
				return invalid
			}
		} else if x.FailureCategory != x.Status {
			return invalid
		}
	}
	if !oneOf(r.Quality.Status, "unknown", "passed", "failed") || r.Quality.Status == "unknown" && r.Quality.Source != "none" || r.Quality.Status != "unknown" && r.Quality.Source != "unverified_checker" {
		return invalid
	}
	return nil
}

func cleanupLiabilitiesV2(c CleanupReceiptV2, accounting bool) []string {
	// V2 cannot attest termination or access revocation at all.
	liabilities := []string{"termination_unresolved", "access_revocation_unknown"}
	for i, name := range []string{"execution_cleanup_unknown", "data_cleanup_unknown", "recovery_cleanup_unknown"} {
		if !oneOf(c.ObservedScopes[i].Deletion, "reported_complete", "not_applicable") {
			liabilities = append(liabilities, name)
		}
	}
	if accounting {
		liabilities = append(liabilities, "accounting_unresolved")
	}
	return liabilities
}

func validateCleanupV2(c CleanupReceiptV2, accounting bool) error {
	invalid := errors.New("INVALID_CLEANUP")
	if len(c.ObservedScopes) != 3 || !c.Quarantine || !c.ReuseDenied || !oneOf(c.Termination, "unknown", "failed") || !oneOf(c.AccessRevocation, "unknown", "failed") || c.VerifierID != nil || c.Method != (CleanupMethod{"unverified_scope_observation", 1}) {
		return invalid
	}
	for i, kind := range []string{"execution_resource", "temporary_data", "recovery_copies"} {
		scope := c.ObservedScopes[i]
		id := c.Binding.AssessmentID
		if i == 0 {
			id = c.Binding.AttemptID
		}
		if scope.Kind != kind || scope.ScopeID != id || !oneOf(scope.Deletion, "reported_complete", "unknown", "failed", "not_applicable") || i != 0 && scope.Deletion == "not_applicable" {
			return invalid
		}
	}
	if c.ObservedAt != nil && !timestamp(*c.ObservedAt) || c.PriorReceiptSHA256 != nil && !digest(*c.PriorReceiptSHA256) || c.Outcome != "unresolved" || c.RemainingLiabilities == nil || !slices.Equal(c.RemainingLiabilities, cleanupLiabilitiesV2(c, accounting)) {
		return invalid
	}
	return nil
}
