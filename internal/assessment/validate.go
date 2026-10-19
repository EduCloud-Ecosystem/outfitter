package assessment

import (
	"errors"
	"slices"

	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

var metricUnits = map[string]string{
	"cpu_consumed": "nanoseconds", "gpu_allocated_duration": "nanoseconds",
	"setup_duration": "nanoseconds", "loading_duration": "nanoseconds", "transfer_duration": "nanoseconds",
	"queue_duration": "nanoseconds", "execution_duration": "nanoseconds", "cleanup_duration": "nanoseconds",
	"sandbox_overhead": "nanoseconds", "instrumentation_overhead": "nanoseconds",
	"peak_memory": "bytes", "scratch_peak": "bytes", "output_peak": "bytes", "io_read": "bytes", "io_write": "bytes", "gpu_peak_memory": "bytes",
	"gpu_utilization": "parts_per_million", "completed_units": "count",
}

func validObservation(o Observation) bool {
	return oneOf(o.SourceKind, "legacy_projection", "unverified_observation", "synthetic_fixture") && !o.Authenticated && !o.IndependentlyVerified
}

func DecodeReceipts(raw []byte) (ReceiptSet, error) {
	var s ReceiptSet
	if decodeClosed(raw, &s, MaxReceiptBytes) != nil {
		return ReceiptSet{}, errors.New("INVALID_RECEIPT_SET")
	}
	return s, nil
}

// Validate never upgrades an imported claim into execution or cleanup authority.
func Validate(m Manifest, s ReceiptSet) (Result, error) {
	invalid := errors.New("INVALID_RECEIPT_SET")
	if validateManifest(m) != nil || s.Schema != SetSchema || s.Measurement.Schema != MeasurementSchema || s.Cleanup.Schema != CleanupSchema || validateBinding(m, s.Measurement.Binding) != nil || s.Measurement.Binding != s.Cleanup.Binding || !validObservation(s.Measurement.Observation) || !validObservation(s.Cleanup.Observation) {
		return Result{}, invalid
	}
	r, c := s.Measurement, s.Cleanup
	if !provider.ValidID(c.ReceiptID) || r.CleanupReceiptID != c.ReceiptID || validateCleanup(c, r.ConservativeAccounting.UnresolvedLiability) != nil || validateMeasurement(r, c) != nil {
		return Result{}, invalid
	}
	cleanupHash, e := hashCanonical(c)
	if e != nil || r.CleanupReceiptSHA256 != cleanupHash {
		return Result{}, errors.New("CLEANUP_DIGEST_MISMATCH")
	}
	measurementHash, e := hashCanonical(r)
	if e != nil {
		return Result{}, invalid
	}
	manifestHash, e := hashCanonical(m)
	if e != nil {
		return Result{}, invalid
	}
	result := Result{Schema: ResultSchema, Validation: "valid_unverified", ManifestSHA256: manifestHash, MeasurementSHA256: measurementHash, CleanupSHA256: cleanupHash, Receipts: s}
	raw, e := Canonical(result)
	if e != nil || len(raw) > MaxOutputBytes {
		return Result{}, errors.New("RECEIPT_OUTPUT_LIMIT")
	}
	return result, nil
}

func validateMeasurement(r MeasurementReceipt, c CleanupReceipt) error {
	invalid := errors.New("INVALID_MEASUREMENT")
	if len(r.MeasuredConsumption) != len(metricUnits) {
		return invalid
	}
	for name, unit := range metricUnits {
		v, ok := r.MeasuredConsumption[name]
		if !ok || v.Unit != unit || !oneOf(v.Scope, "whole_attempt", "process", "sandbox") {
			return invalid
		}
		switch v.Availability {
		case "available":
			if v.Value == nil || !between(*v.Value, 0, 9007199254740991) || v.Source != "unverified_collector" || name == "gpu_utilization" && *v.Value > 1000000 {
				return invalid
			}
		case "unknown", "unsupported":
			if v.Value != nil || v.Source != "none" {
				return invalid
			}
		default:
			return invalid
		}
		if r.Observation.SourceKind == "legacy_projection" && (v.Availability != "unsupported" || v.Scope != "whole_attempt") {
			return invalid
		}
	}
	p := r.ProviderReportedCharge
	if !oneOf(p.Availability, "unknown", "unsupported") || p.Value != nil || p.Unit != "unknown" || p.Source != "none" || r.ReservationCeiling.Source != "legacy_broker_receipt" {
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
		if reserved != 30 || !between(a.ChargedCPUSeconds, 0, reserved) {
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
			if !oneOf(x.FailureCategory, "execution_failed", "timeout", "out_of_memory") || r.Observation.SourceKind == "legacy_projection" && x.FailureCategory != "execution_failed" {
				return invalid
			}
		} else if x.FailureCategory != x.Status {
			return invalid
		}
	}
	if !oneOf(r.Quality.Status, "unknown", "passed", "failed") || (r.Quality.Status == "unknown" && r.Quality.Source != "none") || (r.Quality.Status != "unknown" && r.Quality.Source != "unverified_checker") || r.Observation.SourceKind == "legacy_projection" && (r.Quality.Status != "unknown" || c.Observation.SourceKind != "legacy_projection") {
		return invalid
	}
	return nil
}

func cleanupLiabilities(c CleanupReceipt, accounting bool) []string {
	liabilities := []string{}
	if c.Termination != "verified" {
		liabilities = append(liabilities, "termination_unresolved")
	}
	if c.AccessRevocation != "verified" {
		liabilities = append(liabilities, "access_revocation_unknown")
	}
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

func validateCleanup(c CleanupReceipt, accounting bool) error {
	invalid := errors.New("INVALID_CLEANUP")
	if len(c.ObservedScopes) != 3 || !c.Quarantine || !c.ReuseDenied || !oneOf(c.Termination, "verified", "unknown", "failed") || !oneOf(c.AccessRevocation, "verified", "unknown", "failed") || c.Method.Version != 1 || !oneOf(c.Method.Name, "legacy_provider_assertion", "unverified_scope_observation") {
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
	if c.ObservedAt != nil && !timestamp(*c.ObservedAt) || c.VerifierID != nil && !provider.ValidID(*c.VerifierID) || c.PriorReceiptSHA256 != nil && !digest(*c.PriorReceiptSHA256) {
		return invalid
	}
	expected := cleanupLiabilities(c, accounting)
	if c.RemainingLiabilities == nil || !slices.Equal(c.RemainingLiabilities, expected) {
		return invalid
	}
	// This command has no qualified verifier. Reject an imported claim of
	// complete verification rather than laundering it through a false wrapper.
	if len(expected) == 0 || c.Outcome != "unresolved" {
		return invalid
	}
	if c.Observation.SourceKind == "legacy_projection" && (c.Outcome != "unresolved" || c.Method.Name != "legacy_provider_assertion" || c.Termination != "unknown" || c.AccessRevocation != "unknown" || c.ObservedScopes[1].Deletion != "unknown" || c.ObservedScopes[2].Deletion != "unknown" || c.ObservedAt != nil || c.VerifierID != nil || c.PriorReceiptSHA256 != nil) {
		return invalid
	}
	return nil
}
