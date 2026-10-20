package assessmentexec

import "github.com/EduCloud-Ecosystem/outfitter/internal/assessment"

// Project uses the accepted v2 structural validator. The result is explicitly
// unverified and cannot settle a journal hold or attest data deletion. Workload
// metrics retain their original measurement scopes; checker/whole-attempt
// scopes remain separately visible in the bounded execution result.
func Project(s ExecutionScope, a AssessmentAttempt) (assessment.ReceiptSetV2, error) {
	if a.Validate(s) != nil {
		return assessment.ReceiptSetV2{}, ErrInvalid
	}
	b := assessment.BindingV2{Binding: assessment.Binding{ManifestSHA256: s.ManifestSHA256, AssessmentID: s.AssessmentID, ProjectID: s.ProjectID, AttemptID: a.Slot.AttemptID, StageID: a.Slot.StageID, ReservationID: a.Slot.ReservationID, EnvironmentID: s.Capability.Workload.EnvironmentID, Image: s.Capability.Workload.Image, Hardware: s.Manifest.Hardware, MeasurementMethod: s.Manifest.Measurement, SampleRole: a.Slot.SampleRole, SampleSize: a.Slot.SampleSize, Repetition: a.Slot.Repetition}, ManifestSchema: s.ManifestSchema, ExecutionProfile: s.ExecutionProfile}
	o := assessment.Observation{SourceKind: "synthetic_fixture"}
	deletion := "unknown"
	if !a.AccountingUnresolved {
		deletion = "reported_complete"
	}
	c := assessment.CleanupReceiptV2{Schema: assessment.CleanupSchemaV2, ReceiptID: a.ClaimEpoch, Binding: b, Observation: o, Method: assessment.CleanupMethod{Name: "unverified_scope_observation", Version: 1}, Termination: "unknown", AccessRevocation: "unknown", ObservedScopes: []assessment.ObservedScope{{Kind: "execution_resource", ScopeID: a.Slot.AttemptID, Deletion: deletion}, {Kind: "temporary_data", ScopeID: s.AssessmentID, Deletion: "unknown"}, {Kind: "recovery_copies", ScopeID: s.AssessmentID, Deletion: "unknown"}}, Outcome: "unresolved", Quarantine: true, ReuseDenied: true, RemainingLiabilities: []string{"termination_unresolved", "access_revocation_unknown"}}
	if a.AccountingUnresolved {
		c.RemainingLiabilities = append(c.RemainingLiabilities, "execution_cleanup_unknown")
	}
	c.RemainingLiabilities = append(c.RemainingLiabilities, "data_cleanup_unknown", "recovery_cleanup_unknown")
	if a.AccountingUnresolved {
		c.RemainingLiabilities = append(c.RemainingLiabilities, "accounting_unresolved")
	}
	status, failure := "unknown", "unknown"
	switch a.State {
	case "claimed", "preparing":
		status = "reserved"
		failure = "none"
	case "running", "checking":
		status = "running"
		failure = "none"
	case "terminal":
		status = a.Outcome
		if status == "succeeded" {
			failure = "none"
		} else if status == "failed" {
			failure = "execution_failed"
		} else {
			failure = status
		}
	}
	started := false
	var exit *int
	if p := a.PhaseRecords.Workload; p != nil {
		started = p.StartedObserved
		if p.ExitCode != nil {
			v := int(*p.ExitCode)
			exit = &v
		}
	}
	if status == "running" && !started {
		status = "creating"
	}
	if status == "reserved" || status == "creating" {
		exit = nil
	}
	r := assessment.MeasurementReceiptV2{Schema: assessment.MeasurementSchemaV2, Binding: b, Observation: o, Execution: assessment.Execution{Status: status, Admitted: true, ExecutionStarted: started, CancelRequested: a.FailureCode == "cancelled", ExitCode: exit, FailureCategory: failure, DiagnosticsAvailable: false}, MeasuredConsumption: a.Metrics.Workload.Map(), ReservationCeiling: assessment.ReservationCeiling{CPUSeconds: a.Compute.ReservedCPUSeconds, Source: "unverified_profile_reservation"}, ConservativeAccounting: assessment.Accounting{ChargedCPUSeconds: a.Compute.ChargedCPUSeconds, Provenance: a.Compute.Provenance, UnresolvedLiability: a.AccountingUnresolved}, ProviderReportedCharge: assessment.ProviderCharge{Availability: "unknown", Unit: "unknown", Source: "none"}, Quality: assessment.Quality{Status: a.Quality.Status, Source: a.Quality.Source}, CleanupReceiptID: c.ReceiptID, CleanupReceiptSHA256: Digest(c)}
	set := assessment.ReceiptSetV2{Schema: assessment.SetSchemaV2, Measurement: r, Cleanup: c}
	if _, e := assessment.ValidateV2(s.Manifest, set); e != nil {
		return assessment.ReceiptSetV2{}, ErrInvalid
	}
	return set, nil
}
