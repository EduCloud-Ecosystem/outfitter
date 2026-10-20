package assessment

import (
	"bytes"
	"testing"
)

// Compare accepted neighboring forms with independently authored Python bytes.
// The width argument is explicit in testdata/v2/README.md; numeric maxima alone
// are insufficient (false/null and longer failure strings mattered here).
func TestV2MaximumAcceptedNeighborForms(t *testing.T) {
	check := func(t *testing.T, m ManifestV2, s ReceiptSetV2) {
		t.Helper()
		slot := m.Reservations[len(m.Reservations)-1]
		b := &s.Measurement.Binding
		b.AttemptID, b.ReservationID, b.StageID = slot.AttemptID, slot.ReservationID, slot.StageID
		b.SampleRole, b.SampleSize, b.Repetition = slot.SampleRole, slot.SampleSize, slot.Repetition
		b.ManifestSHA256, _ = hashCanonical(m)
		s.Cleanup.Binding = *b
		s.Cleanup.ObservedScopes[0].ScopeID = b.AttemptID
		s.Cleanup.RemainingLiabilities = cleanupLiabilitiesV2(s.Cleanup, s.Measurement.ConservativeAccounting.UnresolvedLiability)
		rehashV2(t, &s)
		result, e := ValidateV2(m, s)
		if e != nil {
			t.Fatal("neighbor must be valid, not merely shorter", e)
		}
		objects := map[string]any{"benchmark-manifest": m, "cleanup-receipt": s.Cleanup, "measurement-receipt": s.Measurement, "receipt-set": s, "validate-result": result, "validate-request": map[string]any{"operation": "validate_v2", "manifest": m, "receipts": s}}
		for name, object := range objects {
			raw, e := Canonical(object)
			maximum := vector(t, "v2/maximum-"+name+".canonical")
			settled := vector(t, "v2/maximum-settled-"+name+".canonical")
			if len(settled) > len(maximum) {
				maximum = settled
			}
			if e != nil || len(raw) > len(maximum) {
				t.Fatalf("%s exceeds independent maximum: %d > %d (%v)", name, len(raw), len(maximum), e)
			}
		}
	}
	for _, category := range []string{"execution_failed", "out_of_memory", "timeout"} {
		for _, started := range []bool{false, true} {
			for _, code := range []*int{nil, ptrIntV2(0), ptrIntV2(255)} {
				if !started && code != nil {
					continue
				}
				m, s := sharedV2(t, "maximum-")
				s.Measurement.Execution.FailureCategory = category
				s.Measurement.Execution.ExecutionStarted, s.Measurement.Execution.ExitCode = started, code
				check(t, m, s)
			}
		}
	}
	for _, status := range []string{"reserved", "creating", "running", "unknown", "cancelled", "succeeded", "not_admitted", "failed_settled"} {
		t.Run(status, func(t *testing.T) {
			m, s := sharedV2(t, "maximum-")
			x := &s.Measurement.Execution
			x.Status, x.FailureCategory = status, "none"
			switch status {
			case "running":
				x.ExecutionStarted = true
			case "unknown", "cancelled":
				x.FailureCategory = status
			case "succeeded", "failed_settled":
				x.ExecutionStarted, x.ExitCode = true, ptrIntV2(0)
				if status == "failed_settled" {
					x.Status, x.FailureCategory, x.ExitCode = "failed", "execution_failed", ptrIntV2(255)
				}
				s.Measurement.ConservativeAccounting = Accounting{1800, "estimated_upper_bound", false}
				s.Cleanup.ObservedScopes[0].Deletion = "reported_complete"
			case "not_admitted":
				x.Status, x.FailureCategory, x.Admitted = "failed", "not_admitted", false
				s.Measurement.ConservativeAccounting = Accounting{0, "not_admitted", false}
				s.Measurement.ReservationCeiling.CPUSeconds = 0
				s.Cleanup.ObservedScopes[0].Deletion = "not_applicable"
			}
			check(t, m, s)
		})
	}
	for _, deletion := range []string{"reported_complete", "unknown", "failed"} {
		for i := 0; i < 3; i++ {
			m, s := sharedV2(t, "maximum-")
			s.Cleanup.ObservedScopes[i].Deletion = deletion
			check(t, m, s)
		}
	}
	for _, nullable := range []string{"timestamp", "prior", "reused", "quality", "observation", "booleans"} {
		m, s := sharedV2(t, "maximum-")
		switch nullable {
		case "timestamp":
			s.Cleanup.ObservedAt = nil
		case "prior":
			s.Cleanup.PriorReceiptSHA256 = nil
		case "reused":
			s.Measurement.Execution.Reused = nil
		case "quality":
			s.Measurement.Quality = Quality{"unknown", "none"}
		case "observation":
			s.Measurement.Observation.SourceKind, s.Cleanup.Observation.SourceKind = "synthetic_fixture", "synthetic_fixture"
		case "booleans":
			s.Measurement.Execution.CancelRequested, s.Measurement.Execution.DiagnosticsAvailable = true, true
		}
		check(t, m, s)
	}
	for name := range metricUnits {
		for _, availability := range []string{"unknown", "unsupported"} {
			m, s := sharedV2(t, "maximum-")
			if bytes.HasPrefix([]byte(name), []byte("gpu_")) && availability == "unknown" {
				continue
			}
			v := s.Measurement.MeasuredConsumption[name]
			v.Availability, v.Value, v.Source = availability, nil, "none"
			s.Measurement.MeasuredConsumption[name] = v
			check(t, m, s)
		}
	}
	for fits := 1; fits <= 3; fits++ {
		for reps := int64(1); reps <= 3; reps++ {
			m, s := sharedV2(t, "maximum-")
			m.Design.FittingSizes = m.Design.FittingSizes[:fits]
			m.Design.Repetitions, m.Design.Seeds = reps, m.Design.Seeds[:reps]
			if fits != 3 || reps != 3 {
				m.Design.SmallerDesign = &SmallerDesign{true, "allowance_limited"}
			}
			m.Ceilings.MaxAttempts = int64(fits+1) * reps
			m.Ceilings.CPUSeconds = m.Ceilings.MaxAttempts * 1800
			kept := []ReservationBinding{}
			for _, r := range m.Reservations {
				if r.Repetition <= reps && (r.SampleRole == "held_out" || r.SampleSize <= m.Design.FittingSizes[fits-1]) {
					kept = append(kept, r)
				}
			}
			m.Reservations = kept
			check(t, m, s)
		}
	}
}

func ptrIntV2(n int) *int { return &n }
