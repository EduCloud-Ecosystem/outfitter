package assessment

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func vector(t *testing.T, name string) []byte {
	t.Helper()
	raw, e := os.ReadFile("testdata/" + name + ".json")
	if e != nil {
		t.Fatal(e)
	}
	return raw
}

func shared(t *testing.T) (Manifest, ReceiptSet) {
	t.Helper()
	m, e := DecodeManifest(vector(t, "benchmark-manifest"))
	if e != nil {
		t.Fatal(e)
	}
	s, e := DecodeReceipts(vector(t, "receipt-set"))
	if e != nil {
		t.Fatal(e)
	}
	return m, s
}

func rehash(t *testing.T, s *ReceiptSet) {
	t.Helper()
	h, e := hashCanonical(s.Cleanup)
	if e != nil {
		t.Fatal(e)
	}
	s.Measurement.CleanupReceiptSHA256 = h
}

func invoke(t *testing.T, request any) (Result, error) {
	t.Helper()
	var out bytes.Buffer
	e := Serve(bytes.NewReader(encoded(t, request)), &out)
	var result Result
	if e == nil {
		if e := json.Unmarshal(out.Bytes(), &result); e != nil {
			t.Fatal(e)
		}
	} else if out.Len() != 0 || len(e.Error()) > 64 || strings.Contains(e.Error(), "SECRET") {
		t.Fatal("input leaked or partial success emitted", e)
	}
	return result, e
}

func validateRequest(m Manifest, s ReceiptSet) any {
	return map[string]any{"operation": "validate", "manifest": m, "receipts": s}
}

func boundLegacy(t *testing.T, source LegacyReceipt) (Manifest, Binding) {
	t.Helper()
	m, s := shared(t)
	m.ProjectID, m.Workload.Image = source.ProjectID, source.Image
	m.Reservations[0].AttemptID, m.Reservations[0].ReservationID = source.RequestID, source.RequestID
	b := s.Measurement.Binding
	b.ProjectID, b.Image, b.AttemptID, b.ReservationID = source.ProjectID, source.Image, source.RequestID, source.RequestID
	b.ManifestSHA256, _ = hashCanonical(m)
	return m, b
}

func projectRequest(t *testing.T, source any) any {
	t.Helper()
	r, e := DecodeLegacy(encoded(t, source))
	if e != nil {
		t.Fatal(e)
	}
	m, b := boundLegacy(t, r)
	return map[string]any{"operation": "project_legacy", "manifest": m, "binding": b, "cleanup_receipt_id": "cleanup-1", "source_receipt": source}
}

func TestFrozenCrossLanguageVector(t *testing.T) {
	m, s := shared(t)
	result, e := Validate(m, s)
	if e != nil {
		t.Fatal(e)
	}
	for name, value := range map[string]any{"benchmark-manifest": m, "cleanup-receipt": s.Cleanup, "measurement-receipt": s.Measurement, "receipt-set": s, "validate-result": result} {
		raw, e := Canonical(value)
		if e != nil || !bytes.Equal(raw, vector(t, name+".canonical")) {
			t.Fatalf("canonical drift: %s %v", name, e)
		}
	}
	var out bytes.Buffer
	if e := Serve(bytes.NewReader(vector(t, "validate-request")), &out); e != nil || !bytes.Equal(out.Bytes(), vector(t, "validate-result.canonical")) {
		t.Fatal("CLI canonical drift", e)
	}
	if result.ExecutionAuthorized || result.CleanupTrusted || result.ActualResourceAccepted {
		t.Fatal("unverified source promoted")
	}
	if result.ManifestSHA256 != "506a3587d73e70c1d545631ea2dd455056db92570e394230d5f56d81a7738420" || result.CleanupSHA256 != "e0e747be8a561293893585aec0dbc48e36d90903cf14c34af7160999959e9cf5" || result.MeasurementSHA256 != "36805e95c8852613a944509c98d1ab5c4a6cba1b842c489a020ac85fc586a52d" {
		t.Fatal("digest drift")
	}
}

// Failed controls are intentionally retained: before the validation fix these
// crafted untrusted claims passed the real command handler.
func TestUntrustedVerifiedCleanupRejected(t *testing.T) {
	for _, source := range []string{"unverified_observation", "synthetic_fixture"} {
		t.Run(source, func(t *testing.T) {
			m, s := shared(t)
			s.Measurement.Observation.SourceKind = source
			s.Cleanup.Observation.SourceKind = source
			s.Cleanup.Method.Name = "unverified_scope_observation"
			s.Cleanup.Termination = "verified"
			s.Cleanup.AccessRevocation = "verified"
			for i := range s.Cleanup.ObservedScopes {
				s.Cleanup.ObservedScopes[i].Deletion = "reported_complete"
			}
			time, verifier := "2026-10-19T12:00:00Z", "untrusted-verifier"
			s.Cleanup.ObservedAt, s.Cleanup.VerifierID = &time, &verifier
			s.Cleanup.RemainingLiabilities = []string{}
			s.Cleanup.Outcome = "verified"
			rehash(t, &s)
			if _, e := invoke(t, validateRequest(m, s)); e == nil {
				t.Fatal("untrusted verified-cleanup claim accepted")
			}
		})
	}
}

func TestActiveCompletedContradictionsRejected(t *testing.T) {
	for _, status := range []string{"reserved", "creating", "running"} {
		t.Run(status, func(t *testing.T) {
			source := successfulReceipt(t)
			source.Status = status
			source.UnresolvedLiability = true
			source.Compute.Charged = 0
			source.Compute.Provenance = "reserved"
			m, b := boundLegacy(t, LegacyReceipt{Receipt: source})
			request := map[string]any{"operation": "project_legacy", "manifest": m, "binding": b, "cleanup_receipt_id": "cleanup-1", "source_receipt": source}
			if _, e := invoke(t, request); e == nil {
				t.Error("completed legacy receipt accepted as active")
			}
			m, s := shared(t)
			s.Measurement.Execution.Status = status
			s.Measurement.ConservativeAccounting = Accounting{0, "reserved", true}
			s.Cleanup.RemainingLiabilities = cleanupLiabilities(s.Cleanup, true)
			rehash(t, &s)
			if _, e := invoke(t, validateRequest(m, s)); e == nil {
				t.Error("completed measurement accepted as active")
			}
		})
	}
}

func TestReceiptBindingsAndClosedSemantics(t *testing.T) {
	changes := map[string]func(*ReceiptSet){
		"manifest":       func(s *ReceiptSet) { s.Measurement.Binding.ManifestSHA256 = strings.Repeat("a", 64) },
		"assessment":     func(s *ReceiptSet) { s.Measurement.Binding.AssessmentID = "different" },
		"project":        func(s *ReceiptSet) { s.Measurement.Binding.ProjectID = "different" },
		"attempt":        func(s *ReceiptSet) { s.Measurement.Binding.AttemptID = "different" },
		"reservation":    func(s *ReceiptSet) { s.Measurement.Binding.ReservationID = "different" },
		"stage":          func(s *ReceiptSet) { s.Measurement.Binding.StageID = "different" },
		"environment":    func(s *ReceiptSet) { s.Measurement.Binding.EnvironmentID = "different" },
		"image":          func(s *ReceiptSet) { s.Measurement.Binding.Image = "python@sha256:" + strings.Repeat("a", 64) },
		"hardware":       func(s *ReceiptSet) { s.Measurement.Binding.Hardware.CPUCount++ },
		"method":         func(s *ReceiptSet) { s.Measurement.Binding.MeasurementMethod.Version++ },
		"role":           func(s *ReceiptSet) { s.Measurement.Binding.SampleRole = "held_out" },
		"size":           func(s *ReceiptSet) { s.Measurement.Binding.SampleSize++ },
		"repetition":     func(s *ReceiptSet) { s.Measurement.Binding.Repetition++ },
		"cleanup-id":     func(s *ReceiptSet) { s.Measurement.CleanupReceiptID = "different" },
		"authenticated":  func(s *ReceiptSet) { s.Measurement.Observation.Authenticated = true },
		"independent":    func(s *ReceiptSet) { s.Cleanup.Observation.IndependentlyVerified = true },
		"quarantine":     func(s *ReceiptSet) { s.Cleanup.Quarantine = false },
		"reuse":          func(s *ReceiptSet) { s.Cleanup.ReuseDenied = false },
		"omit-liability": func(s *ReceiptSet) { s.Cleanup.RemainingLiabilities = s.Cleanup.RemainingLiabilities[1:] },
		"metric-missing": func(s *ReceiptSet) { delete(s.Measurement.MeasuredConsumption, "cpu_consumed") },
		"metric-extra":   func(s *ReceiptSet) { s.Measurement.MeasuredConsumption["secret"] = Metric{} },
		"unit": func(s *ReceiptSet) {
			v := s.Measurement.MeasuredConsumption["cpu_consumed"]
			v.Unit = "cpu_seconds"
			s.Measurement.MeasuredConsumption["cpu_consumed"] = v
		},
		"unsupported-zero": func(s *ReceiptSet) {
			v := s.Measurement.MeasuredConsumption["cpu_consumed"]
			z := int64(0)
			v.Value = &z
			s.Measurement.MeasuredConsumption["cpu_consumed"] = v
		},
		"provider-zero": func(s *ReceiptSet) { z := int64(0); s.Measurement.ProviderReportedCharge.Value = &z },
		"undercharge":   func(s *ReceiptSet) { s.Measurement.ConservativeAccounting.ChargedCPUSeconds-- },
		"legacy-measured": func(s *ReceiptSet) {
			v := s.Measurement.MeasuredConsumption["cpu_consumed"]
			z := int64(30)
			v.Value = &z
			v.Availability = "available"
			v.Source = "unverified_collector"
			s.Measurement.MeasuredConsumption["cpu_consumed"] = v
		},
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			m, s := shared(t)
			change(&s)
			// Match both receipt bindings to ensure the manifest check, not merely
			// equality between the two receipts, rejects a substituted identity.
			s.Cleanup.Binding = s.Measurement.Binding
			rehash(t, &s)
			if _, e := invoke(t, validateRequest(m, s)); e == nil {
				t.Fatal("contradiction accepted")
			}
		})
	}
	m, s := shared(t)
	s.Measurement.CleanupReceiptSHA256 = strings.Repeat("a", 64)
	if _, e := invoke(t, validateRequest(m, s)); e == nil {
		t.Fatal("modified cleanup digest accepted")
	}
}

func TestPartialCleanupAndUnknownCountersRemainUntrusted(t *testing.T) {
	m, s := shared(t)
	s.Measurement.Observation.SourceKind = "unverified_observation"
	s.Cleanup.Observation.SourceKind = "unverified_observation"
	s.Cleanup.Method.Name = "unverified_scope_observation"
	s.Cleanup.Termination = "failed"
	s.Cleanup.AccessRevocation = "failed"
	s.Cleanup.ObservedScopes[1].Deletion = "failed"
	s.Cleanup.RemainingLiabilities = cleanupLiabilities(s.Cleanup, false)
	v := s.Measurement.MeasuredConsumption["cpu_consumed"]
	v.Availability = "unknown"
	s.Measurement.MeasuredConsumption["cpu_consumed"] = v
	prior := strings.Repeat("b", 64)
	s.Cleanup.PriorReceiptSHA256 = &prior
	rehash(t, &s)
	before := encoded(t, s)
	result, e := invoke(t, validateRequest(m, s))
	if e != nil || !bytes.Equal(before, encoded(t, result.Receipts)) || !bytes.Equal(before, encoded(t, s)) || result.CleanupTrusted {
		t.Fatal("failed observation overwritten or trusted", e)
	}
	v.Availability = "available"
	n := int64(9)
	v.Value = &n
	v.Source = "unverified_collector"
	s.Measurement.MeasuredConsumption["cpu_consumed"] = v
	result, e = invoke(t, validateRequest(m, s))
	if e != nil || result.ActualResourceAccepted {
		t.Fatal("supplied counter incorrectly accepted as actual", e)
	}
}

func TestReservationPreparationDoesNotAuthorizeReceipts(t *testing.T) {
	m, s := shared(t)
	m.Reservations = []ReservationBinding{}
	if _, e := DecodeManifest(encoded(t, m)); e != nil {
		t.Fatal("empty preparation refused", e)
	}
	s.Measurement.Binding.ManifestSHA256, _ = hashCanonical(m)
	s.Cleanup.Binding = s.Measurement.Binding
	rehash(t, &s)
	if _, e := invoke(t, validateRequest(m, s)); e == nil {
		t.Fatal("undeclared attempt accepted")
	}
	for _, kind := range []string{"attempt", "reservation", "slot", "size", "repetition"} {
		t.Run(kind, func(t *testing.T) {
			m, _ := shared(t)
			r := m.Reservations[0]
			switch kind {
			case "attempt":
				r.ReservationID = "other"
				r.SampleSize = 20
			case "reservation":
				r.AttemptID = "other"
				r.SampleSize = 20
			case "slot":
				r.AttemptID = "other"
				r.ReservationID = "other"
			case "size":
				r.AttemptID = "other"
				r.ReservationID = "other"
				r.SampleSize = 15
			case "repetition":
				r.AttemptID = "other"
				r.ReservationID = "other"
				r.Repetition = 4
			}
			m.Reservations = append(m.Reservations, r)
			if _, e := DecodeManifest(encoded(t, m)); e == nil {
				t.Fatal("invalid reservation accepted")
			}
		})
	}
}

func TestStrictCLIJSONAndNoRawDiagnostics(t *testing.T) {
	raw := vector(t, "validate-request.canonical")
	attacks := [][]byte{
		append([]byte(`{"secret":"SECRET",`), raw[1:]...),
		append([]byte(`{"operation":"validate",`), raw[1:]...),
		append(append([]byte{}, raw...), []byte(` {"secret":"SECRET"}`)...),
		bytes.ReplaceAll(raw, []byte(`"admitted":true`), []byte(`"Admitted":true`)),
		bytes.ReplaceAll(raw, []byte(`"admitted":true`), []byte(`"admitted":null`)),
		bytes.ReplaceAll(raw, []byte(`"value":null`), []byte(`"value":0`)),
		[]byte(strings.Repeat(" ", MaxRequestBytes+1)),
	}
	for _, value := range []string{"null", "true", "1.0", "1e0", "NaN", "Infinity", "-1", "9007199254740992", `"SECRET"`} {
		attacks = append(attacks, bytes.ReplaceAll(raw, []byte(`"version":1`), []byte(`"version":`+value)))
	}
	for i, payload := range attacks {
		var out bytes.Buffer
		e := Serve(bytes.NewReader(payload), &out)
		if e == nil || out.Len() != 0 || len(e.Error()) > 64 || strings.Contains(e.Error(), "SECRET") {
			t.Fatalf("attack %d accepted/leaked: %v", i, e)
		}
	}
}
