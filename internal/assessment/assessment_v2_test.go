package assessment

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func sharedV2(t *testing.T, prefix string) (ManifestV2, ReceiptSetV2) {
	t.Helper()
	m, e := DecodeManifestV2(vector(t, "v2/"+prefix+"benchmark-manifest.canonical"))
	if e != nil {
		t.Fatal(e)
	}
	s, e := DecodeReceiptsV2(vector(t, "v2/"+prefix+"receipt-set.canonical"))
	if e != nil {
		t.Fatal(e)
	}
	return m, s
}

func rehashV2(t *testing.T, s *ReceiptSetV2) {
	t.Helper()
	h, e := hashCanonical(s.Cleanup)
	if e != nil {
		t.Fatal(e)
	}
	s.Measurement.CleanupReceiptSHA256 = h
}

func invokeV2(t *testing.T, m ManifestV2, s ReceiptSetV2) (ResultV2, error) {
	t.Helper()
	var out bytes.Buffer
	e := Serve(bytes.NewReader(encoded(t, map[string]any{"operation": "validate_v2", "manifest": m, "receipts": s})), &out)
	var result ResultV2
	if e == nil {
		if e := json.Unmarshal(out.Bytes(), &result); e != nil {
			t.Fatal(e)
		}
	} else if out.Len() != 0 || len(e.Error()) > 64 || strings.Contains(e.Error(), "SECRET") {
		t.Fatal("input leaked or partial success emitted", e)
	}
	return result, e
}

func TestV2IndependentCanonicalVectorsAndBounds(t *testing.T) {
	var digests map[string]string
	var sizes map[string]int
	if e := json.Unmarshal(vector(t, "v2/digests"), &digests); e != nil {
		t.Fatal(e)
	}
	if e := json.Unmarshal(vector(t, "v2/sizes"), &sizes); e != nil {
		t.Fatal(e)
	}
	for name, expected := range digests {
		raw, e := os.ReadFile("testdata/v2/" + name)
		hash := sha256.Sum256(raw)
		if e != nil || hex.EncodeToString(hash[:]) != expected {
			t.Fatal("independent fixture digest drift", name, e)
		}
		if size, ok := sizes[name]; ok && size != len(raw) {
			t.Fatal("independent size drift", name)
		}
	}
	for _, prefix := range []string{"", "maximum-", "maximum-settled-", "unknown-held-", "unknown-settled-"} {
		t.Run(prefix+"canonical", func(t *testing.T) {
			m, s := sharedV2(t, prefix)
			result, e := ValidateV2(m, s)
			if e != nil {
				t.Fatal(e)
			}
			values := map[string]any{"benchmark-manifest": m, "cleanup-receipt": s.Cleanup, "measurement-receipt": s.Measurement, "receipt-set": s, "validate-result": result}
			for name, value := range values {
				raw, e := Canonical(value)
				if e != nil || !bytes.Equal(raw, vector(t, "v2/"+prefix+name+".canonical")) {
					t.Fatal("Python/Go canonical mismatch", name, e)
				}
			}
			request := vector(t, "v2/"+prefix+"validate-request.canonical")
			var out bytes.Buffer
			if e := Serve(bytes.NewReader(request), &out); e != nil || !bytes.Equal(out.Bytes(), vector(t, "v2/"+prefix+"validate-result.canonical")) {
				t.Fatal("CLI canonical mismatch", e)
			}
			if result.ExecutionAuthorized || result.CleanupTrusted || result.ActualResourceAccepted || result.Validation != "valid_unverified" || result.Schema != ResultSchemaV2 {
				t.Fatal("imported metadata promoted")
			}
			if len(vector(t, "v2/"+prefix+"benchmark-manifest.canonical")) >= MaxManifestV2Bytes || len(vector(t, "v2/"+prefix+"receipt-set.canonical")) >= MaxReceiptBytes || out.Len() >= MaxOutputBytes || len(request) >= MaxRequestBytes {
				t.Fatal("maximum metadata exceeds existing wire bounds")
			}
			if strings.HasPrefix(prefix, "maximum-") && (len(m.Reservations) != 12 || len(m.Workload.Image) != 512 || len(m.AssessmentID) != 80 || m.Instruction == nil || s.Cleanup.PriorReceiptSHA256 == nil) {
				t.Fatal("maximum form omitted a maximum-width field or collection")
			}
			t.Logf("scalar-only manifest=%d receipt_set=%d result=%d request=%d bytes", len(vector(t, "v2/"+prefix+"benchmark-manifest.canonical")), len(vector(t, "v2/"+prefix+"receipt-set.canonical")), out.Len(), len(request))
		})
	}
}

func TestV2ManifestLimitsAndDesign(t *testing.T) {
	changes := map[string]func(*ManifestV2){
		"input-plus-one":            func(m *ManifestV2) { m.Ceilings.InputBytes++ },
		"scratch-plus-one":          func(m *ManifestV2) { m.Ceilings.ScratchBytes++ },
		"memory-plus-one":           func(m *ManifestV2) { m.Ceilings.MemoryBytes++ },
		"memory-wrong-five-gb":      func(m *ManifestV2) { m.Ceilings.MemoryBytes = 5000000000 },
		"memory-below-min":          func(m *ManifestV2) { m.Ceilings.MemoryBytes = 33554431 },
		"input-zero":                func(m *ManifestV2) { m.Ceilings.InputBytes = 0 },
		"input-negative":            func(m *ManifestV2) { m.Ceilings.InputBytes = -1 },
		"scratch-below-min":         func(m *ManifestV2) { m.Ceilings.ScratchBytes = 1 },
		"output-plus-one":           func(m *ManifestV2) { m.Ceilings.OutputBytes++ },
		"output-zero":               func(m *ManifestV2) { m.Ceilings.OutputBytes = 0 },
		"timeout-plus-one":          func(m *ManifestV2) { m.Ceilings.TimeoutSecondsPerAttempt++ },
		"timeout-zero":              func(m *ManifestV2) { m.Ceilings.TimeoutSecondsPerAttempt = 0 },
		"rows-plus-one":             func(m *ManifestV2) { m.Ceilings.InputRows++ },
		"rows-zero":                 func(m *ManifestV2) { m.Ceilings.InputRows = 0 },
		"cpu-allocation":            func(m *ManifestV2) { m.Ceilings.CPUCountPerAttempt = 2 },
		"cpu-seconds":               func(m *ManifestV2) { m.Ceilings.CPUSeconds-- },
		"max-attempts":              func(m *ManifestV2) { m.Ceilings.MaxAttempts-- },
		"input-output-over-scratch": func(m *ManifestV2) { m.Ceilings.ScratchBytes = m.Ceilings.InputBytes + m.Ceilings.OutputBytes - 1 },
		"hardware-memory":           func(m *ManifestV2) { m.Hardware.MemoryBytes = m.Ceilings.MemoryBytes - 1 },
		"hardware-cpu":              func(m *ManifestV2) { m.Hardware.CPUCount = 0 },
		"gpu":                       func(m *ManifestV2) { m.Hardware.GPUCount = 1 },
		"profile-id":                func(m *ManifestV2) { m.ExecutionProfile.ID = "different" },
		"profile-version":           func(m *ManifestV2) { m.ExecutionProfile.Version++ },
		"schema-v1":                 func(m *ManifestV2) { m.Schema = "educloud.benchmark-manifest/v1" },
		"rows-above-input":          func(m *ManifestV2) { m.Ceilings.InputRows = m.Design.HeldOutSize - 1 },
		"heldout-reused":            func(m *ManifestV2) { m.Design.HeldOutSize = m.Design.FittingSizes[0] },
		"unordered-fitting": func(m *ManifestV2) {
			m.Design.FittingSizes[0], m.Design.FittingSizes[1] = m.Design.FittingSizes[1], m.Design.FittingSizes[0]
		},
		"duplicate-seeds": func(m *ManifestV2) { m.Design.Seeds[1] = m.Design.Seeds[0] },
		"missing-smaller-design": func(m *ManifestV2) {
			m.Design.FittingSizes = m.Design.FittingSizes[:1]
			m.Ceilings.MaxAttempts = 6
			m.Ceilings.CPUSeconds = 10800
		},
		"duplicate-attempt":     func(m *ManifestV2) { m.Reservations[1].AttemptID = m.Reservations[0].AttemptID },
		"duplicate-reservation": func(m *ManifestV2) { m.Reservations[1].ReservationID = m.Reservations[0].ReservationID },
		"duplicate-slot":        func(m *ManifestV2) { m.Reservations[1].Repetition = m.Reservations[0].Repetition },
		"unknown-slot":          func(m *ManifestV2) { m.Reservations[0].SampleSize = 100 },
		"null-reservations":     func(m *ManifestV2) { m.Reservations = nil },
		"missing-instruction":   func(m *ManifestV2) { m.Instruction = nil },
		"mutable-image":         func(m *ManifestV2) { m.Workload.Image = "python:latest" },
		"extra-stage":           func(m *ManifestV2) { m.Workload.Stages = append(m.Workload.Stages, "other") },
		"dependency":            func(m *ManifestV2) { m.Workload.Dependencies = []string{"SECRET"} },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			m, _ := sharedV2(t, "maximum-")
			change(&m)
			if _, e := DecodeManifestV2(encoded(t, m)); e == nil {
				t.Fatal("invalid manifest accepted independently of receipt hash")
			}
		})
	}
	m, _ := sharedV2(t, "")
	m.Ceilings = CeilingsV2{MaxAttempts: 2, CPUSeconds: 2, CPUCountPerAttempt: 1, TimeoutSecondsPerAttempt: 1, MemoryBytes: 33554432, InputBytes: 1, InputRows: 2, ScratchBytes: 2, OutputBytes: 1}
	m.Design = Design{SizeUnit: "rows", FittingSizes: []int64{1}, HeldOutSize: 2, Repetitions: 1, Seeds: []int64{0}, Sampling: "synthetic_fixture", Exclusions: "none", Skew: "unknown", SmallerDesign: &SmallerDesign{true, "operator_selected"}}
	m.Reservations = []ReservationBinding{}
	if _, e := DecodeManifestV2(encoded(t, m)); e != nil {
		t.Fatal("valid minimum scalar profile with empty preparation refused", e)
	}
	_, s := sharedV2(t, "")
	s.Measurement.Binding.ManifestSHA256, _ = hashCanonical(m)
	s.Cleanup.Binding = s.Measurement.Binding
	rehashV2(t, &s)
	if _, e := invokeV2(t, m, s); e == nil {
		t.Fatal("preparation granted an undeclared receipt slot")
	}
}

func TestV2ReceiptContradictionsWithoutDigestMasking(t *testing.T) {
	changes := map[string]func(*ReceiptSetV2){
		"manifest-digest":           func(s *ReceiptSetV2) { s.Measurement.Binding.ManifestSHA256 = strings.Repeat("0", 64) },
		"manifest-schema":           func(s *ReceiptSetV2) { s.Measurement.Binding.ManifestSchema = "educloud.benchmark-manifest/v1" },
		"binding-profile":           func(s *ReceiptSetV2) { s.Measurement.Binding.ExecutionProfile.Version++ },
		"assessment":                func(s *ReceiptSetV2) { s.Measurement.Binding.AssessmentID = "different" },
		"project":                   func(s *ReceiptSetV2) { s.Measurement.Binding.ProjectID = "different" },
		"attempt":                   func(s *ReceiptSetV2) { s.Measurement.Binding.AttemptID = "different" },
		"reservation":               func(s *ReceiptSetV2) { s.Measurement.Binding.ReservationID = "different" },
		"stage":                     func(s *ReceiptSetV2) { s.Measurement.Binding.StageID = "different" },
		"environment":               func(s *ReceiptSetV2) { s.Measurement.Binding.EnvironmentID = "different" },
		"image":                     func(s *ReceiptSetV2) { s.Measurement.Binding.Image = "sha256:" + strings.Repeat("f", 64) },
		"hardware":                  func(s *ReceiptSetV2) { s.Measurement.Binding.Hardware.CPUCount++ },
		"method":                    func(s *ReceiptSetV2) { s.Measurement.Binding.MeasurementMethod.Version++ },
		"sample-role":               func(s *ReceiptSetV2) { s.Measurement.Binding.SampleRole = "held_out" },
		"sample-size":               func(s *ReceiptSetV2) { s.Measurement.Binding.SampleSize++ },
		"repetition":                func(s *ReceiptSetV2) { s.Measurement.Binding.Repetition++ },
		"receipt-schema":            func(s *ReceiptSetV2) { s.Measurement.Schema = MeasurementSchema },
		"cleanup-schema":            func(s *ReceiptSetV2) { s.Cleanup.Schema = CleanupSchema },
		"set-schema":                func(s *ReceiptSetV2) { s.Schema = SetSchema },
		"authenticated":             func(s *ReceiptSetV2) { s.Measurement.Observation.Authenticated = true },
		"independent":               func(s *ReceiptSetV2) { s.Cleanup.Observation.IndependentlyVerified = true },
		"legacy-observation":        func(s *ReceiptSetV2) { s.Cleanup.Observation.SourceKind = "legacy_projection" },
		"legacy-reservation":        func(s *ReceiptSetV2) { s.Measurement.ReservationCeiling.Source = "legacy_broker_receipt" },
		"aggregate-reservation":     func(s *ReceiptSetV2) { s.Measurement.ReservationCeiling.CPUSeconds = 21600 },
		"charge-drop":               func(s *ReceiptSetV2) { s.Measurement.ConservativeAccounting.ChargedCPUSeconds-- },
		"aggregate-charge":          func(s *ReceiptSetV2) { s.Measurement.ConservativeAccounting.ChargedCPUSeconds = 21600 },
		"active-settled":            func(s *ReceiptSetV2) { s.Measurement.Execution.Status = "running" },
		"success-without-start":     func(s *ReceiptSetV2) { s.Measurement.Execution.ExecutionStarted = false },
		"success-nonzero-exit":      func(s *ReceiptSetV2) { n := 1; s.Measurement.Execution.ExitCode = &n },
		"termination-verified":      func(s *ReceiptSetV2) { s.Cleanup.Termination = "verified" },
		"revocation-verified":       func(s *ReceiptSetV2) { s.Cleanup.AccessRevocation = "verified" },
		"verifier":                  func(s *ReceiptSetV2) { v := "untrusted"; s.Cleanup.VerifierID = &v },
		"outcome-verified":          func(s *ReceiptSetV2) { s.Cleanup.Outcome = "verified" },
		"no-quarantine":             func(s *ReceiptSetV2) { s.Cleanup.Quarantine = false },
		"reuse":                     func(s *ReceiptSetV2) { s.Cleanup.ReuseDenied = false },
		"scope-count":               func(s *ReceiptSetV2) { s.Cleanup.ObservedScopes = s.Cleanup.ObservedScopes[:2] },
		"actual-scope-substitution": func(s *ReceiptSetV2) { s.Cleanup.ObservedScopes[1].ScopeID = "other-owned-scope" },
		"missing-liability":         func(s *ReceiptSetV2) { s.Cleanup.RemainingLiabilities = s.Cleanup.RemainingLiabilities[1:] },
		"cleanup-id":                func(s *ReceiptSetV2) { s.Measurement.CleanupReceiptID = "different" },
		"admitted-not-applicable":   func(s *ReceiptSetV2) { s.Cleanup.ObservedScopes[0].Deletion = "not_applicable" },
		"missing-metric":            func(s *ReceiptSetV2) { delete(s.Measurement.MeasuredConsumption, "cpu_consumed") },
		"extra-metric":              func(s *ReceiptSetV2) { s.Measurement.MeasuredConsumption["secret"] = Metric{} },
		"provider-zero":             func(s *ReceiptSetV2) { n := int64(0); s.Measurement.ProviderReportedCharge.Value = &n },
		"quality-source":            func(s *ReceiptSetV2) { s.Measurement.Quality = Quality{"passed", "verified"} },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			m, s := sharedV2(t, "")
			change(&s)
			s.Cleanup.Binding = s.Measurement.Binding
			rehashV2(t, &s)
			if _, e := invokeV2(t, m, s); e == nil {
				t.Fatal("contradiction accepted with a freshly matching cleanup digest")
			}
		})
	}
	m, s := sharedV2(t, "")
	s.Measurement.CleanupReceiptSHA256 = strings.Repeat("0", 64)
	if _, e := invokeV2(t, m, s); e == nil {
		t.Fatal("wrong cleanup digest accepted")
	}
}

func TestV2LifecycleHoldsAndCharges(t *testing.T) {
	for _, state := range []string{"not_admitted", "reserved", "creating", "running", "unknown", "failed", "out_of_memory", "timeout", "cancelled", "succeeded"} {
		t.Run(state, func(t *testing.T) {
			m, s := sharedV2(t, "")
			x := &s.Measurement.Execution
			x.Status, x.FailureCategory = state, "none"
			settled := oneOf(state, "failed", "out_of_memory", "timeout", "cancelled", "succeeded")
			if !settled {
				x.ExitCode = nil
				x.ExecutionStarted = oneOf(state, "running", "unknown")
				s.Measurement.ConservativeAccounting = Accounting{0, "reserved", true}
				s.Cleanup.ObservedScopes[0].Deletion = "unknown"
			}
			switch state {
			case "not_admitted":
				x.Status, x.Admitted, x.FailureCategory = "failed", false, "not_admitted"
				s.Measurement.ReservationCeiling.CPUSeconds = 0
				s.Measurement.ConservativeAccounting = Accounting{0, "not_admitted", false}
				s.Cleanup.ObservedScopes[0].Deletion = "not_applicable"
			case "unknown", "cancelled":
				x.FailureCategory = state
				x.ExitCode = nil
			case "failed", "out_of_memory", "timeout":
				x.Status, x.FailureCategory = "failed", state
				if state == "failed" {
					x.FailureCategory = "execution_failed"
				}
				n := 137
				x.ExitCode = &n
			}
			s.Cleanup.RemainingLiabilities = cleanupLiabilitiesV2(s.Cleanup, s.Measurement.ConservativeAccounting.UnresolvedLiability)
			rehashV2(t, &s)
			before := encoded(t, s)
			result, e := invokeV2(t, m, s)
			if e != nil || !bytes.Equal(before, encoded(t, result.Receipts)) || !bytes.Equal(before, encoded(t, s)) {
				t.Fatal("lifecycle metadata lost or rejected", e)
			}
			if state == "unknown" {
				if result.Receipts.Measurement.ReservationCeiling.CPUSeconds != 1800 || !result.Receipts.Measurement.ConservativeAccounting.UnresolvedLiability {
					t.Fatal("unknown lost full reservation hold")
				}
				s.Measurement.ReservationCeiling.CPUSeconds--
				if _, e := invokeV2(t, m, s); e == nil {
					t.Fatal("unknown reservation reduced")
				}
			}
		})
	}
}

func TestV2UnknownExecutionAndCPUAccountingStaySeparate(t *testing.T) {
	for _, prefix := range []string{"unknown-held-", "unknown-settled-"} {
		m, s := sharedV2(t, prefix)
		before := encoded(t, s)
		result, e := invokeV2(t, m, s)
		if e != nil || !bytes.Equal(before, encoded(t, result.Receipts)) {
			t.Fatal("unknown metadata changed", e)
		}
		r, c := result.Receipts.Measurement, result.Receipts.Cleanup
		if r.Execution.Status != "unknown" || r.Execution.FailureCategory != "unknown" || r.Execution.ExitCode != nil || r.Quality != (Quality{"unknown", "none"}) || r.ReservationCeiling.CPUSeconds != 1800 || result.ExecutionAuthorized || result.CleanupTrusted || result.ActualResourceAccepted || c.Outcome != "unresolved" || !c.Quarantine || !c.ReuseDenied {
			t.Fatal("unknown promoted or identity/hold changed")
		}
		for _, v := range r.MeasuredConsumption {
			if v.Value != nil || v.Source != "none" {
				t.Fatal("invented consumption")
			}
		}
		if r.ProviderReportedCharge.Value != nil || r.ProviderReportedCharge.Source != "none" || c.ObservedScopes[1].Deletion != "unknown" || c.ObservedScopes[2].Deletion != "unknown" || c.Termination != "unknown" || c.AccessRevocation != "unknown" {
			t.Fatal("CPU settlement laundered broader cleanup/billing")
		}
		if prefix == "unknown-held-" && r.ConservativeAccounting != (Accounting{0, "reserved", true}) {
			t.Fatal("full hold changed")
		}
		if prefix == "unknown-settled-" && r.ConservativeAccounting != (Accounting{1800, "estimated_upper_bound", false}) {
			t.Fatal("full charge changed")
		}
	}
	changes := map[string]func(*ReceiptSetV2){
		"zero":                             func(s *ReceiptSetV2) { s.Measurement.ConservativeAccounting.ChargedCPUSeconds = 0 },
		"partial":                          func(s *ReceiptSetV2) { s.Measurement.ConservativeAccounting.ChargedCPUSeconds = 1799 },
		"aggregate":                        func(s *ReceiptSetV2) { s.Measurement.ConservativeAccounting.ChargedCPUSeconds = 21600 },
		"wrong-provenance":                 func(s *ReceiptSetV2) { s.Measurement.ConservativeAccounting.Provenance = "reserved" },
		"charged-and-held":                 func(s *ReceiptSetV2) { s.Measurement.ConservativeAccounting.UnresolvedLiability = true },
		"execution-cleanup-unknown":        func(s *ReceiptSetV2) { s.Cleanup.ObservedScopes[0].Deletion = "unknown" },
		"execution-cleanup-failed":         func(s *ReceiptSetV2) { s.Cleanup.ObservedScopes[0].Deletion = "failed" },
		"execution-cleanup-not-applicable": func(s *ReceiptSetV2) { s.Cleanup.ObservedScopes[0].Deletion = "not_applicable" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			m, s := sharedV2(t, "unknown-settled-")
			change(&s)
			s.Cleanup.RemainingLiabilities = cleanupLiabilitiesV2(s.Cleanup, s.Measurement.ConservativeAccounting.UnresolvedLiability)
			rehashV2(t, &s)
			if _, e := invokeV2(t, m, s); e == nil {
				t.Fatal("invalid CPU settlement accepted")
			}
		})
	}
	for _, status := range []string{"reserved", "creating", "running"} {
		m, s := sharedV2(t, "unknown-settled-")
		s.Measurement.Execution.Status, s.Measurement.Execution.FailureCategory = status, "none"
		s.Measurement.Execution.ExecutionStarted = status == "running"
		if _, e := invokeV2(t, m, s); e == nil {
			t.Fatal("active CPU settlement accepted", status)
		}
	}
}

func TestV2MetricScopesAndUnavailableValues(t *testing.T) {
	for _, scope := range []string{"process", "sandbox"} {
		for _, elapsed := range []string{"execution_only", "whole_attempt"} {
			m, s := sharedV2(t, "")
			n := int64(17)
			s.Measurement.MeasuredConsumption["cpu_consumed"] = Metric{"available", &n, "nanoseconds", scope, "unverified_collector"}
			s.Measurement.MeasuredConsumption["execution_duration"] = Metric{"available", &n, "nanoseconds", elapsed, "unverified_collector"}
			result, e := invokeV2(t, m, s)
			if e != nil || result.Receipts.Measurement.MeasuredConsumption["cpu_consumed"].Scope != scope || result.Receipts.Measurement.MeasuredConsumption["execution_duration"].Scope != elapsed {
				t.Fatal("metric scope relabeled or refused", e)
			}
		}
	}
	for _, name := range []string{"cpu_consumed", "execution_duration", "gpu_allocated_duration", "gpu_peak_memory", "gpu_utilization"} {
		for _, attack := range []string{"zero-unsupported", "unknown-zero", "scope", "unit", "verified-source", "negative", "overflow", "gpu-available"} {
			t.Run(name+"/"+attack, func(t *testing.T) {
				m, s := sharedV2(t, "")
				v := s.Measurement.MeasuredConsumption[name]
				n := int64(0)
				switch attack {
				case "zero-unsupported":
					v.Value = &n
				case "unknown-zero":
					v.Value, v.Availability = &n, "unknown"
				case "scope":
					v.Scope = "scope_not_recorded"
				case "unit":
					v.Unit = "seconds"
				case "verified-source":
					v.Source = "verified_collector"
				case "negative":
					n = -1
					v.Value, v.Availability, v.Source = &n, "available", "unverified_collector"
				case "overflow":
					n = 9007199254740992
					v.Value, v.Availability, v.Source = &n, "available", "unverified_collector"
				case "gpu-available":
					if !strings.HasPrefix(name, "gpu_") {
						return
					}
					v.Value, v.Availability, v.Source = &n, "available", "unverified_collector"
				}
				s.Measurement.MeasuredConsumption[name] = v
				if _, e := invokeV2(t, m, s); e == nil {
					t.Fatal("invalid metric accepted")
				}
			})
		}
	}
}

func TestV2PortableNegativesAndVersionIsolation(t *testing.T) {
	var cases []struct {
		Name  string          `json:"name"`
		Path  []string        `json:"path"`
		Value json.RawMessage `json:"value"`
		Old   string          `json:"old"`
		New   string          `json:"new"`
	}
	if e := json.Unmarshal(vector(t, "v2/negative-cases"), &cases); e != nil {
		t.Fatal(e)
	}
	raw := vector(t, "v2/validate-request.canonical")
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			bad := raw
			if len(tc.Path) != 0 {
				var root map[string]any
				d := json.NewDecoder(bytes.NewReader(raw))
				d.UseNumber()
				if e := d.Decode(&root); e != nil {
					t.Fatal(e)
				}
				node := root
				for _, key := range tc.Path[:len(tc.Path)-1] {
					node = node[key].(map[string]any)
				}
				var value any
				d = json.NewDecoder(bytes.NewReader(tc.Value))
				d.UseNumber()
				if e := d.Decode(&value); e != nil {
					t.Fatal(e)
				}
				node[tc.Path[len(tc.Path)-1]] = value
				bad = encoded(t, root)
			} else {
				if !bytes.Contains(raw, []byte(tc.Old)) {
					t.Fatal("mutation did not match")
				}
				bad = bytes.ReplaceAll(raw, []byte(tc.Old), []byte(tc.New))
			}
			var out bytes.Buffer
			e := Serve(bytes.NewReader(bad), &out)
			if e == nil || out.Len() != 0 || len(e.Error()) > 64 || strings.Contains(e.Error(), "SECRET") {
				t.Fatal("invalid request accepted/leaked", e)
			}
		})
	}
	m, s := sharedV2(t, "")
	if _, e := DecodeManifest(encoded(t, m)); e == nil {
		t.Fatal("old decoder broadened")
	}
	v1m, _ := shared(t)
	if _, e := DecodeManifestV2(encoded(t, v1m)); e == nil {
		t.Fatal("v2 decoder accepts v1")
	}
	var out bytes.Buffer
	old := bytes.ReplaceAll(vector(t, "validate-request.canonical"), []byte(`"operation":"validate"`), []byte(`"operation":"validate_v2"`))
	if e := Serve(bytes.NewReader(old), &out); e == nil || out.Len() != 0 {
		t.Fatal("v1 promoted to v2", e)
	}
	legacy := map[string]any{"operation": "project_legacy", "manifest": m, "binding": s.Measurement.Binding, "cleanup_receipt_id": "cleanup-v2", "source_receipt": successfulReceipt(t)}
	if e := Serve(bytes.NewReader(encoded(t, legacy)), &out); e == nil || out.Len() != 0 {
		t.Fatal("v2 projected through legacy", e)
	}
	prepareRaw, e := os.ReadFile("../owneddata/testdata/prepare-request.json")
	if e != nil {
		t.Fatal(e)
	}
	if _, e := DecodeOwnedControl("prepare", prepareRaw); e != nil {
		t.Fatal("v1 control baseline invalid", e)
	}
	var prepare map[string]json.RawMessage
	if e := json.Unmarshal(prepareRaw, &prepare); e != nil {
		t.Fatal(e)
	}
	// Keep the otherwise-valid old envelope and its stricter opaque IDs, so
	// failure proves version isolation rather than unrelated ID validation.
	if e := json.Unmarshal(prepare["project_id"], &m.ProjectID); e != nil {
		t.Fatal(e)
	}
	m.AssessmentID = strings.Repeat("1", 32)
	prepare["manifest"] = encoded(t, m)
	if _, e := DecodeOwnedControl("prepare", encoded(t, prepare)); e == nil {
		t.Fatal("owned-control/v1 accepts v2")
	}
}

func TestV2ClosedNestedJSONAndEncodedLimits(t *testing.T) {
	raw := vector(t, "v2/benchmark-manifest.canonical")
	var manifest map[string]json.RawMessage
	if e := json.Unmarshal(raw, &manifest); e != nil {
		t.Fatal(e)
	}
	var ceilings map[string]json.RawMessage
	if e := json.Unmarshal(manifest["ceilings"], &ceilings); e != nil {
		t.Fatal(e)
	}
	for key, value := range ceilings {
		old := []byte(`"` + key + `":` + string(value))
		for _, token := range []string{"null", "true", "false", "1.0", "1e0", "NaN", "Infinity", "-1", "9007199254740992", "9223372036854775808", `"SECRET"`} {
			t.Run(key+"/"+token, func(t *testing.T) {
				bad := bytes.ReplaceAll(raw, old, []byte(`"`+key+`":`+token))
				if bytes.Equal(raw, bad) {
					t.Fatal("mutation missed")
				}
				if _, e := DecodeManifestV2(bad); e == nil {
					t.Fatal("malformed integer accepted")
				}
			})
		}
		duplicate := append(append(append([]byte{}, old...), ','), old...)
		if _, e := DecodeManifestV2(bytes.ReplaceAll(raw, old, duplicate)); e == nil {
			t.Fatal("duplicate nested ceiling accepted", key)
		}
		var missing map[string]json.RawMessage
		if e := json.Unmarshal(raw, &missing); e != nil {
			t.Fatal(e)
		}
		copyCeilings := map[string]json.RawMessage{}
		for k, v := range ceilings {
			if k != key {
				copyCeilings[k] = v
			}
		}
		missing["ceilings"] = encoded(t, copyCeilings)
		if _, e := DecodeManifestV2(encoded(t, missing)); e == nil {
			t.Fatal("missing required ceiling accepted", key)
		}
	}
	for _, key := range []string{"execution_profile", "design", "workload", "hardware", "quality", "ceilings", "measurement"} {
		old := []byte(`"` + key + `":{`)
		bad := bytes.ReplaceAll(raw, old, []byte(`"`+key+`":{"secret":"SECRET",`))
		if bytes.Equal(raw, bad) {
			t.Fatal("mutation missed", key)
		}
		if _, e := DecodeManifestV2(bad); e == nil {
			t.Fatal("unknown nested field accepted", key)
		}
	}
	// Whitespace is valid JSON but still counts against each encoded bound.
	if _, e := DecodeManifestV2(append(bytes.Repeat([]byte(" "), MaxManifestV2Bytes-len(raw)+1), raw...)); e == nil {
		t.Fatal("manifest encoded bound expanded")
	}
	receipts := vector(t, "v2/receipt-set.canonical")
	if _, e := DecodeReceiptsV2(append(bytes.Repeat([]byte(" "), MaxReceiptBytes-len(receipts)+1), receipts...)); e == nil {
		t.Fatal("receipt encoded bound expanded")
	}
	request := vector(t, "v2/validate-request.canonical")
	var out bytes.Buffer
	if e := Serve(bytes.NewReader(append(bytes.Repeat([]byte(" "), MaxRequestBytes-len(request)+1), request...)), &out); e == nil || out.Len() != 0 {
		t.Fatal("request encoded bound expanded", e)
	}
	for _, old := range []string{`"authenticated":false`, `"quarantine":true`, `"reused":false`, `"exit_code":0`} {
		colon := strings.IndexByte(old, ':')
		bad := bytes.ReplaceAll(request, []byte(old), []byte(old[:colon+1]+"null"))
		// reused and exit_code intentionally allow null; test their integer/
		// boolean confusion instead of rejecting their documented null form.
		if strings.HasPrefix(old, `"reused"`) {
			bad = bytes.ReplaceAll(request, []byte(old), []byte(`"reused":0`))
		}
		if strings.HasPrefix(old, `"exit_code"`) {
			bad = bytes.ReplaceAll(request, []byte(old), []byte(`"exit_code":true`))
		}
		if e := Serve(bytes.NewReader(bad), &out); e == nil || out.Len() != 0 {
			t.Fatal("nested scalar type confusion accepted", old, e)
		}
	}
}
