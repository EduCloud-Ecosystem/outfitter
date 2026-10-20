package assessmentexec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestExecutionApprovedLiteralReplyContract(t *testing.T) {
	s := executionFixture(t)
	binding, _ := json.Marshal(s.Binding())
	c := Control{Schema: ControlSchema, Operation: "status", Binding: s.Binding()}
	// Outer keys, enums, nulls and trust values are an independent literal
	// protocol witness. They are not obtained from the production encoder.
	raw := []byte(fmt.Sprintf(`{"schema":"outfitter.assessment-execution-status/v1","operation":"status","code":"OK","binding":%s,"generation":1,"state":"preparing","deny_latched":false,"reason":"none","expires_at":"2026-10-20T02:00:00Z","declared_upload_ceiling_bytes":5000000000,"sealed_length_bytes":null,"writer_epoch":"77777777777777777777777777777777","watch_epoch":null,"admitting_owner_epoch":"88888888888888888888888888888888","clock_epoch":"99999999999999999999999999999999","deadline_tick_ms":7200000,"writer_started":false,"upload_complete":false,"content_close_confirmed":false,"content_close_uncertain":false,"execution_deny_latched":false,"liveness":"unknown","observations":[],"observations_saturated":false,"attempt":null,"trust":{"assurance":"synthetic","upload_authorized":false,"execution_authorized":false,"cleanup_trusted":false,"actual_resource_accepted":false}}`+"\n", binding))
	if _, e := DecodeStatus(raw, c); e != nil {
		t.Fatal("approved literal status", e)
	}
	for name, pair := range map[string][2]string{
		"unapproved-lookup-code": {`"code":"OK"`, `"code":"LOOKUP_ONLY"`},
		"unapproved-liveness":    {`"liveness":"unknown"`, `"liveness":"not_observed"`},
		"duplicate":              {`"generation":1`, `"generation":1,"generation":1`},
		"boolean-integer":        {`"generation":1`, `"generation":true`},
		"missing-null":           {`,"attempt":null`, ``},
		"spurious-completion":    {`"upload_complete":false`, `"upload_complete":true`},
		"promoted-trust":         {`"cleanup_trusted":false`, `"cleanup_trusted":true`},
		"wrong-operation":        {`"operation":"status"`, `"operation":"seal"`},
	} {
		t.Run(name, func(t *testing.T) {
			bad := bytes.Replace(raw, []byte(pair[0]), []byte(pair[1]), 1)
			if bytes.Equal(raw, bad) {
				t.Fatal("mutation missed")
			}
			if _, e := DecodeStatus(bad, c); e == nil {
				t.Fatal("accepted invalid literal reply")
			}
		})
	}
	if _, e := DecodeStatus(append(raw, '\n'), c); e == nil {
		t.Fatal("accepted second frame")
	}
	c.Binding.ProjectID = strings.Repeat("a", 32)
	if _, e := DecodeStatus(raw, c); e == nil {
		t.Fatal("uncorrelated project reply")
	}
}
func TestExecutionResultCrossFieldAccountingAndIdentity(t *testing.T) {
	s := maximumIndependentScope(t)
	a := maximumIndependentAttempt(t, s)
	set, e := Project(s, a)
	if e != nil {
		t.Fatal(e)
	}
	digest := Digest(set)
	result := Result{Schema: ResultSchema, Operation: "execute", Code: "HELD", Disposition: "existing_attempt", Binding: s.Binding(), Slot: a.Slot, HandoffNonce: a.HandoffNonce, BindingDigest: a.BindingDigest, ScopeGeneration: s.Generation, Attempt: a.Public(), Compute: a.Compute, AccountingUnresolved: a.AccountingUnresolved, NormalizedReceipts: &set, NormalizedReceiptsSHA256: &digest, Trust: Untrusted()}
	gen := s.Generation
	c := Control{Schema: ControlSchema, Operation: "execute", Binding: s.Binding(), ExpectedGeneration: &gen, AdmittingOwnerEpoch: &s.AdmittingOwnerEpoch, Slot: &a.Slot, HandoffNonce: &a.HandoffNonce}
	raw, e := EncodeReply(result)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := DecodeResult(raw, c, s, &a); e != nil {
		t.Fatal(e)
	}
	heldWithoutReceipts := Clone(result)
	heldWithoutReceipts.NormalizedReceipts = nil
	heldWithoutReceipts.NormalizedReceiptsSHA256 = nil
	hb, _ := json.Marshal(heldWithoutReceipts)
	if _, e := DecodeResult(append(hb, '\n'), c, s, &a); e != nil {
		t.Fatal("HELD with explicitly absent normalized receipts", e)
	}
	for name, mutate := range map[string]func(*Result){
		"execution-status": func(r *Result) { r.NormalizedReceipts.Measurement.Execution.Status = "succeeded" },
		"exit-code":        func(r *Result) { v := 4; r.NormalizedReceipts.Measurement.Execution.ExitCode = &v },
		"provider-charge":  func(r *Result) { r.NormalizedReceipts.Measurement.ProviderReportedCharge.Source = "invented" },
		"both-hardware": func(r *Result) {
			r.NormalizedReceipts.Measurement.Binding.Hardware.Kind = "invented"
			r.NormalizedReceipts.Cleanup.Binding.Hardware.Kind = "invented"
		},
		"both-method": func(r *Result) {
			r.NormalizedReceipts.Measurement.Binding.MeasurementMethod.Method = "invented"
			r.NormalizedReceipts.Cleanup.Binding.MeasurementMethod.Method = "invented"
		},
		"cleanup-scope":     func(r *Result) { r.NormalizedReceipts.Cleanup.ObservedScopes[0].ScopeID = "invented" },
		"cleanup-liability": func(r *Result) { r.NormalizedReceipts.Cleanup.RemainingLiabilities = []string{} },
		"cleanup-method":    func(r *Result) { r.NormalizedReceipts.Cleanup.Method.Name = "invented" },
		"claim-receipt-id": func(r *Result) {
			r.NormalizedReceipts.Cleanup.ReceiptID = strings.Repeat("a", 32)
			r.NormalizedReceipts.Measurement.CleanupReceiptID = r.NormalizedReceipts.Cleanup.ReceiptID
		},
		"old-disposition":    func(r *Result) { r.Disposition = "TERMINAL" },
		"old-code":           func(r *Result) { r.Code = "RECORDED" },
		"changed-slot":       func(r *Result) { r.Slot.SampleSize++ },
		"charge-drop":        func(r *Result) { r.Compute.ChargedCPUSeconds = 0 },
		"double-count":       func(r *Result) { r.AccountingUnresolved = true },
		"wrong-handoff":      func(r *Result) { r.HandoffNonce = strings.Repeat("a", 32) },
		"wrong-quality-code": func(r *Result) { r.Attempt.Quality.Checker.CodeSHA256 = strings.Repeat("a", 64) },
		"receipt-charge-drop": func(r *Result) {
			r.NormalizedReceipts.Measurement.ConservativeAccounting.ChargedCPUSeconds = 0
			v := Digest(r.NormalizedReceipts)
			r.NormalizedReceiptsSHA256 = &v
		},
		"imported-trust": func(r *Result) {
			r.NormalizedReceipts.Cleanup.Observation.Authenticated = true
			v := Digest(r.NormalizedReceipts.Cleanup)
			r.NormalizedReceipts.Measurement.CleanupReceiptSHA256 = v
			v = Digest(r.NormalizedReceipts)
			r.NormalizedReceiptsSHA256 = &v
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad := Clone(result)
			mutate(&bad)
			// Rehash every imported layer: digest consistency alone is not authority.
			bad.NormalizedReceipts.Measurement.CleanupReceiptSHA256 = Digest(bad.NormalizedReceipts.Cleanup)
			d := Digest(bad.NormalizedReceipts)
			bad.NormalizedReceiptsSHA256 = &d
			b, e := json.Marshal(bad)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = DecodeResult(append(b, '\n'), c, s, &a); e == nil {
				t.Fatal("invalid correlated result accepted")
			}
		})
	}
}

func TestExecutionApprovedLiteralErrorCodes(t *testing.T) {
	for _, code := range []string{"ASSESSMENT_INVALID", "ASSESSMENT_DISABLED", "ASSESSMENT_DENIED", "ASSESSMENT_MISSING", "ASSESSMENT_COLLISION", "ASSESSMENT_STALE", "ASSESSMENT_BUSY", "ASSESSMENT_UNRESOLVED", "ASSESSMENT_METADATA_LIMIT"} {
		b, e := EncodeReply(Error{Schema: ErrorSchema, Code: code})
		if e != nil || !bytes.Contains(b, []byte(code)) {
			t.Fatalf("literal error %s: %v", code, e)
		}
	}
	for _, code := range []string{"ASSESSMENT_HELD", "ASSESSMENT_EXECUTION_HELD", "ASSESSMENT_EXECUTION_INVALID", "HELD", "UNRESOLVED"} {
		if _, e := EncodeReply(Error{Schema: ErrorSchema, Code: code}); e == nil {
			t.Fatalf("accepted drift %s", code)
		}
	}
}
func TestExecutionMetricScopeIsSpecificToMetric(t *testing.T) {
	for _, scope := range []string{"execution_only", "whole_attempt"} {
		m := UnknownMetrics()
		m.ExecutionDuration.Scope = scope
		if m.Validate() != nil {
			t.Fatal("duration", scope)
		}
	}
	for _, scope := range []string{"process", "sandbox"} {
		m := UnknownMetrics()
		m.CPUConsumed.Scope = scope
		if m.Validate() != nil {
			t.Fatal("CPU", scope)
		}
	}
	for name, mutate := range map[string]func(*MetricSet){
		"CPU-execution-only":   func(m *MetricSet) { m.CPUConsumed.Scope = "execution_only" },
		"duration-process":     func(m *MetricSet) { m.ExecutionDuration.Scope = "process" },
		"other-execution-only": func(m *MetricSet) { m.PeakMemory.Scope = "execution_only" },
	} {
		t.Run(name, func(t *testing.T) {
			m := UnknownMetrics()
			mutate(&m)
			if m.Validate() == nil {
				t.Fatal("accepted wrong per-metric scope")
			}
		})
	}
}
