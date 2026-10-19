package assessment

import (
	"bytes"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"

	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

// LegacyReceipt accepts both the existing broker receipt and its synthetic
// consumer envelope. Diagnostics and source binding hashes are input only;
// projection must never copy them into retained assessment metadata.
type LegacyReceipt struct {
	provider.Receipt
	Consumer               string `json:"consumer,omitempty"`
	JobID                  string `json:"job_id,omitempty"`
	Attempt                int    `json:"attempt,omitempty"`
	BindingSHA256          string `json:"binding_sha256,omitempty"`
	Reused                 bool   `json:"reused,omitempty"`
	ReconciliationRequired bool   `json:"reconciliation_required,omitempty"`
	GradePublished         bool   `json:"grade_published,omitempty"`
}

const MaxLegacyBytes = 7 << 20 // Existing 1 MiB combined output may be JSON-escaped.

var immutableImage = regexp.MustCompile(`^(sha256:[a-f0-9]{64}|[a-zA-Z0-9][a-zA-Z0-9._:/-]*@sha256:[a-f0-9]{64})$`)

func digest(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && strings.ToLower(s) == s
}

func DecodeLegacy(raw []byte) (LegacyReceipt, error) {
	var result LegacyReceipt
	invalid := errors.New("INVALID_LEGACY_RECEIPT")
	if strictJSON(raw, &result, MaxLegacyBytes) != nil {
		return LegacyReceipt{}, invalid
	}
	required := []string{"admitted", "execution_started", "schema_version", "request_id", "project_id", "status", "exit_code", "error", "stdout", "stderr", "cleanup_verified", "unresolved_liability", "image", "created_at", "started_at", "finished_at", "deadline", "cancel_requested", "compute", "teardown_risk"}
	fields, e := objectFields(raw, required, map[string]bool{"exit_code": true, "started_at": true, "finished_at": true})
	if e != nil {
		return LegacyReceipt{}, invalid
	}
	if _, e := objectFields(fields["compute"], []string{"reserved_cpu_seconds", "charged_cpu_seconds", "provenance"}, nil); e != nil {
		return LegacyReceipt{}, invalid
	}
	if value, ok := fields["output_error"]; ok && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return LegacyReceipt{}, invalid
	}
	consumerFields := []string{"consumer", "job_id", "attempt", "binding_sha256", "reused", "reconciliation_required", "grade_published"}
	consumerEnvelope := false
	for _, key := range consumerFields {
		_, found := fields[key]
		consumerEnvelope = consumerEnvelope || found
	}
	if consumerEnvelope {
		if _, e := objectFields(raw, consumerFields, nil); e != nil || result.Consumer == "" {
			return LegacyReceipt{}, invalid
		}
	}
	if ValidateLegacy(result) != nil {
		return LegacyReceipt{}, invalid
	}
	return result, nil
}

// ValidateLegacy checks the existing local accounting assertions. Passing this
// function does not establish that the supplied receipt came from a real broker.
func ValidateLegacy(r LegacyReceipt) error {
	invalid := errors.New("INVALID_LEGACY_RECEIPT")
	if r.SchemaVersion != 1 || !provider.ValidID(r.RequestID) || !provider.ValidID(r.ProjectID) || len(r.Image) > 512 || !immutableImage.MatchString(r.Image) || len(r.Stdout)+len(r.Stderr) > 1048576 || len(r.Error) > 2048 || len(r.OutputError) > 256 || len(r.TeardownRisk) > 256 {
		return invalid
	}
	if r.ExitCode != nil && (*r.ExitCode < 0 || *r.ExitCode > 255) {
		return invalid
	}
	active := r.Status == "reserved" || r.Status == "creating" || r.Status == "running"
	if !active && r.Status != "succeeded" && r.Status != "failed" && r.Status != "cancelled" && r.Status != "unknown" {
		return invalid
	}
	c := r.Compute
	if c.Reserved < 0 || c.Reserved > 600 || c.Charged < 0 || c.Charged > c.Reserved {
		return invalid
	}
	if !r.Admitted {
		if r.Status != "failed" || r.ExecutionStarted || r.CancelRequested || !r.CleanupVerified || r.UnresolvedLiability || c.Reserved != 0 || c.Charged != 0 || c.Provenance != "not_admitted" || r.ExitCode != nil || r.StartedAt != nil || r.FinishedAt != nil {
			return invalid
		}
	} else {
		if c.Reserved == 0 || r.CreatedAt.IsZero() || !r.Deadline.After(r.CreatedAt) || r.ExecutionStarted != (r.StartedAt != nil) {
			return invalid
		}
		if active && (r.ExitCode != nil || r.FinishedAt != nil || r.CleanupVerified) || oneOf(r.Status, "reserved", "creating") && r.ExecutionStarted || r.ExitCode != nil && (!r.ExecutionStarted || r.FinishedAt == nil) {
			return invalid
		}
		if r.UnresolvedLiability {
			if c.Charged != 0 || c.Provenance != "reserved" {
				return invalid
			}
		} else if c.Charged != c.Reserved || c.Provenance != "estimated_upper_bound" || !r.CleanupVerified || active || r.FinishedAt == nil {
			return invalid
		}
		if r.Status == "running" && !r.ExecutionStarted {
			return invalid
		}
		if r.Status == "succeeded" && (!r.ExecutionStarted || r.ExitCode == nil || *r.ExitCode != 0 || !r.CleanupVerified || r.UnresolvedLiability || r.CancelRequested) {
			return invalid
		}
	}
	if r.Consumer != "" {
		id, e := provider.ConsumerRequestID(r.Consumer, r.ProjectID, r.JobID)
		if e != nil || id != r.RequestID || r.Attempt != 1 || !digest(r.BindingSHA256) || r.GradePublished || r.ReconciliationRequired != (active || r.Status == "unknown" || r.UnresolvedLiability || !r.CleanupVerified || r.OutputError != "") {
			return invalid
		}
	} else if r.JobID != "" || r.Attempt != 0 || r.BindingSHA256 != "" || r.Reused || r.ReconciliationRequired || r.GradePublished {
		return invalid
	}
	return nil
}
