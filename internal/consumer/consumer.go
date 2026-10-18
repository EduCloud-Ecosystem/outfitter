// Package consumer defines a synthetic, one-attempt caller contract. It never
// publishes grades or retries work; accounting and execution belong to broker.
package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/EduCloud-Ecosystem/outfitter/internal/broker"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

type Request struct {
	SchemaVersion int    `json:"schema_version"`
	ProjectID     string `json:"project_id"`
	provider.ConsumerBinding
	Workload provider.Request `json:"workload"`
}
type Preflight struct {
	SchemaVersion      int      `json:"schema_version"`
	RequestID          string   `json:"request_id"`
	BindingSHA256      string   `json:"binding_sha256"`
	ReservedCPUSeconds int64    `json:"reserved_cpu_seconds"`
	Profile            string   `json:"profile"`
	GradePublished     bool     `json:"grade_published"`
	Limitations        []string `json:"limitations"`
}
type Receipt struct {
	provider.Receipt
	Consumer               string `json:"consumer"`
	JobID                  string `json:"job_id"`
	Attempt                int    `json:"attempt"`
	BindingSHA256          string `json:"binding_sha256"`
	Reused                 bool   `json:"reused"`
	ReconciliationRequired bool   `json:"reconciliation_required"`
	GradePublished         bool   `json:"grade_published"`
}

func Decode(r io.Reader) (Request, error) {
	var req Request
	raw, e := io.ReadAll(io.LimitReader(r, 65537))
	if e != nil || len(raw) > 65536 {
		return req, errors.New("consumer request exceeds 64 KiB or cannot be read")
	}
	// A bounded decoder still rejects trailing values and unknown fields.
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if e = dec.Decode(&req); e != nil {
		return req, e
	}
	if dec.Decode(new(any)) != io.EOF {
		return req, errors.New("trailing consumer request JSON")
	}
	return req, nil
}
func prepare(req Request) (provider.Request, error) {
	w := req.Workload
	if req.SchemaVersion != 1 || w.SchemaVersion != 1 || w.Consumer != nil || w.RequestID != "" || w.ProjectID != "" {
		return w, errors.New("consumer/workload schema 1 required; workload identity is derived, not supplied")
	}
	id, e := provider.ConsumerRequestID(req.Consumer, req.ProjectID, req.JobID)
	if e != nil {
		return w, e
	}
	w.RequestID = id
	w.ProjectID = req.ProjectID
	binding := req.ConsumerBinding
	w.Consumer = &binding
	return w, w.Validate()
}

// Check is pure: no ledger, path reads, provider lookup, or resource creation.
func Check(req Request) (Preflight, error) {
	w, e := prepare(req)
	if e != nil {
		return Preflight{}, e
	}
	return Preflight{SchemaVersion: 1, RequestID: w.RequestID, BindingSHA256: w.Hash(), ReservedCPUSeconds: w.ReservationAmount(), Profile: "reviewed-local/synthetic/one-attempt", Limitations: []string{"declared contract only; no image or capacity inspection", "no real course admission, grading, native provider or production Cairn integration", "supervised watchdog; no host-loss self-expiry", "hashes attest bytes, not instructor authorization"}}, nil
}
func envelope(w provider.Request, r provider.Receipt, reused bool) Receipt {
	active := r.Status == "reserved" || r.Status == "creating" || r.Status == "running"
	return Receipt{Receipt: r, Consumer: w.Consumer.Consumer, JobID: w.Consumer.JobID, Attempt: 1, BindingSHA256: w.Hash(), Reused: reused, ReconciliationRequired: active || r.Status == "unknown" || r.UnresolvedLiability || !r.CleanupVerified || r.OutputError != ""}
}
func Run(ctx context.Context, b *broker.Broker, req Request) (Receipt, error) {
	w, e := prepare(req)
	if e != nil {
		return Receipt{}, e
	}
	r, reused, e := b.RunOnce(ctx, w)
	if e != nil {
		return Receipt{}, e
	}
	return envelope(w, r, reused), nil
}

// Status reads the same authority and diagnostic references without touching the
// source inputs, changing admission state, or contacting the provider.
func Status(s *ledger.Store, consumer, project, job string) (Receipt, error) {
	id, e := provider.ConsumerRequestID(consumer, project, job)
	if e != nil {
		return Receipt{}, e
	}
	record, e := s.Record(id)
	if e != nil {
		return Receipt{}, e
	}
	if record.Request.Consumer == nil {
		return Receipt{}, errors.New("request is not a consumer job")
	}
	r, e := s.Receipt(id)
	if e != nil {
		return Receipt{}, e
	}
	return envelope(record.Request, r, true), nil
}
