package ledger

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

// NativeAccount uses explicitly named native units; it never contributes to CPU
// seconds. Policy is an immutable adapter contract, not credentials or payloads.
type NativeAccount struct {
	ID       string          `json:"id"`
	Provider string          `json:"provider"`
	Unit     string          `json:"unit"`
	Scope    string          `json:"scope"`
	Budget   int64           `json:"budget"`
	Policy   json.RawMessage `json:"policy"`
}

// NativeObservation is a cumulative sample, not a charge or an additive interval.
type NativeObservation struct {
	ID             string    `json:"id"`
	Amount         int64     `json:"amount"`
	Provenance     string    `json:"provenance"`
	At             time.Time `json:"at"`
	EvidenceSHA256 string    `json:"evidence_sha256"`
}
type NativeRecord struct {
	Observations       []NativeObservation `json:"observations,omitempty"`
	RequestID          string              `json:"request_id"`
	AccountID          string              `json:"account_id"`
	Owner              string              `json:"owner"`
	ReservationID      string              `json:"reservation_id"`
	PayloadHash        string              `json:"payload_hash"`
	CreatedAt          time.Time           `json:"created_at"`
	Deadline           time.Time           `json:"deadline"`
	Reserved           int64               `json:"reserved"`
	CleanupVerified    bool                `json:"cleanup_verified"`
	Ambiguous          bool                `json:"ambiguous"`
	Settled            *int64              `json:"settled,omitempty"`
	SettlementEvidence string              `json:"settlement_evidence,omitempty"`
	State              json.RawMessage     `json:"state"`
	Revision           uint64              `json:"revision"`
}
type NativeUsage struct {
	AccountID string `json:"account_id"`
	Unit      string `json:"unit"`
	Budget    int64  `json:"budget"`
	Reserved  int64  `json:"reserved"`
	Settled   int64  `json:"settled"`
	Remaining int64  `json:"remaining"`
	Overrun   bool   `json:"overrun"`
}

func NativeCalculate(d Data, id string) NativeUsage {
	a := d.NativeAccounts[id]
	u := NativeUsage{AccountID: id, Unit: a.Unit, Budget: a.Budget}
	for _, r := range d.NativeRuns {
		if r.AccountID == id {
			if r.Settled == nil {
				u.Reserved += r.Reserved
			} else {
				u.Settled += *r.Settled
				u.Overrun = u.Overrun || *r.Settled > r.Reserved
			}
		}
	}
	u.Remaining = u.Budget - u.Reserved - u.Settled
	return u
}
func validateNative(d Data) error {
	if d.Version < 3 && (len(d.NativeAccounts) > 0 || len(d.NativeRuns) > 0) {
		return errors.New("native accounting requires ledger v3")
	}
	if len(d.NativeAccounts) > MaxProjects || len(d.NativeRuns) > MaxRecords {
		return errors.New("native ledger capacity exceeded")
	}
	for id, a := range d.NativeAccounts {
		if id != a.ID || !provider.ValidID(id) || !provider.ValidID(a.Provider) || !provider.ValidID(a.Unit) || !provider.ValidID(a.Scope) || a.Budget < 1 || a.Budget > 1000000000 || !json.Valid(a.Policy) || len(a.Policy) > 16384 {
			return errors.New("invalid native account")
		}
	}
	for id, r := range d.NativeRuns {
		_, ok := d.NativeAccounts[r.AccountID]
		if !ok || id != r.RequestID || !provider.ValidID(id) || r.Owner != d.Owner || !validHex(r.ReservationID, 16) || !validHex(r.PayloadHash, 32) || r.CreatedAt.IsZero() || !r.Deadline.After(r.CreatedAt) || r.Deadline.Sub(r.CreatedAt) > time.Hour || r.Reserved < 1 || r.Reserved > 1000000000 || r.Revision < 1 || !json.Valid(r.State) || len(r.State) > 16384 {
			return errors.New("invalid native reservation")
		}
		if len(r.Observations) > 64 {
			return errors.New("native observation capacity exceeded")
		}
		seen := map[string]bool{}
		for _, o := range r.Observations {
			if !provider.ValidID(o.ID) || seen[o.ID] || o.Amount < 0 || o.Amount > 1000000000 || (o.Provenance != "estimated" && o.Provenance != "provider_observed") || o.At.Before(r.CreatedAt) || !validHex(o.EvidenceSHA256, 32) {
				return errors.New("invalid native observation")
			}
			seen[o.ID] = true
		}
		if _, ok := d.Runs[id]; ok {
			return errors.New("native/local request ID collision")
		}
		if r.Settled != nil {
			if *r.Settled < 0 || *r.Settled > 1000000000 || !r.CleanupVerified || r.Ambiguous || !validHex(r.SettlementEvidence, 32) {
				return errors.New("invalid native settlement")
			}
		} else if r.SettlementEvidence != "" {
			return errors.New("settlement evidence without settled charge")
		}
	}
	for id := range d.NativeAccounts {
		u := NativeCalculate(d, id)
		if u.Reserved > u.Budget || (u.Remaining < 0 && !u.Overrun) {
			return errors.New("native budget overdrawn without recorded overrun")
		}
	}
	return nil
}
func (s *Store) InitNativeAccount(a NativeAccount) error {
	return s.Update(func(d *Data) error {
		if e := RequireAdmission(*d); e != nil {
			return e
		}
		if old, ok := d.NativeAccounts[a.ID]; ok {
			x, _ := json.Marshal(old)
			y, _ := json.Marshal(a)
			if string(x) != string(y) {
				return errors.New("native account policy and allowance are immutable")
			}
			return nil
		}
		if d.NativeAccounts == nil {
			d.NativeAccounts = map[string]NativeAccount{}
		}
		if d.NativeRuns == nil {
			d.NativeRuns = map[string]NativeRecord{}
		}
		d.Version = 3
		d.NativeAccounts[a.ID] = a
		return nil
	})
}

// Settlement is explicit provider evidence, never inferred from deletion or an
// estimate. Above-ceiling charges stay visible and halt future native admission.
func (s *Store) SettleNative(id string, amount int64, evidenceSHA256 string) error {
	return s.Update(func(d *Data) error {
		if e := RequireAdmission(*d); e != nil {
			return e
		}
		r, ok := d.NativeRuns[id]
		if !ok {
			return errors.New("unknown native request")
		}
		if r.Settled != nil {
			if *r.Settled == amount && r.SettlementEvidence == evidenceSHA256 {
				return nil
			}
			return errors.New("settlement is immutable")
		}
		if !r.CleanupVerified || r.Ambiguous {
			return errors.New("unresolved effects cannot settle")
		}
		r.Settled = &amount
		r.SettlementEvidence = evidenceSHA256
		r.Revision++
		d.NativeRuns[id] = r
		return nil
	})
}

// AppendNativeObservation never releases reservations or changes a settlement.
func (s *Store) AppendNativeObservation(id string, o NativeObservation) error {
	return s.Update(func(d *Data) error {
		if e := RequireAdmission(*d); e != nil {
			return e
		}
		r, ok := d.NativeRuns[id]
		if !ok {
			return errors.New("unknown native request")
		}
		for _, old := range r.Observations {
			if old.ID == o.ID {
				if old == o {
					return nil
				}
				return errors.New("observation ID collision")
			}
		}
		r.Observations = append(r.Observations, o)
		r.Revision++
		d.NativeRuns[id] = r
		return nil
	})
}
