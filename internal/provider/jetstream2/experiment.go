package jetstream2

// This envelope is authority for one simulated experiment only. It is separate
// from Policy because compatibility already binds the complete native policy.
import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
)

type OfflineExperiment struct {
	Version            int                   `json:"version"`
	EvidenceScope      string                `json:"evidence_scope"`
	AuthorityID        string                `json:"authority_id"`
	AccountID          string                `json:"account_id"`
	ConfigDigest       string                `json:"config_digest"`
	PolicyDigest       string                `json:"policy_digest"`
	ProfileDigest      string                `json:"profile_digest"`
	SnapshotDigest     string                `json:"snapshot_digest"`
	Request            WorkerRequest         `json:"request"`
	ReservationID      string                `json:"reservation_id"`
	NotBefore          time.Time             `json:"not_before"`
	ExpiresAt          time.Time             `json:"expires_at"`
	MaxLifetimeSeconds int                   `json:"max_lifetime_seconds"`
	MaxAttempts        int                   `json:"max_attempts"`
	Profile            CompatibilityProfile  `json:"profile"`
	Snapshot           CompatibilitySnapshot `json:"snapshot"`
}

// DecodeOfflineExperiment rejects malformed envelopes without exposing inputs.
// Binding to a particular durable authority is checked during configuration.
func DecodeOfflineExperiment(raw []byte) (OfflineExperiment, error) {
	var x OfflineExperiment
	if len(raw) > 8192 || compatibilityJSON(raw, &x) != nil {
		return OfflineExperiment{}, errors.New("INVALID_EXPERIMENT_JSON")
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return OfflineExperiment{}, errors.New("INVALID_EXPERIMENT_JSON")
	}
	typ := reflect.TypeOf(x)
	for i := 0; i < typ.NumField(); i++ {
		b, ok := fields[typ.Field(i).Tag.Get("json")]
		if !ok || bytes.Equal(bytes.TrimSpace(b), []byte("null")) {
			return OfflineExperiment{}, errors.New("INVALID_EXPERIMENT_JSON")
		}
	}
	if len(fields["profile"]) > 2048 || len(fields["snapshot"]) > 4096 || x.Version != 1 || x.EvidenceScope != "synthetic_offline" || x.MaxAttempts != 1 || x.MaxLifetimeSeconds < 1 || x.MaxLifetimeSeconds > 3600 || !compatibilityTime(x.NotBefore) || !compatibilityTime(x.ExpiresAt) || !x.NotBefore.Before(x.ExpiresAt) {
		return OfflineExperiment{}, errors.New("INVALID_EXPERIMENT_JSON")
	}
	p, e := DecodeCompatibilityProfile(fields["profile"])
	if e != nil {
		return OfflineExperiment{}, errors.New("INVALID_EXPERIMENT_JSON")
	}
	s, e := DecodeCompatibilitySnapshot(fields["snapshot"])
	if e != nil {
		return OfflineExperiment{}, errors.New("INVALID_EXPERIMENT_JSON")
	}
	if requiredCompatibilityFields(fields["request"], reflect.TypeOf(x.Request)) != nil {
		return OfflineExperiment{}, errors.New("INVALID_EXPERIMENT_JSON")
	}
	if len(p.Rules) > 8 || len(s.Rules.value) > 8 {
		return OfflineExperiment{}, errors.New("INVALID_EXPERIMENT_JSON")
	}
	x.Profile, x.Snapshot = p, s
	return x, nil
}

func normalizeExperiment(x OfflineExperiment, c Config, p Policy) (OfflineExperiment, []byte, error) {
	fail := func() (OfflineExperiment, []byte, error) {
		return OfflineExperiment{}, nil, errors.New("INVALID_OFFLINE_EXPERIMENT")
	}
	token, e := hex.DecodeString(x.ReservationID)
	if e != nil || len(token) != 16 || x.ReservationID != strings.ToLower(x.ReservationID) || x.Version != 1 || x.EvidenceScope != "synthetic_offline" || x.MaxAttempts != 1 || x.MaxLifetimeSeconds < 1 || x.MaxLifetimeSeconds > 3600 || x.Request.MaxSeconds < 1 || x.Request.MaxSeconds > x.MaxLifetimeSeconds || !ref.MatchString(x.Request.RequestID) || !ref.MatchString(x.Request.Owner) || x.Request.CeilingMilliSU < 1 || x.Request.CeilingMilliSU > 1000000000 || p.Rescue == nil || x.AuthorityID != p.Rescue.AuthorityID || x.AccountID != accountID(c) || !compatibilityTime(x.NotBefore) || !compatibilityTime(x.ExpiresAt) || !x.NotBefore.Before(x.ExpiresAt) || x.ExpiresAt.After(p.ApprovalExpiresAt) {
		return fail()
	}
	if _, e = loopbackOrigin(c.AuthURL); e != nil {
		return fail()
	}
	x.NotBefore, x.ExpiresAt = x.NotBefore.UTC(), x.ExpiresAt.UTC()
	x.Profile, e = compatibilityProfile(x.Profile)
	if e != nil {
		return fail()
	}
	x.Snapshot, e = compatibilitySnapshot(x.Snapshot)
	if e != nil {
		return fail()
	}
	report, e := EvaluateCompatibility(x.Profile, x.Snapshot, c, p, x.NotBefore)
	if e != nil || x.ConfigDigest != report.ConfigDigest || x.PolicyDigest != report.PolicyDigest || x.ProfileDigest != report.ProfileDigest || x.SnapshotDigest != report.SnapshotDigest || x.Profile.ConfigDigest != report.ConfigDigest || x.Profile.PolicyDigest != report.PolicyDigest || x.Snapshot.ConfigDigest != report.ConfigDigest || x.Snapshot.PolicyDigest != report.PolicyDigest || x.Snapshot.ProfileDigest != report.ProfileDigest {
		return fail()
	}
	pb, _ := json.Marshal(x.Profile)
	sb, _ := json.Marshal(x.Snapshot)
	if len(pb) > 2048 || len(sb) > 4096 || len(x.Profile.Rules) > 8 || len(x.Snapshot.Rules.value) > 8 {
		return fail()
	}
	b, e := json.Marshal(x)
	if e != nil || len(b) > 8192 {
		return fail()
	}
	return x, b, nil
}

func ConfigureOfflineExperiment(j *DurableJournal, x OfflineExperiment) error {
	if j == nil || j.Store == nil {
		return errors.New("OFFLINE_JOURNAL_REQUIRED")
	}
	x, b, e := normalizeExperiment(x, j.config, j.policy)
	if e != nil {
		return e
	}
	return j.Store.Update(func(d *ledger.Data) error {
		if ledger.RequireAdmission(*d) != nil || d.Authority.ID != x.AuthorityID || d.Owner != x.Request.Owner {
			return errors.New("EXPERIMENT_AUTHORITY_MISMATCH")
		}
		a, ok := d.NativeAccounts[j.accountID]
		policy, _ := json.Marshal(j.policy)
		if !ok || !bytes.Equal(a.Policy, policy) {
			return errors.New("EXPERIMENT_POLICY_MISMATCH")
		}
		if a.Experiment != nil {
			if !bytes.Equal(a.Experiment.Envelope, b) {
				return errors.New("EXPERIMENT_IS_IMMUTABLE")
			}
			return nil
		}
		for _, r := range d.NativeRuns {
			if r.AccountID == j.accountID {
				return errors.New("EXPERIMENT_ACCOUNT_ALREADY_USED")
			}
		}
		a.Experiment = &ledger.NativeExperiment{Envelope: b, SHA256: rescueHash(b), RequestID: x.Request.RequestID, ReservationID: x.ReservationID}
		d.NativeAccounts[j.accountID] = a
		return nil
	})
}

func (j *DurableJournal) experiment(d ledger.Data) (*OfflineExperiment, error) {
	a, ok := d.NativeAccounts[j.accountID]
	if !ok {
		return nil, errors.New("EXPERIMENT_ACCOUNT_MISSING")
	}
	if a.Experiment == nil {
		return nil, nil
	}
	policy, _ := json.Marshal(j.policy)
	if !bytes.Equal(policy, a.Policy) {
		return nil, errors.New("EXPERIMENT_POLICY_MISMATCH")
	}
	x, e := DecodeOfflineExperiment(a.Experiment.Envelope)
	if e != nil {
		return nil, e
	}
	x, b, e := normalizeExperiment(x, j.config, j.policy)
	if e != nil || !bytes.Equal(b, a.Experiment.Envelope) || rescueHash(b) != a.Experiment.SHA256 || a.Experiment.RequestID != x.Request.RequestID || a.Experiment.ReservationID != x.ReservationID || x.Request.Owner != d.Owner {
		return nil, errors.New("EXPERIMENT_BINDING_MISMATCH")
	}
	return &x, nil
}
func (j *DurableJournal) checkExperiment(d ledger.Data, x *OfflineExperiment, w WorkerRequest, now, deadline time.Time) error {
	if ledger.RequireAdmission(d) != nil || d.Authority.ID != x.AuthorityID || x.Request != w {
		return errors.New("EXPERIMENT_AUTHORITY_OR_REQUEST_MISMATCH")
	}
	if !compatibilityTime(now) || now.Before(x.NotBefore) || !now.Before(x.ExpiresAt) || !deadline.After(now) || deadline.After(x.ExpiresAt) || deadline.Sub(now) > time.Duration(x.MaxLifetimeSeconds)*time.Second {
		return errors.New("EXPERIMENT_TIME_OR_LIFETIME_MISMATCH")
	}
	report, e := EvaluateCompatibility(x.Profile, x.Snapshot, j.config, j.policy, now)
	if e != nil || !report.MetadataMatch {
		return errors.New("EXPERIMENT_METADATA_NOT_MATCHED")
	}
	return nil
}

// prepareExperiment makes exact repeats lookup-only, before provider preflight.
// Fresh checks are repeated under the admission transaction and at both POSTs.
func (j *DurableJournal) prepareExperiment(ctx context.Context, w WorkerRequest) (r WorkerRecord, replay, enabled bool, err error) {
	if err = ctx.Err(); err != nil {
		return
	}
	err = j.Store.View(func(d ledger.Data) error {
		x, e := j.experiment(d)
		if e != nil {
			return e
		}
		if x == nil {
			return nil
		}
		enabled = true
		if ledger.RequireAdmission(d) != nil || d.Authority.ID != x.AuthorityID || w != x.Request {
			return errors.New("EXPERIMENT_AUTHORITY_OR_REQUEST_MISMATCH")
		}
		if n, ok := d.NativeRuns[w.RequestID]; ok {
			r, e = j.decode(n)
			if e == nil && (r.Request != w || r.ReservationID != x.ReservationID) {
				e = errors.New("EXPERIMENT_RESERVATION_MISMATCH")
			}
			replay = e == nil
			return e
		}
		now := time.Now().UTC()
		return j.checkExperiment(d, x, w, now, now.Add(time.Duration(w.MaxSeconds)*time.Second))
	})
	return
}
func (j *DurableJournal) checkExperimentPOST(ctx context.Context, r WorkerRecord) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	return j.Store.View(func(d ledger.Data) error {
		x, e := j.experiment(d)
		if e != nil {
			return e
		}
		if x == nil {
			if r.ExperimentDigest != "" {
				return errors.New("EXPERIMENT_MISSING")
			}
			return nil
		}
		n, ok := d.NativeRuns[r.Request.RequestID]
		if !ok {
			return errors.New("EXPERIMENT_RESERVATION_MISSING")
		}
		stored, e := j.decode(n)
		if e != nil || digest(stored) != digest(r) || r.ExperimentDigest != d.NativeAccounts[j.accountID].Experiment.SHA256 || r.ReservationID != x.ReservationID {
			return errors.New("EXPERIMENT_RESERVATION_MISMATCH")
		}
		now := time.Now().UTC()
		if now.Before(r.CreatedAt) {
			return errors.New("EXPERIMENT_CLOCK_BEFORE_RESERVATION")
		}
		return j.checkExperiment(d, x, r.Request, now, r.Deadline)
	})
}
