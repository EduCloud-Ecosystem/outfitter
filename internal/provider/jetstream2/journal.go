package jetstream2

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

// Policy is an operator-reviewed finite experiment permit, not proof of an
// independent host, available allocation or provider-enforced spending cap.
type Policy struct {
	Config                   Config        `json:"config"`
	BudgetMilliSU            int64         `json:"budget_milli_su"`
	RateMilliSUPerHour       int64         `json:"rate_milli_su_per_hour"`
	RateCheckedAt            string        `json:"rate_checked_at"`
	RateSource               string        `json:"rate_source"`
	SupervisorID             string        `json:"supervisor_id"`
	ApprovalExpiresAt        time.Time     `json:"approval_expires_at"`
	AllowSupervisedException bool          `json:"allow_supervised_exception"`
	Rescue                   *RescuePolicy `json:"rescue,omitempty"`
}
type DurableJournal struct {
	Store     *ledger.Store
	config    Config
	policy    Policy
	accountID string
}

func digest(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// Bind the allowance to the allocation project, not mutable image/network choices.
func accountID(c Config) string { return "js2-" + digest(c.ProjectID)[:32] }
func (p Policy) validate() error {
	if e := p.Config.Validate(); e != nil {
		return e
	}
	if p.Rescue != nil {
		if e := p.Rescue.validate(p.Config); e != nil {
			return e
		}
	}
	if p.BudgetMilliSU < 1 || p.BudgetMilliSU > 1000000000 || p.RateMilliSUPerHour < 1 || p.RateMilliSUPerHour > 1000000000 || !provider.ValidID(p.SupervisorID) || !p.AllowSupervisedException || p.ApprovalExpiresAt.IsZero() {
		return errors.New("finite native budget, rate and supervised-experiment permit required")
	}
	if _, e := time.Parse("2006-01-02", p.RateCheckedAt); e != nil {
		return errors.New("dated rate required")
	}
	return httpsURL(p.RateSource)
}
func ConfigureJournal(s *ledger.Store, p Policy) (*DurableJournal, error) {
	if e := p.validate(); e != nil {
		return nil, e
	}
	if p.Rescue != nil {
		if e := s.View(func(d ledger.Data) error {
			if d.Authority == nil || d.Authority.ID != p.Rescue.AuthorityID {
				return errors.New("rescue source authority mismatch")
			}
			return nil
		}); e != nil {
			return nil, e
		}
	}
	b, _ := json.Marshal(p)
	if e := s.InitNativeAccount(ledger.NativeAccount{ID: accountID(p.Config), Provider: "jetstream2", Unit: "milli_su", Scope: p.Config.ProjectID, Budget: p.BudgetMilliSU, Policy: b}); e != nil {
		return nil, e
	}
	return OpenJournal(s, p.Config)
}
func OpenJournal(s *ledger.Store, c Config) (*DurableJournal, error) {
	if e := c.Validate(); e != nil {
		return nil, e
	}
	j := &DurableJournal{Store: s, config: c, accountID: accountID(c)}
	e := s.View(func(d ledger.Data) error {
		a, ok := d.NativeAccounts[j.accountID]
		if !ok {
			return errors.New("native account is not configured; live creation remains disabled")
		}
		if e := strict(a.Policy, &j.policy); e != nil {
			return e
		}
		if e := j.policy.validate(); e != nil {
			return e
		}
		if j.policy.Config != c || a.Budget != j.policy.BudgetMilliSU || a.Provider != "jetstream2" || a.Unit != "milli_su" || a.Scope != c.ProjectID {
			return errors.New("native account binding mismatch")
		}
		return nil
	})
	return j, e
}
func strict(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return e
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing native state")
	}
	return nil
}
func (j *DurableJournal) decode(n ledger.NativeRecord) (WorkerRecord, error) {
	var r WorkerRecord
	if e := strict(n.State, &r); e != nil {
		return r, e
	}
	h := sha256.Sum256([]byte(r.Request.Owner + ":" + r.Request.RequestID))
	if n.AccountID != j.accountID || n.RequestID != r.Request.RequestID || n.Owner != r.Request.Owner || n.ReservationID != r.ReservationID || n.PayloadHash != digest(r.Request) || n.Reserved != r.ReservedMilliSU || n.Reserved != r.Request.CeilingMilliSU || !n.CreatedAt.Equal(r.CreatedAt) || !n.Deadline.Equal(r.Deadline) || !r.Deadline.Equal(r.CreatedAt.Add(time.Duration(r.Request.MaxSeconds)*time.Second)) || r.ConfigHash != digest(j.config) || r.Name != "outfitter-"+hex.EncodeToString(h[:16]) || r.ProjectID != j.config.ProjectID || r.Region != j.config.Region || r.ImageID != j.config.ImageID || r.FlavorID != j.config.FlavorID || !r.SupervisorApproved || n.CleanupVerified != r.CleanupVerified || n.Ambiguous != r.CreateAmbiguous {
		return r, errors.New("native adapter identity mismatch")
	}
	if r.Request.MaxSeconds < 1 || r.Request.MaxSeconds > 3600 || r.ReservedMilliSU < (j.policy.RateMilliSUPerHour*int64(r.Request.MaxSeconds)+3599)/3600 || r.Deadline.After(j.policy.ApprovalExpiresAt) || (r.ServerAttempted && (!r.PortAttempted || r.PortID == "")) || (r.ServerID != "" && !r.ServerAttempted) || (r.PortID != "" && !r.PortAttempted) {
		return r, errors.New("native attempt or reservation mismatch")
	}
	if (r.PortID != "" && !ref.MatchString(r.PortID)) || (r.ServerID != "" && !ref.MatchString(r.ServerID)) {
		return r, errors.New("invalid native resource ID")
	}
	if r.Status != "reserved" && r.Status != "creating" && r.Status != "provisioning" && r.Status != "unknown" && r.Status != "deleted" {
		return r, errors.New("invalid native lifecycle")
	}
	if r.CleanupVerified && r.Status != "deleted" {
		return r, errors.New("cleanup status mismatch")
	}
	if !r.UnresolvedLiability || r.AccountingStatus != "reserved_unsettled" || r.SettledMilliSU != nil || r.SettlementEvidence != "" {
		return r, errors.New("adapter cannot settle native charges")
	}
	if r.ExperimentDigest != n.ExperimentDigest || (n.ExperimentDigest != "" && (r.Rescue == nil || r.EvidenceScope != "synthetic_offline" || r.LiveReadiness != "BLOCKED" || r.GuestExecution != "NOT_RUN")) || (n.ExperimentDigest == "" && (r.EvidenceScope != "" || r.LiveReadiness != "" || r.GuestExecution != "")) {
		return r, errors.New("native experiment record mismatch")
	}
	// Settlement belongs to the envelope; it never rewrites provider evidence.
	if e := validateRescueRecord(j.policy.Rescue, j.config, r); e != nil {
		return r, e
	}
	if n.Settled != nil {
		r.AccountingStatus = "provider_settled"
		r.UnresolvedLiability = false
		r.SettledMilliSU = n.Settled
		r.SettlementEvidence = n.SettlementEvidence
	}
	return r, nil
}
func (j *DurableJournal) Admit(ctx context.Context, w WorkerRequest) (WorkerRecord, bool, error) {
	var r WorkerRecord
	fresh := false
	if e := ctx.Err(); e != nil {
		return r, false, e
	}
	e := j.Store.Update(func(d *ledger.Data) error {
		if e := ledger.RequireAdmission(*d); e != nil {
			return e
		}
		if j.policy.Rescue != nil && (d.Authority == nil || d.Authority.ID != j.policy.Rescue.AuthorityID) {
			return errors.New("rescue authority cannot take over")
		}
		if !provider.ValidID(w.RequestID) || w.Owner != d.Owner || w.MaxSeconds < 1 || w.MaxSeconds > 3600 || w.CeilingMilliSU < 1 || w.CeilingMilliSU > 1000000000 {
			return errors.New("invalid native request or owner")
		}
		experiment, e := j.experiment(*d)
		if e != nil {
			return e
		}
		if experiment != nil && (w != experiment.Request || d.Authority.ID != experiment.AuthorityID) {
			return errors.New("EXPERIMENT_AUTHORITY_OR_REQUEST_MISMATCH")
		}
		if n, ok := d.NativeRuns[w.RequestID]; ok {
			var e error
			r, e = j.decode(n)
			if e != nil {
				return e
			}
			if r.Request != w {
				return errors.New("native request payload collision")
			}
			if experiment != nil && r.ReservationID != experiment.ReservationID {
				return errors.New("EXPERIMENT_RESERVATION_MISMATCH")
			}
			return nil
		}
		if len(d.NativeRuns)+len(d.Runs) >= ledger.MaxRecords {
			return errors.New("native request capacity reached")
		}
		if _, ok := d.Runs[w.RequestID]; ok {
			return errors.New("local/native request collision")
		}
		for _, n := range d.NativeRuns {
			if n.Settled == nil {
				return errors.New("native concurrency one: unresolved request remains")
			}
			if *n.Settled > n.Reserved {
				return errors.New("native overrun requires operator review")
			}
		}
		u := ledger.NativeCalculate(*d, j.accountID)
		if w.CeilingMilliSU > u.Remaining || w.CeilingMilliSU < (j.policy.RateMilliSUPerHour*int64(w.MaxSeconds)+3599)/3600 {
			return errors.New("insufficient native allowance or lifetime reservation")
		}
		now := time.Now().UTC()
		deadline := now.Add(time.Duration(w.MaxSeconds) * time.Second)
		if experiment != nil {
			if d.NativeAccounts[j.accountID].Experiment.Consumed {
				return errors.New("EXPERIMENT_ALREADY_CONSUMED")
			}
			if e := j.checkExperiment(*d, experiment, w, now, deadline); e != nil {
				return e
			}
		}
		if deadline.After(j.policy.ApprovalExpiresAt) {
			return errors.New("supervisor approval expires before request deadline")
		}
		var token [16]byte
		if _, e := rand.Read(token[:]); e != nil {
			return e
		}
		h := sha256.Sum256([]byte(w.Owner + ":" + w.RequestID))
		r = WorkerRecord{Request: w, ProjectID: j.config.ProjectID, Region: j.config.Region, ImageID: j.config.ImageID, FlavorID: j.config.FlavorID, ConfigHash: digest(j.config), ReservationID: hex.EncodeToString(token[:]), ReservedMilliSU: w.CeilingMilliSU, CreatedAt: now, Deadline: deadline, SupervisorApproved: true, Name: "outfitter-" + hex.EncodeToString(h[:16]), Status: "reserved", AccountingStatus: "reserved_unsettled", UnresolvedLiability: true}
		b, _ := json.Marshal(r)
		if j.policy.Rescue != nil {
			r.Rescue = &RescueRecord{Policy: *j.policy.Rescue}
			b, _ = json.Marshal(r)
		}
		if experiment != nil {
			a := d.NativeAccounts[j.accountID]
			a.Experiment.Consumed = true
			d.NativeAccounts[j.accountID] = a
			r.ReservationID = experiment.ReservationID
			r.ExperimentDigest = a.Experiment.SHA256
			r.EvidenceScope = "synthetic_offline"
			r.LiveReadiness = "BLOCKED"
			r.GuestExecution = "NOT_RUN"
			b, _ = json.Marshal(r)
		}
		if d.NativeRuns == nil {
			d.NativeRuns = map[string]ledger.NativeRecord{}
		}
		d.NativeRuns[w.RequestID] = ledger.NativeRecord{ExperimentDigest: r.ExperimentDigest, RequestID: w.RequestID, AccountID: j.accountID, Owner: w.Owner, ReservationID: r.ReservationID, PayloadHash: digest(w), CreatedAt: now, Deadline: deadline, Reserved: w.CeilingMilliSU, State: b, Revision: 1}
		fresh = true
		return nil
	})
	return r, fresh && e == nil, e
}
func (j *DurableJournal) Lookup(ctx context.Context, id string) (WorkerRecord, error) {
	var r WorkerRecord
	if e := ctx.Err(); e != nil {
		return r, e
	}
	e := j.Store.View(func(d ledger.Data) error {
		n, ok := d.NativeRuns[id]
		if !ok {
			return errors.New("unknown native request")
		}
		var e error
		r, e = j.decode(n)
		return e
	})
	return r, e
}
func (j *DurableJournal) Save(ctx context.Context, r WorkerRecord) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	return j.Store.Update(func(d *ledger.Data) error {
		n, ok := d.NativeRuns[r.Request.RequestID]
		if !ok {
			return errors.New("missing native intent")
		}
		old, e := j.decode(n)
		if e != nil {
			return e
		}
		if n.Settled != nil {
			return errors.New("settled native record is immutable")
		}
		if (old.PortID != "" && old.PortID != r.PortID) || (old.ServerID != "" && old.ServerID != r.ServerID) || (old.PortAttempted && !r.PortAttempted) || (old.ServerAttempted && !r.ServerAttempted) || (old.CreateAmbiguous && !r.CreateAmbiguous) {
			return errors.New("native effects cannot be forgotten or replaced")
		}
		n.CleanupVerified = r.CleanupVerified
		if old.Rescue != nil {
			if r.Rescue == nil {
				return errors.New("cleanup executor cannot change")
			}
			for _, pair := range [][2]string{{old.Rescue.PortScope, r.Rescue.PortScope}, {old.Rescue.PortACK, r.Rescue.PortACK}, {old.Rescue.ServerScope, r.Rescue.ServerScope}, {old.Rescue.ServerACK, r.Rescue.ServerACK}} {
				if pair[0] != "" && pair[0] != pair[1] {
					return errors.New("rescue evidence cannot be replaced")
				}
			}
		}
		n.Ambiguous = r.CreateAmbiguous
		n.Revision++
		n.State, e = json.Marshal(r)
		if e != nil {
			return e
		}
		if _, e = j.decode(n); e != nil {
			return e
		}
		d.NativeRuns[r.Request.RequestID] = n
		return nil
	})
}

// LockWorker survives requester death via OS-released advisory locks. It shares
// the existing cutover lease and is intentionally local-filesystem-only.
func (j *DurableJournal) LockWorker(ctx context.Context, id string) (func(), error) {
	if !provider.ValidID(id) {
		return nil, errors.New("invalid request ID")
	}
	release, e := j.Store.ExecutionLease()
	if e != nil {
		return nil, e
	}
	fd, e := syscall.Open(filepath.Join(j.Store.Dir, "native-"+id+".lock"), syscall.O_RDWR|syscall.O_CREAT|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
	if e != nil {
		release()
		return nil, e
	}
	for {
		e = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
		if e == nil {
			break
		}
		if e != syscall.EWOULDBLOCK && e != syscall.EAGAIN {
			syscall.Close(fd)
			release()
			return nil, e
		}
		select {
		case <-ctx.Done():
			syscall.Close(fd)
			release()
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return func() { syscall.Flock(fd, syscall.LOCK_UN); syscall.Close(fd); release() }, nil
}
func (j *DurableJournal) Pending(ctx context.Context) ([]WorkerRecord, error) {
	out := []WorkerRecord{}
	e := j.Store.View(func(d ledger.Data) error {
		for _, n := range d.NativeRuns {
			if n.AccountID != j.accountID || n.Settled != nil {
				continue
			}
			r, e := j.decode(n)
			if e != nil {
				return e
			}
			out = append(out, r)
		}
		return ctx.Err()
	})
	sort.Slice(out, func(i, k int) bool { return out[i].Request.RequestID < out[k].Request.RequestID })
	return out, e
}
