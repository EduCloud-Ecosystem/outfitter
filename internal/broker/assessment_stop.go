package broker

import (
	"context"
	"reflect"
	"sort"
	"time"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

// The existing watcher can stop an exact denied/expired resource while its
// execution owner still holds the reader lease. This does not remove data,
// publish close proof, settle accounting, or infer anything from a process ID.
func (b *Broker) assessmentStopDeniedAttempts(ctx context.Context, binding x.ScopeBinding) error {
	if b.assessment == nil || b.assessment.provider == nil {
		return x.ErrHeld
	}
	var attempts []x.AssessmentAttempt
	var scopeReason string
	e := b.Store.View(func(d ledger.Data) error {
		if d.Version != 7 || d.AssessmentExecution == nil {
			return x.ErrDenied
		}
		s, ok := d.AssessmentExecution.Scopes[binding.ScopeID]
		if !ok || s.Binding() != binding {
			return x.ErrCollision
		}
		if s.DenyLatched {
			scopeReason = s.Reason
		}
		for _, a := range d.AssessmentExecution.Attempts {
			if a.Binding == binding && !a.SerialClear() {
				attempts = append(attempts, a)
			}
		}
		return nil
	})
	if e != nil {
		return assessmentError(e)
	}
	sort.Slice(attempts, func(i, j int) bool { return attempts[i].Slot.AttemptID < attempts[j].Slot.AttemptID })
	cleanupCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for _, candidate := range attempts {
		if cleanupCtx.Err() != nil {
			return x.ErrHeld
		}
		// A fresh validated committed denial plans Stop-only rescue, never
		// claim continuation or positive closure/accounting authority. A new
		// denial still needs its own definitive ACK; errors never reread/fallback.
		a := candidate
		if !a.DenyLatched {
			a, e = b.assessmentChangeAttempt(binding, candidate.Slot, x.AttemptDeny, func(v *x.AssessmentAttempt) error {
				if v.DenyLatched {
					return nil
				}
				n, ne := b.assessmentNow()
				reason := ""
				if scopeReason != "" {
					reason = "access_ended"
					if scopeReason == "cancelled" || scopeReason == "expired" || scopeReason == "clock_rollback" {
						reason = scopeReason
					}
				}
				if reason != "" { /* absorbing scope denial already committed */
				} else if ne != nil {
					reason = "io_uncertain"
				} else if n.Epoch != v.ClockEpoch || n.Wall.Before(mustOwnedTime(v.WallHighWater)) || n.TickMS < v.TickHighWaterMS {
					reason = "clock_rollback"
				} else if n.TickMS >= v.DeadlineTickMS || owneddata.Stamp(n.Wall)[:19]+"Z" >= v.Deadline {
					reason = "expired"
				}
				if reason != "" {
					v.DenyLatched = true
					v.FailureCode = reason
				}
				return nil
			})
			if e != nil {
				continue
			}
		}
		if !a.DenyLatched {
			continue
		}
		// An attempt snapshot can be the first observer of actual rollback.
		// Latch the scope before stop; do not wait for the next watch tick.
		if a.FailureCode == "clock_rollback" {
			if _, e = b.assessmentDeny(binding, "clock_rollback"); e != nil {
				continue
			}
		}
		for _, phase := range []string{"workload", "checker"} {
			s, current, e := b.assessmentAttempt(binding, a.Slot)
			if e != nil {
				return e
			}
			p := current.PhaseRecords.Workload
			if phase == "checker" {
				p = current.PhaseRecords.Checker
			}
			if !current.DenyLatched || p == nil || !p.CreateClaimed || p.ResourceIdentity == nil || p.RemovalObserved {
				continue
			}
			// Exact generation/identity rechecks bracket each external wait. Resource
			// responses are never themselves persisted as reader/collector finality.
			generation := current.Generation
			identity := *p.ResourceIdentity
			req, e := assessmentPhaseRequestFor(s, current, phase)
			if e != nil {
				continue
			}
			unchanged := func() bool {
				_, latest, e := b.assessmentAttempt(binding, current.Slot)
				if e != nil || latest.Generation != generation || !latest.DenyLatched {
					return false
				}
				lp := latest.PhaseRecords.Workload
				if phase == "checker" {
					lp = latest.PhaseRecords.Checker
				}
				return lp != nil && lp.ResourceIdentity != nil && *lp.ResourceIdentity == identity && reflect.DeepEqual(latest.Binding, binding)
			}
			if !unchanged() {
				continue
			}
			obs, e := b.assessment.provider.Inspect(cleanupCtx, req)
			if e != nil || !obs.Exists || obs.Stopped || obs.Identity != identity || !unchanged() {
				continue
			}
			if e = b.assessment.provider.Stop(cleanupCtx, identity); e != nil {
				continue
			}
			_ = unchanged() // Changed concurrent completion is not an acknowledgement here.
		}
	}
	return nil
}
func mustOwnedTime(s string) time.Time { v, _ := owneddata.Time(s, true); return v }
