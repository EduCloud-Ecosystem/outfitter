package broker

import (
	"context"
	"sort"
	"time"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

// Finalize observes/tears down only exact owned resources. No lifecycle marker,
// lease availability, process death or resource absence can confirm an unknown
// reader/collector close or clear a possibly late create/start.
func (b *Broker) assessmentCleanupPhases(ctx context.Context, c x.Control) error {
	if b.assessment == nil {
		return x.ErrHeld
	}
	s, a, e := b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil {
		return e
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for _, phase := range []string{"workload", "checker"} {
		p := a.PhaseRecords.Workload
		if phase == "checker" {
			p = a.PhaseRecords.Checker
		}
		if p == nil || !p.CreateClaimed {
			continue
		}
		req, re := assessmentPhaseRequestFor(s, a, phase)
		if re != nil {
			continue
		}
		obs, oe := b.assessment.provider.Inspect(cleanupCtx, req)
		if oe != nil {
			continue
		}
		identity := obs.Identity
		owned := owneddata.Hex(identity.ContainerID, 32) && identity.BrokerOwner == a.Binding.BrokerAuthorityID && identity.AttemptID == a.Slot.AttemptID && identity.Phase == phase && identity.ScopeID == s.ScopeID && identity.CapabilitySHA256 == s.CapabilitySHA256 && (p.ResourceIdentity == nil || *p.ResourceIdentity == identity)
		change := func(fn func(*x.PhaseRecord)) error {
			_, e := b.assessmentChangeAttempt(a.Binding, a.Slot, x.AttemptPhaseSafety, func(v *x.AssessmentAttempt) error {
				target := v.PhaseRecords.Workload
				if phase == "checker" {
					target = v.PhaseRecords.Checker
				}
				if target == nil {
					return x.ErrHeld
				}
				fn(target)
				return nil
			})
			return e
		}
		if obs.Exists {
			if !owned {
				continue
			}
			// Preserve ambiguity even when a late object is discovered and removed.
			// Resource identity is a late bind only after a definitive create return;
			// an ambiguous create can still be cleaned using this exact observation.
			if p.ResourceIdentity == nil && p.CreateReturned {
				if change(func(v *x.PhaseRecord) { v.ResourceIdentity = &identity }) != nil {
					continue
				}
			}
			if !obs.Stopped {
				if b.assessment.provider.Stop(cleanupCtx, identity) != nil {
					continue
				}
				obs, oe = b.assessment.provider.Inspect(cleanupCtx, req)
				if oe != nil || obs.Identity != identity || !obs.Stopped {
					continue
				}
			}
			if change(func(v *x.PhaseRecord) { v.StoppedObserved = true }) != nil {
				continue
			}
			if b.assessment.provider.Remove(cleanupCtx, identity) != nil {
				continue
			}
			obs, oe = b.assessment.provider.Inspect(cleanupCtx, req)
			if oe != nil || obs.Exists {
				continue
			}
		}
		if !obs.Exists && p.CreateReturned && p.ResourceIdentity != nil && !p.CreateAmbiguous {
			if change(func(v *x.PhaseRecord) { v.StoppedObserved = true }) != nil {
				continue
			}
			_ = change(func(v *x.PhaseRecord) { v.RemovalObserved = true })
		}
	}
	return nil
}

func (b *Broker) assessmentFinalize(ctx context.Context, c x.Control) error {
	_, a, e := b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil {
		return e
	}
	if a.State != "held" && a.State != "terminal" {
		a, e = b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptFinish, func(v *x.AssessmentAttempt) error {
			if v.State == "held" || v.State == "terminal" {
				return nil
			}
			v.State = "held"
			return nil
		})
		if e != nil {
			return e
		}
	}
	// Successful mutation ACKs plan the next branch only. Each later mutation
	// rechecks its actual current record; the final clearance read stays fresh.
	if a.EffectsClosed() && a.AccountingUnresolved {
		a, e = b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptSettle, func(v *x.AssessmentAttempt) error {
			if !v.EffectsClosed() || v.State != "held" && v.State != "terminal" {
				return x.ErrHeld
			}
			if !v.AccountingUnresolved {
				if !v.ComputeCleanupObserved || v.Compute.ChargedCPUSeconds != v.Compute.ReservedCPUSeconds || v.Compute.Provenance != "estimated_upper_bound" {
					return x.ErrHeld
				}
				return nil
			}
			if v.ComputeCleanupObserved || v.Compute.ChargedCPUSeconds != 0 || v.Compute.Provenance != "reserved" {
				return x.ErrHeld
			}
			v.ComputeCleanupObserved = true
			v.AccountingUnresolved = false
			v.Compute.ChargedCPUSeconds = v.Compute.ReservedCPUSeconds
			v.Compute.Provenance = "estimated_upper_bound"
			return nil
		})
		if e != nil {
			return e
		}
	}
	if a.EffectsClosed() && a.OwnershipCloseConfirmed && !a.OwnershipCloseUncertain && !a.AccountingUnresolved && a.State != "terminal" {
		_, e = b.assessmentChangeAttempt(c.Binding, *c.Slot, x.AttemptFinish, func(v *x.AssessmentAttempt) error {
			if v.State == "terminal" {
				return nil
			}
			if !v.EffectsClosed() || !v.OwnershipCloseConfirmed || v.OwnershipCloseUncertain || v.AccountingUnresolved || !v.ComputeCleanupObserved || v.State != "held" && v.State != "finishing" {
				return x.ErrHeld
			}
			v.State = "terminal"
			if v.DenyLatched {
				if v.FailureCode == "cancelled" {
					v.Outcome = "cancelled"
				} else {
					v.Outcome = "failed"
				}
			} else if v.Quality.Status == "passed" && v.PhaseRecords.Workload != nil && v.PhaseRecords.Workload.ExitCode != nil && *v.PhaseRecords.Workload.ExitCode == 0 && v.PhaseRecords.Checker != nil && v.PhaseRecords.Checker.ExitCode != nil && *v.PhaseRecords.Checker.ExitCode == 0 {
				v.Outcome = "succeeded"
			}
			return nil
		})
		if e != nil {
			return e
		}
	}
	_, a, e = b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil {
		return e
	}
	if !a.SerialClear() {
		return x.ErrHeld
	}
	return nil
}

// A slot-specific metadata operation never removes its retained input scope or
// touches another attempt. Concurrent effect ownership prevents close proof.
func (b *Broker) assessmentCleanupAttempt(ctx context.Context, c x.Control) error {
	_, a, e := b.assessmentAttempt(c.Binding, *c.Slot)
	if e != nil {
		return e
	}
	// A new invocation has no original live ownership. Even a free lease cannot
	// certify an old reader, callback or ownership release; never reacquire.
	if !a.SerialClear() {
		return x.ErrHeld
	}
	return nil
}

func (b *Broker) assessmentCleanup(ctx context.Context, binding x.ScopeBinding) error {
	s, _, e := b.assessmentRead(binding)
	if e != nil {
		return e
	}
	if !s.DenyLatched {
		return nil
	}
	if s.CleanupOwnershipCloseConfirmed {
		for _, o := range s.Observations {
			if o.OwnedRemoval == "observed_absent" && !s.LeaseCloseUncertain {
				return nil
			}
		}
		return x.ErrHeld
	}
	if b.assessment == nil || b.assessment.owned == nil {
		return x.ErrHeld
	}
	// Known normal activity is refused without an epoch, descriptor or poll
	// observation. The same metadata predicate repeats atomically in BEGIN.
	if b.Store.AssessmentCleanupReadyV7(binding) != nil {
		return x.ErrHeld
	}
	c := x.Control{Binding: binding}
	_, owner, e := b.Store.BeginAssessmentScopeRoleV7(c, "cleanup", b.assessmentRoots(), b.assessmentBeginGuard(binding, true))
	if e != nil {
		return assessmentError(e)
	}
	defer owner.CloseAndCommitOwnership()
	if owner.AcquireExecutionSet() != nil || owner.FreshCurrent(b.assessmentBeginGuard(binding, true)) != nil {
		return x.ErrHeld
	}
	if b.assessment.storage == nil || b.assessment.storage.Remove(ctx, s) != nil {
		return x.ErrHeld
	}
	return owner.RemoveOwnedScopeAndClose()
}
func (b *Broker) assessmentObserveScope(binding x.ScopeBinding, writers, readers, collector, removal, reason string) error {
	if _, e := b.assessmentChangeScope(binding, x.ScopeUnresolved, func(s *x.ExecutionScope) error {
		if s.State == "cleaning" {
			s.State = "cleanup_unresolved"
		}
		return nil
	}); e != nil {
		return e
	}
	id, e := ownedID()
	if e != nil {
		return x.ErrHeld
	}
	e = b.Store.Update(func(d *ledger.Data) error {
		if d.Version != 7 || d.AssessmentExecution == nil {
			return x.ErrDenied
		}
		s, ok := d.AssessmentExecution.Scopes[binding.ScopeID]
		if !ok {
			return x.ErrDenied
		}
		n, ne := b.assessmentNow()
		at := s.WallHighWater
		if ne == nil && owneddata.Stamp(n.Wall) > at {
			at = owneddata.Stamp(n.Wall)
		}
		revocation := "unknown"
		if writers == "observed" && readers == "observed" && collector == "observed" {
			revocation = "observed"
		}
		o := x.ScopeObservation{Schema: x.ScopeObservationSchema, ObservationID: id, ScopeID: s.ScopeID, ObservedAt: at, Method: "synthetic_execution_scope_v1", Assurance: "synthetic", NewAdmission: "denied", WritersDrained: writers, ReadersDrained: readers, CollectorDrained: collector, ScopeAccessRevocation: revocation, OwnedRemoval: removal, Reason: reason}
		next, e := x.AddScopeObservation(s, o)
		if e != nil {
			return e
		}
		d.AssessmentExecution.Scopes[s.ScopeID] = next
		return nil
	})
	return assessmentError(e)
}

// AssessmentReap is metadata-only when the production boundary is absent. It
// never settles a frozen ordinary/native liability or activates v6 data scopes.
func (b *Broker) AssessmentReap(ctx context.Context) error {
	var scopes []x.ExecutionScope
	e := b.Store.View(func(d ledger.Data) error {
		if d.Version != 7 || d.AssessmentExecution == nil {
			return x.ErrDenied
		}
		for _, s := range d.AssessmentExecution.Scopes {
			scopes = append(scopes, s)
		}
		return nil
	})
	if e != nil {
		return assessmentError(e)
	}
	sort.Slice(scopes, func(i, j int) bool { return scopes[i].ScopeID < scopes[j].ScopeID })
	for _, s := range scopes {
		if ctx.Err() != nil {
			return x.ErrHeld
		}
		reason := "restart"
		if b.assessment != nil {
			n, ne := b.assessmentNow()
			if ne == nil {
				reason = ownedClockReason(s.OwnedProjection(), n)
				if reason == "" && b.assessment.current(s, n) != nil {
					reason = "owner_mismatch"
				}
			}
		}
		if reason != "" && !s.DenyLatched {
			_, _ = b.assessmentDeny(s.Binding(), reason)
		}
		_ = b.assessmentStopDeniedAttempts(ctx, s.Binding())
		if e = b.assessmentCleanup(ctx, s.Binding()); e != nil {
			return e
		}
	}
	return nil
}

func (b *Broker) AssessmentWatch(ctx context.Context, binding x.ScopeBinding) (err error) {
	if !b.assessmentAvailable() {
		return x.ErrDisabled
	}
	s, _, e := b.assessmentRead(binding)
	if e != nil {
		return e
	}
	owner := b.assessment.watchOwner
	if owner == nil {
		return x.ErrHeld
	}
	defer func() {
		if e := owner.CloseAndCommitOwnership(); e != nil {
			_, _ = b.assessmentDeny(binding, "io_uncertain")
			err = x.ErrHeld
		}
		_ = b.assessmentCleanup(context.WithoutCancel(ctx), binding)
	}()
	if e = owner.PublishWatchReady(b.assessmentBeginGuard(binding, false)); e != nil {
		_, _ = b.assessmentDeny(binding, "io_uncertain")
		return e
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	high, _ := b.assessmentNow()
	for {
		select {
		case <-ctx.Done():
			_, _ = b.assessmentDeny(binding, "restart")
			_ = b.assessmentStopDeniedAttempts(context.WithoutCancel(ctx), binding)
			return x.ErrHeld
		case <-ticker.C:
		}
		if b.assessmentFault("watch-before-snapshot") != nil {
			return x.ErrHeld
		}
		var n ownedClock
		reason := ""
		// Sample after obtaining this same authority snapshot. A pre-lock sample
		// can precede a legitimate concurrent writer's committed high-water mark.
		// Process-local high-water still detects genuine wall/monotonic rollback.
		e = b.Store.View(func(d ledger.Data) error {
			if d.Version != 7 || d.AssessmentExecution == nil {
				return x.ErrDenied
			}
			var ok bool
			s, ok = d.AssessmentExecution.Scopes[binding.ScopeID]
			if !ok || s.Binding() != binding {
				return x.ErrCollision
			}
			var ne error
			n, ne = b.assessmentNow()
			if ne != nil {
				reason = "io_uncertain"
			} else if n.Epoch != high.Epoch || n.Wall.Before(high.Wall) || n.TickMS < high.TickMS {
				reason = "clock_rollback"
			} else {
				high = n
			}
			if reason == "" {
				reason = ownedClockReason(s.OwnedProjection(), n)
			}
			if reason == "" && b.assessment.current(s, n) != nil {
				reason = "owner_mismatch"
			}
			return nil
		})
		if e != nil {
			return e
		}
		if reason != "" && !s.DenyLatched {
			_, _ = b.assessmentDeny(binding, reason)
		}
		_ = b.assessmentStopDeniedAttempts(ctx, binding)
		if s.DenyLatched || reason != "" {
			return x.ErrHeld
		}
	}
}
