package broker

import (
	"reflect"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

func (b *Broker) assessmentRead(binding x.ScopeBinding) (x.ExecutionScope, *x.AssessmentAttempt, error) {
	var s x.ExecutionScope
	if binding.Validate() != nil {
		return s, nil, x.ErrInvalid
	}
	e := b.Store.View(func(d ledger.Data) error {
		if d.Version != 7 || d.AssessmentExecution == nil || d.Authority.ID != binding.BrokerAuthorityID || d.AssessmentExecution.Identity.WaypointAuthorityID != binding.WaypointAuthorityID {
			return x.ErrDenied
		}
		var ok bool
		s, ok = d.AssessmentExecution.Scopes[binding.ScopeID]
		if !ok || !reflect.DeepEqual(s.Binding(), binding) {
			return x.ErrCollision
		}
		if b.assessment != nil && assessmentRootMetadata(b.assessment.owned.stateRoot, d.AssessmentExecution.Identity.Directory) != nil {
			return x.ErrHeld
		}
		return nil
	})
	return s, nil, assessmentError(e)
}
func (b *Broker) assessmentAttempt(binding x.ScopeBinding, slot x.Slot) (x.ExecutionScope, x.AssessmentAttempt, error) {
	var s x.ExecutionScope
	var a x.AssessmentAttempt
	e := b.Store.View(func(d ledger.Data) error {
		if d.Version != 7 || d.AssessmentExecution == nil {
			return x.ErrDenied
		}
		var ok bool
		s, ok = d.AssessmentExecution.Scopes[binding.ScopeID]
		if !ok || !reflect.DeepEqual(s.Binding(), binding) {
			return x.ErrCollision
		}
		a, ok = d.AssessmentExecution.Attempts[slot.AttemptID]
		if !ok || a.Slot != slot || !reflect.DeepEqual(a.Binding, binding) {
			return x.ErrCollision
		}
		return nil
	})
	return s, a, assessmentError(e)
}
func (b *Broker) assessmentChangeScope(binding x.ScopeBinding, event x.Event, mutate func(*x.ExecutionScope) error) (x.ExecutionScope, error) {
	var out x.ExecutionScope
	if b.assessmentFault("before-journal:"+string(event)) != nil {
		return out, x.ErrHeld
	}
	refused := false
	e := b.Store.Update(func(d *ledger.Data) error {
		if d.Version != 7 || d.AssessmentExecution == nil {
			return x.ErrDenied
		}
		s, ok := d.AssessmentExecution.Scopes[binding.ScopeID]
		if !ok || !reflect.DeepEqual(s.Binding(), binding) {
			return x.ErrCollision
		}
		n, clockErr := b.assessmentNow()
		at, tick := s.WallHighWater, s.TickHighWaterMS
		if clockErr == nil && n.Epoch == s.ClockEpoch {
			at = owneddata.Stamp(n.Wall)
			tick = n.TickMS
		}
		needed := int64(1)
		if event == x.ScopeWriterClaim {
			needed = 6
		}
		if scopeForwardEvent(event, s) && (x.ScopeNormalCapacity-s.NormalTransitionsUsed < needed || clockErr != nil || b.assessment == nil || b.assessment.current(s, n) != nil) {
			var e error
			out, e = executionDenyValue(s, "io_uncertain", at, tick)
			if e != nil {
				return e
			}
			d.AssessmentExecution.Scopes[s.ScopeID] = out
			refused = true
			return nil
		}
		next := x.Clone(s)
		if e := mutate(&next); e != nil {
			return e
		}
		var e error
		out, e = x.AdvanceScope(s, event, at, tick, func(v *x.ExecutionScope) { *v = next })
		if e != nil {
			return e
		}
		d.AssessmentExecution.Scopes[s.ScopeID] = out
		return nil
	})
	if e == nil && b.assessmentFault("after-journal:"+string(event)) != nil {
		return out, x.ErrHeld
	}
	if e == nil && refused {
		return out, x.ErrDenied
	}
	return out, assessmentError(e)
}
func (b *Broker) assessmentChangeAttempt(binding x.ScopeBinding, slot x.Slot, event x.Event, mutate func(*x.AssessmentAttempt) error) (x.AssessmentAttempt, error) {
	var out x.AssessmentAttempt
	if b.assessmentFault("before-journal:"+string(event)) != nil {
		return out, x.ErrHeld
	}
	refused := false
	e := b.Store.Update(func(d *ledger.Data) error {
		if d.Version != 7 || d.AssessmentExecution == nil {
			return x.ErrDenied
		}
		s, ok := d.AssessmentExecution.Scopes[binding.ScopeID]
		if !ok || !reflect.DeepEqual(s.Binding(), binding) {
			return x.ErrCollision
		}
		a, ok := d.AssessmentExecution.Attempts[slot.AttemptID]
		if !ok || a.Slot != slot {
			return x.ErrCollision
		}
		n, clockErr := b.assessmentNow()
		at, tick := a.WallHighWater, a.TickHighWaterMS
		if clockErr == nil && n.Epoch == a.ClockEpoch {
			at = owneddata.Stamp(n.Wall)
			tick = n.TickMS
		}
		if attemptForwardEvent(event, a) && (a.NormalTransitionsUsed >= x.AttemptNormalCapacity || a.OwnershipCloseUncertain || a.OwnershipCloseConfirmed || clockErr != nil || b.assessment == nil || b.assessment.current(s, n) != nil || n.TickMS >= a.DeadlineTickMS || at[:19]+"Z" >= a.Deadline || at < a.WallHighWater || tick < a.TickHighWaterMS) {
			denied, e := executionDenyValue(s, "io_uncertain", at, tick)
			if e != nil {
				return e
			}
			d.AssessmentExecution.Scopes[s.ScopeID] = denied
			out, e = x.AdvanceAttempt(a, s, x.AttemptDeny, at, tick, func(v *x.AssessmentAttempt) {
				if !v.DenyLatched {
					v.DenyLatched = true
					v.FailureCode = "io_uncertain"
				}
			})
			if e != nil {
				return e
			}
			d.AssessmentExecution.Attempts[slot.AttemptID] = out
			refused = true
			return nil
		}
		next := x.Clone(a)
		if e := mutate(&next); e != nil {
			return e
		}
		var e error
		out, e = x.AdvanceAttempt(a, s, event, at, tick, func(v *x.AssessmentAttempt) { *v = next })
		if e != nil {
			return e
		}
		d.AssessmentExecution.Attempts[slot.AttemptID] = out
		return nil
	})
	if e == nil && b.assessmentFault("after-journal:"+string(event)) != nil {
		return out, x.ErrHeld
	}
	if e == nil && refused {
		return out, x.ErrDenied
	}
	return out, assessmentError(e)
}
func scopeForwardEvent(event x.Event, s x.ExecutionScope) bool {
	switch event {
	case x.ScopeIdentity, x.ScopeContentIdentity, x.ScopeContentClosed:
		return !s.DenyLatched
	case x.ScopeWatchClaim, x.ScopeWatchReady, x.ScopeWriterClaim, x.ScopeSealBegin, x.ScopeUploadComplete, x.ScopeSeal, x.ScopeSlotClaim:
		return true
	}
	return false
}
func attemptForwardEvent(event x.Event, a x.AssessmentAttempt) bool {
	switch event {
	case x.AttemptReader, x.AttemptCollector, x.AttemptPhase, x.AttemptPhaseForward, x.AttemptForward:
		return true
	case x.AttemptMetrics, x.AttemptQuality:
		return a.State != "held" && a.State != "finishing" && a.State != "terminal" && !a.DenyLatched
	}
	return false
}
func executionDenyValue(s x.ExecutionScope, reason, at string, tick int64) (x.ExecutionScope, error) {
	return x.AdvanceScope(s, x.ScopeDeny, at, tick, func(v *x.ExecutionScope) {
		if !v.DenyLatched {
			v.DenyLatched = true
			v.ExecutionDenyLatched = true
			v.State = "cleaning"
			v.Reason = reason
		}
	})
}
func (b *Broker) assessmentDeny(binding x.ScopeBinding, reason string) (x.ExecutionScope, error) {
	return b.assessmentChangeScope(binding, x.ScopeDeny, func(s *x.ExecutionScope) error {
		if s.DenyLatched {
			return nil
		}
		s.DenyLatched = true
		s.ExecutionDenyLatched = true
		s.State = "cleaning"
		s.Reason = reason
		return nil
	})
}
func (b *Broker) assessmentFresh(binding x.ScopeBinding, slot *x.Slot) (x.ExecutionScope, error) {
	var s x.ExecutionScope
	if !b.assessmentAvailable() {
		return s, x.ErrDisabled
	}
	e := b.Store.View(func(d ledger.Data) error {
		n, e := b.assessmentNow()
		if e != nil {
			return e
		}
		if d.Version != 7 || d.AssessmentExecution == nil {
			return x.ErrDenied
		}
		var ok bool
		s, ok = d.AssessmentExecution.Scopes[binding.ScopeID]
		if !ok || !reflect.DeepEqual(s.Binding(), binding) {
			return x.ErrCollision
		}
		if assessmentRootMetadata(b.assessment.owned.stateRoot, d.AssessmentExecution.Identity.Directory) != nil {
			return x.ErrHeld
		}
		if e := b.assessment.current(s, n); e != nil {
			return e
		}
		for _, prior := range d.AssessmentExecution.Attempts {
			if prior.Binding.ScopeID == s.ScopeID && (owneddata.Stamp(n.Wall) < prior.WallHighWater || n.TickMS < prior.TickHighWaterMS) {
				return x.ErrDenied
			}
		}
		if slot != nil {
			a, ok := d.AssessmentExecution.Attempts[slot.AttemptID]
			if !ok || a.Slot != *slot || a.DenyLatched || a.OwnershipCloseUncertain || a.OwnershipCloseConfirmed || a.State == "held" || a.State == "terminal" || a.State == "finishing" || n.TickMS >= a.DeadlineTickMS || owneddata.Stamp(n.Wall)[:19]+"Z" >= a.Deadline {
				return x.ErrDenied
			}
		}
		return nil
	})
	if e != nil {
		_, _ = b.assessmentDeny(binding, "io_uncertain")
	}
	return s, assessmentError(e)
}
