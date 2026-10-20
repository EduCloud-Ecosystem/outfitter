package ledger

import (
	"bytes"
	"encoding/json"
	"reflect"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

// This is the existing JSON authority. There is deliberately no v7 constructor,
// migration, grant, import, or activation operation. Only test code may seed it.
func executionIdentity(d Data) *OwnedDataIdentity {
	if d.AssessmentExecution == nil {
		return nil
	}
	i := d.AssessmentExecution.Identity
	return &OwnedDataIdentity{i.WaypointAuthorityID, i.Directory, i.JournalLease, i.ExecutionLease}
}
func executionCount(d Data) int {
	if d.AssessmentExecution == nil {
		return 0
	}
	n := 0
	for _, s := range d.AssessmentExecution.Scopes {
		n += 1 + len(s.Slots)
	}
	return n
}
func validateAssessmentExecution(d Data) error {
	if d.Version != 7 {
		if d.AssessmentExecution != nil {
			return x.ErrInvalid
		}
		return nil
	}
	a := d.AssessmentExecution
	if a == nil || a.Schema != x.AuthoritySchema || a.Scopes == nil || a.Attempts == nil || d.OwnedDataScopes != nil || d.OwnedDataIdentity != nil || d.Authority == nil || d.Authority.Admission != "active" || d.Authority.Restore != nil || d.Authority.Fence != nil {
		return x.ErrInvalid
	}
	i := a.Identity
	if !validHex(i.WaypointAuthorityID, 16) || !i.Directory.Valid() || !i.JournalLease.Valid() || !i.ExecutionLease.Valid() {
		return x.ErrInvalid
	}
	ids := map[string]bool{}
	assessments := map[string]bool{}
	planned := map[string]string{}
	for id, r := range d.Runs {
		ids[id] = true
		if r.Stdout != "" || r.Stderr != "" {
			return x.ErrInvalid
		}
	}
	for id := range d.NativeRuns {
		ids[id] = true
	}
	for id, s := range a.Scopes {
		if id != s.ScopeID || s.Validate() != nil || s.BrokerAuthorityID != d.Authority.ID || s.WaypointAuthorityID != i.WaypointAuthorityID || assessments[s.AssessmentID] || ids[id] {
			return x.ErrInvalid
		}
		if _, ok := d.Projects[s.ProjectID]; !ok {
			return x.ErrInvalid
		}
		assessments[s.AssessmentID] = true
		ids[id] = true
	}
	for id, s := range a.Scopes {
		for _, slot := range s.Slots {
			if ids[slot.AttemptID] || ids[slot.ReservationID] {
				return x.ErrCollision
			}
			ids[slot.AttemptID] = true
			ids[slot.ReservationID] = true
			planned[slot.AttemptID] = id
		}
	}
	for id, r := range a.Attempts {
		parent, ok := planned[id]
		if !ok || id != r.Slot.AttemptID || r.Validate(a.Scopes[parent]) != nil {
			return x.ErrInvalid
		}
	}
	return nil
}

type executionGuard struct {
	version   int
	frozen    []byte
	authority *x.Authority
}

func executionFrozen(d Data) []byte { d.AssessmentExecution = nil; b, _ := json.Marshal(d); return b }
func captureExecutionGuard(d Data) executionGuard {
	g := executionGuard{version: d.Version}
	if d.Version == 7 {
		g.frozen = executionFrozen(d)
		a := x.Clone(*d.AssessmentExecution)
		g.authority = &a
	}
	return g
}
func initialScope(s x.ExecutionScope) bool {
	if s.Validate() != nil {
		return false
	}
	f := s.FilesystemIdentity
	return s.Generation == 1 && s.NormalTransitionsUsed == 0 && s.SafetyTransitionsUsed == 0 && s.State == "preparing" && !s.DenyLatched && !s.ExecutionDenyLatched && !s.WriterStarted && !s.WatchStarted && !s.UploadComplete && !s.ContentCloseConfirmed && !s.ContentCloseUncertain && s.SealedLengthBytes == nil && len(s.Observations) == 0 && !s.ObservationsSaturated && f.Scope == nil && f.WriterLease == nil && f.WatchLease == nil && f.ExecutionLease == nil && f.Content == nil && f.WorkRoot == nil && f.OutputRoot == nil && !s.PrepareOwnershipCloseConfirmed && !s.WriterOwnershipCloseConfirmed && !s.SealStarted && !s.SealOwnershipCloseConfirmed && !s.WatchOwnershipCloseConfirmed && s.CleanupOwnerEpoch == nil && !s.CleanupOwnershipCloseConfirmed && !s.LeaseCloseUncertain
}
func initialAttempt(a x.AssessmentAttempt, s x.ExecutionScope) bool {
	return a.Validate(s) == nil && a.Generation == 1 && a.NormalTransitionsUsed == 0 && a.SafetyTransitionsUsed == 0 && a.State == "claimed" && a.Outcome == "unknown" && !a.DenyLatched && a.FailureCode == "none" && a.AccountingUnresolved && !a.ComputeCleanupObserved && !a.ReaderStarted && !a.CollectorStarted && !a.ReaderCloseConfirmed && !a.ReaderCloseUncertain && !a.CollectorCloseConfirmed && !a.CollectorCloseUncertain && a.PhaseRecords.Workload == nil && a.PhaseRecords.Checker == nil && reflect.DeepEqual(a.Metrics, x.UnknownMeasurements()) && a.Quality.Status == "unknown" && len(a.CleanupObservations) == 0 && !a.ObservationsSaturated && a.MeasurementDigest == nil && !a.OwnershipCloseConfirmed && !a.OwnershipCloseUncertain
}
func preserveExecutionGuard(g executionGuard, d Data, completion *assessmentCompletion) error {
	if g.version != 7 {
		if d.Version == 7 {
			return x.ErrDisabled
		}
		return nil
	}
	a := d.AssessmentExecution
	if d.Version != 7 || a == nil || !bytes.Equal(g.frozen, executionFrozen(d)) || a.Schema != g.authority.Schema || a.Identity != g.authority.Identity {
		return x.ErrInvalid
	}
	claims := map[string]int{}
	newClaims := 0
	for id, next := range a.Attempts {
		old, ok := g.authority.Attempts[id]
		if ok {
			if !old.OwnershipCloseConfirmed && next.OwnershipCloseConfirmed && !completion.permitsAttempt(next) {
				return x.ErrDenied
			}
			if x.AttemptTransition(old, next, a.Scopes[next.Binding.ScopeID]) != nil {
				return x.ErrInvalid
			}
			continue
		}
		s, exists := g.authority.Scopes[next.Binding.ScopeID]
		if !exists || s.State != "sealed" || s.DenyLatched || s.ExecutionDenyLatched || !s.ContentCloseConfirmed || s.ContentCloseUncertain || !initialAttempt(next, s) || CanClaimNextSlotV7(g.authority, d.Runs, d.NativeRuns) != nil {
			return x.ErrDenied
		}
		claims[s.ScopeID]++
		newClaims++
	}
	// One transaction can admit one irreversible attempt, never an unobserved
	// batch whose effects would race the serial-clearance predicate.
	if newClaims > 1 {
		return x.ErrDenied
	}
	for id := range g.authority.Attempts {
		if _, ok := a.Attempts[id]; !ok {
			return x.ErrInvalid
		}
	}
	for id, old := range g.authority.Scopes {
		next, ok := a.Scopes[id]
		if ok && old.CleanupOwnerEpoch == nil && next.CleanupOwnerEpoch != nil && !cleanupReadyV7(g.authority, old) {
			return x.ErrHeld
		}
		if ok && !completion.permitsScopeChanges(old, next) {
			return x.ErrDenied
		}
		if !ok || claims[id] > 1 || x.ScopeTransition(old, next, claims[id] == 1) != nil {
			return x.ErrInvalid
		}
	}
	for id, s := range a.Scopes {
		if _, ok := g.authority.Scopes[id]; !ok {
			if !initialScope(s) || newClaims != 0 {
				return x.ErrInvalid
			}
		}
	}
	return validateAssessmentExecution(d)
}

// CanClaimNextSlotV7 is authority-wide, not project-local. Full CPU settlement
// cannot substitute for resource/reader/collector/transport finality. Healthy
// sealed input may remain for another distinct immutable slot.
func CanClaimNextSlotV7(a *x.Authority, runs map[string]provider.Record, native map[string]NativeRecord) error {
	if a == nil {
		return x.ErrDenied
	}
	for _, r := range runs {
		if r.UnresolvedLiability || r.Active() {
			return x.ErrHeld
		}
	}
	for _, r := range native {
		if r.Settled == nil {
			return x.ErrHeld
		}
	}
	for _, r := range a.Attempts {
		if !r.SerialClear() {
			return x.ErrHeld
		}
	}
	for _, s := range a.Scopes {
		if !s.TransientOwnershipClosed(false) || s.CleanupOwnerEpoch != nil && !s.CleanupOwnershipCloseConfirmed {
			return x.ErrHeld
		}
		if s.ContentCloseUncertain || s.WriterStarted && !s.ContentCloseConfirmed {
			return x.ErrHeld
		}
		if !s.DenyLatched && s.State != "sealed" {
			return x.ErrHeld
		}
		if s.DenyLatched {
			removed := false
			for _, o := range s.Observations {
				if o.OwnedRemoval == "observed_absent" && o.WritersDrained == "observed" && o.ReadersDrained == "observed" && o.CollectorDrained == "observed" && o.ScopeAccessRevocation == "observed" {
					removed = true
				}
			}
			if !removed {
				return x.ErrHeld
			}
		}
	}
	return nil
}

// Reserve the full unit envelope for every planned attempt, including those
// never dispatched. Free bytes and deletion of data never enlarge authority.
// Independent maximum-form controls must establish each permitted form fits
// the unchanged unit ceiling; serialization checks actual forms on every write.
func assessmentReservedBytes(d Data) (int, error) {
	if d.Version != 7 {
		return 0, nil
	}
	n := len(d.Runs) + len(d.NativeRuns) + executionCount(d)
	if n > x.MaxUnits {
		return 0, x.ErrCapacity
	}
	for _, s := range d.AssessmentExecution.Scopes {
		b, e := json.Marshal(s)
		if e != nil || len(b) > x.MaxUnitBytes {
			return 0, x.ErrCapacity
		}
	}
	for _, a := range d.AssessmentExecution.Attempts {
		b, e := json.Marshal(a)
		if e != nil || len(b) > x.MaxUnitBytes {
			return 0, x.ErrCapacity
		}
	}
	// Retain map keys/separators in the fixed overhead, then reserve each value's
	// complete 96KiB envelope. 512 units leave >15MiB for frozen project metadata.
	copy := d
	auth := *d.AssessmentExecution
	auth.Scopes = map[string]x.ExecutionScope{}
	auth.Attempts = map[string]x.AssessmentAttempt{}
	copy.AssessmentExecution = &auth
	b, e := json.Marshal(copy)
	if e != nil {
		return 0, e
	}
	reserved := len(b) + executionCount(d)*(x.MaxUnitBytes+128)
	if reserved > x.MaxLedgerBytes {
		return 0, x.ErrCapacity
	}
	return reserved, nil
}
