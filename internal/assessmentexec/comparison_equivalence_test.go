package assessmentexec

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// Independent baseline oracle copied from26bd events.go (SHA5127a986...)
// with function/array identifiers prefixed only. It never calls optimized
// comparison helpers. Unchanged typed validation/Clone/digest are shared.
func legacyMetadataErasePath(m map[string]any, path string) {
	parts := strings.SplitN(path, ".", 2)
	if len(parts) == 1 {
		delete(m, parts[0])
		return
	}
	if child, ok := m[parts[0]].(map[string]any); ok {
		legacyMetadataErasePath(child, parts[1])
	}
}
func legacyMetadataOnly(a, b any, paths ...string) bool {
	var x, y map[string]any
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	// UseNumber preserves exact integer equality for counters and identities.
	d := json.NewDecoder(strings.NewReader(string(ab)))
	d.UseNumber()
	_ = d.Decode(&x)
	d = json.NewDecoder(strings.NewReader(string(bb)))
	d.UseNumber()
	_ = d.Decode(&y)
	for _, p := range append(paths, "generation", "normal_transitions_used", "safety_transitions_used", "wall_high_water", "tick_high_water_ms") {
		legacyMetadataErasePath(x, p)
		legacyMetadataErasePath(y, p)
	}
	return eq(x, y)
}
func legacyMetadataChanged(a, b any, paths ...string) bool {
	if !legacyMetadataOnly(a, b, paths...) {
		return false
	}
	// Clock and counter changes alone never constitute a semantic event.
	return !legacyMetadataOnly(a, b)
}
func legacyMetadataAdvances(oldN, oldS, newN, newS int64, safety bool) bool {
	if safety {
		return newN == oldN && newS == oldS+1
	}
	return newN == oldN+1 && newS == oldS
}
func legacyMetadataScopeObservationKey(o ScopeObservation) string {
	o.ObservationID = ""
	o.ObservedAt = ""
	o.ScopeGeneration = 0
	return Digest(o)
}
func legacyMetadataAttemptObservationKey(o AttemptCleanupObservation) string {
	o.ObservationID = ""
	o.ObservedAt = ""
	o.AttemptGeneration = 0
	return Digest(o)
}
func legacyMetadataAppendScopeObservation(old, next ExecutionScope) bool {
	if old.ObservationsSaturated {
		return false
	}
	if len(old.Observations) == MaxObservations {
		return next.ObservationsSaturated && eq(old.Observations, next.Observations)
	}
	return !next.ObservationsSaturated && len(next.Observations) == len(old.Observations)+1 && eq(old.Observations, next.Observations[:len(old.Observations)])
}
func legacyMetadataAppendAttemptObservation(old, next AssessmentAttempt) bool {
	if old.ObservationsSaturated {
		return false
	}
	if len(old.CleanupObservations) == MaxObservations {
		return next.ObservationsSaturated && eq(old.CleanupObservations, next.CleanupObservations)
	}
	return !next.ObservationsSaturated && len(next.CleanupObservations) == len(old.CleanupObservations)+1 && eq(old.CleanupObservations, next.CleanupObservations[:len(old.CleanupObservations)])
}

func legacyMetadataScopeMask(old, next ExecutionScope, e Event, slotClaim bool) (bool, bool) {
	safety := false
	ok := false
	switch e {
	case ScopeIdentity, ScopeContentIdentity:
		fields := []string{"Scope", "WriterLease", "WatchLease", "ExecutionLease", "WorkRoot", "OutputRoot"}
		jsonFields := []string{"scope", "writer_lease", "watch_lease", "execution_lease", "work_root", "output_root"}
		if e == ScopeContentIdentity {
			fields = []string{"Content"}
			jsonFields = []string{"content"}
		}
		a, b := reflect.ValueOf(old.FilesystemIdentity), reflect.ValueOf(next.FilesystemIdentity)
		for i, f := range fields {
			if a.FieldByName(f).IsNil() && !b.FieldByName(f).IsNil() && legacyMetadataChanged(old, next, "filesystem_identity."+jsonFields[i]) {
				ok = true
			}
		}
		safety = old.DenyLatched
		if e == ScopeContentIdentity {
			ok = ok && old.WriterStarted && !old.ContentCloseConfirmed && !old.ContentCloseUncertain
		}
	case ScopePrepareClosed:
		safety = true
		ok = !old.PrepareOwnershipCloseConfirmed && next.PrepareOwnershipCloseConfirmed && legacyMetadataChanged(old, next, "prepare_ownership_close_confirmed")
	case ScopeWriterClosed:
		safety = true
		ok = old.WriterStarted && !old.WriterOwnershipCloseConfirmed && next.WriterOwnershipCloseConfirmed && legacyMetadataChanged(old, next, "writer_ownership_close_confirmed")
	case ScopeSealBegin:
		ok = old.State == "accepting" && old.UploadComplete && old.WriterOwnershipCloseConfirmed && !old.SealStarted && next.SealStarted && ScopeNormalCapacity-old.NormalTransitionsUsed >= 2 && legacyMetadataChanged(old, next, "seal_started")
	case ScopeSealClosed:
		safety = true
		ok = old.SealStarted && !old.SealOwnershipCloseConfirmed && next.SealOwnershipCloseConfirmed && legacyMetadataChanged(old, next, "seal_ownership_close_confirmed")
	case ScopeWatchClosed:
		safety = true
		ok = old.WatchStarted && !old.WatchOwnershipCloseConfirmed && next.WatchOwnershipCloseConfirmed && legacyMetadataChanged(old, next, "watch_ownership_close_confirmed")
	case ScopeCleanupBegin:
		safety = true
		ok = old.DenyLatched && old.CleanupOwnerEpoch == nil && next.CleanupOwnerEpoch != nil && legacyMetadataChanged(old, next, "cleanup_owner_epoch")
	case ScopeCleanupClosed:
		safety = true
		ok = old.CleanupOwnerEpoch != nil && !old.CleanupOwnershipCloseConfirmed && next.CleanupOwnershipCloseConfirmed && (eq(old.Observations, next.Observations) && old.ObservationsSaturated == next.ObservationsSaturated || legacyMetadataAppendScopeObservation(old, next)) && legacyMetadataChanged(old, next, "cleanup_ownership_close_confirmed", "observations", "observations_saturated")
	case ScopeLeaseUncertain:
		safety = true
		ok = !old.LeaseCloseUncertain && next.LeaseCloseUncertain && legacyMetadataChanged(old, next, "lease_close_uncertain")
	case ScopeWatchClaim:
		ok = !old.WatchStarted && next.WatchStarted && next.WatchEpoch != nil && legacyMetadataChanged(old, next, "watch_started", "watch_epoch")
	case ScopeWatchReady:
		ok = old.State == "preparing" && next.State == "accepting" && legacyMetadataChanged(old, next, "state")
	case ScopeWriterClaim:
		ok = old.State == "accepting" && old.PrepareOwnershipCloseConfirmed && !old.LeaseCloseUncertain && !old.WriterStarted && next.WriterStarted && ScopeNormalCapacity-old.NormalTransitionsUsed >= 6 && legacyMetadataChanged(old, next, "writer_started")
	case ScopeContentClosed:
		safety = old.DenyLatched
		ok = old.WriterStarted && !old.ContentCloseConfirmed && next.ContentCloseConfirmed && legacyMetadataChanged(old, next, "content_close_confirmed")
	case ScopeUploadComplete:
		ok = !old.UploadComplete && next.UploadComplete && old.WriterOwnershipCloseConfirmed && old.ContentCloseConfirmed && !old.ContentCloseUncertain && old.SealedLengthBytes == nil && next.SealedLengthBytes != nil && legacyMetadataChanged(old, next, "upload_complete", "sealed_length_bytes")
	case ScopeSeal:
		ok = old.State == "accepting" && next.State == "sealed" && old.UploadComplete && old.SealStarted && old.SealOwnershipCloseConfirmed && !old.LeaseCloseUncertain && legacyMetadataChanged(old, next, "state")
	case ScopeSlotClaim:
		ok = slotClaim && old.State == "sealed" && legacyMetadataOnly(old, next) && ScopeNormalCapacity-old.NormalTransitionsUsed >= 1
	case ScopeDeny:
		safety = true
		ok = !old.DenyLatched && next.DenyLatched && next.ExecutionDenyLatched && next.State == "cleaning" && legacyMetadataChanged(old, next, "deny_latched", "execution_deny_latched", "reason", "state")
	case ScopeContentUncertain:
		safety = true
		ok = old.WriterStarted && old.DenyLatched && !old.ContentCloseUncertain && next.ContentCloseUncertain && legacyMetadataChanged(old, next, "content_close_uncertain")
	case ScopeUnresolved:
		safety = true
		ok = old.State == "cleaning" && next.State == "cleanup_unresolved" && legacyMetadataChanged(old, next, "state")
	case ScopeObservationEvent:
		safety = true
		ok = old.DenyLatched && legacyMetadataAppendScopeObservation(old, next) && legacyMetadataChanged(old, next, "observations", "observations_saturated")
	}
	if !safety && (old.DenyLatched || old.ExecutionDenyLatched) {
		ok = false
	}
	return ok, safety
}

var legacyMetadataScopeEvents = []Event{ScopePrepareClosed, ScopeWriterClosed, ScopeSealBegin, ScopeSealClosed, ScopeWatchClosed, ScopeCleanupBegin, ScopeCleanupClosed, ScopeLeaseUncertain, ScopeIdentity, ScopeWatchClaim, ScopeWatchReady, ScopeWriterClaim, ScopeContentIdentity, ScopeContentClosed, ScopeUploadComplete, ScopeSeal, ScopeSlotClaim, ScopeDeny, ScopeContentUncertain, ScopeUnresolved, ScopeObservationEvent}

func legacyMetadataScopeTransition(old, next ExecutionScope, slotClaim bool) error {
	if eq(old, next) {
		if slotClaim {
			return ErrInvalid
		}
		return nil
	}
	if next.Validate() != nil || next.Generation != old.Generation+1 || next.WallHighWater < old.WallHighWater || next.TickHighWaterMS < old.TickHighWaterMS {
		return ErrInvalid
	}
	for _, event := range legacyMetadataScopeEvents {
		if slotClaim && event != ScopeSlotClaim {
			continue
		}
		ok, safety := legacyMetadataScopeMask(old, next, event, slotClaim)
		if ok && legacyMetadataAdvances(old.NormalTransitionsUsed, old.SafetyTransitionsUsed, next.NormalTransitionsUsed, next.SafetyTransitionsUsed, safety) {
			return nil
		}
	}
	return ErrInvalid
}
func legacyMetadataAdvanceScope(old ExecutionScope, event Event, at string, tick int64, mutate func(*ExecutionScope)) (ExecutionScope, error) {
	next := Clone(old)
	mutate(&next)
	if eq(old, next) {
		return old, nil
	}
	ok, safety := legacyMetadataScopeMask(old, next, event, event == ScopeSlotClaim)
	if !ok {
		return old, ErrInvalid
	}
	if at > next.WallHighWater {
		next.WallHighWater = at
	}
	if tick > next.TickHighWaterMS {
		next.TickHighWaterMS = tick
	}
	if safety {
		next.SafetyTransitionsUsed++
	} else {
		next.NormalTransitionsUsed++
	}
	next.Generation++
	if legacyMetadataScopeTransition(old, next, event == ScopeSlotClaim) != nil {
		return old, ErrInvalid
	}
	return next, nil
}

// Slot claims are the legacyMetadataOnly valid semantic event with no legacyMetadataChanged scope field:
// the ledger independently requires one newly admitted child in that transaction.
func legacyMetadataCountSlotClaim(old ExecutionScope, at string, tick int64) (ExecutionScope, error) {
	next := Clone(old)
	next.NormalTransitionsUsed++
	next.Generation++
	if at > next.WallHighWater {
		next.WallHighWater = at
	}
	if tick > next.TickHighWaterMS {
		next.TickHighWaterMS = tick
	}
	if legacyMetadataScopeTransition(old, next, true) != nil {
		return old, ErrInvalid
	}
	return next, nil
}
func legacyMetadataAddScopeObservation(old ExecutionScope, o ScopeObservation) (ExecutionScope, error) {
	if old.ObservationsSaturated {
		return old, nil
	}
	for _, prior := range old.Observations {
		if legacyMetadataScopeObservationKey(prior) == legacyMetadataScopeObservationKey(o) {
			return old, nil
		}
	}
	return legacyMetadataAdvanceScope(old, ScopeObservationEvent, o.ObservedAt, old.TickHighWaterMS, func(s *ExecutionScope) {
		if len(s.Observations) == MaxObservations {
			s.ObservationsSaturated = true
		} else {
			o.ScopeGeneration = s.Generation + 1
			s.Observations = append(s.Observations, o)
		}
	})
}

func legacyMetadataPhaseMonotone(old, next *PhaseRecord) bool {
	if old == nil {
		return true
	}
	if next == nil {
		return false
	}
	a, b := reflect.ValueOf(*old), reflect.ValueOf(*next)
	for i := 0; i < a.NumField(); i++ {
		x, y := a.Field(i), b.Field(i)
		if x.Kind() == reflect.Bool && x.Bool() && !y.Bool() {
			return false
		}
		if x.Kind() == reflect.Pointer && !x.IsNil() && !eq(x.Interface(), y.Interface()) {
			return false
		}
	}
	return old.Phase == next.Phase && old.OwnerName == next.OwnerName
}
func legacyMetadataAttemptMask(old, next AssessmentAttempt, e Event) (bool, bool) {
	safety := false
	ok := false
	forward := one(old.State, "claimed", "preparing", "running", "checking") && !old.DenyLatched && !old.OwnershipCloseConfirmed && !old.OwnershipCloseUncertain && !old.ReaderCloseConfirmed && !old.CollectorCloseConfirmed && !old.ReaderCloseUncertain && !old.CollectorCloseUncertain
	switch e {
	case AttemptOwnershipClosed:
		safety = true
		ok = !old.OwnershipCloseConfirmed && next.OwnershipCloseConfirmed && one(old.State, "finishing", "held", "terminal") && legacyMetadataChanged(old, next, "ownership_close_confirmed")
	case AttemptOwnershipUncertain:
		safety = true
		ok = !old.OwnershipCloseUncertain && next.OwnershipCloseUncertain && legacyMetadataChanged(old, next, "ownership_close_uncertain")
	case AttemptReader:
		ok = !old.ReaderStarted && next.ReaderStarted && legacyMetadataChanged(old, next, "reader_started")
	case AttemptCollector:
		ok = !old.CollectorStarted && next.CollectorStarted && legacyMetadataChanged(old, next, "collector_started")
	case AttemptPhase:
		for i, p := range []*PhaseRecord{next.PhaseRecords.Workload, next.PhaseRecords.Checker} {
			name := "workload"
			prior := old.PhaseRecords.Workload
			if i == 1 {
				name = "checker"
				prior = old.PhaseRecords.Checker
			}
			if old.ReaderStarted && old.CollectorStarted && prior == nil && p != nil && eq(*p, PhaseRecord{Phase: name, OwnerName: ResourceName(old.Binding.BrokerAuthorityID, old.Slot.AttemptID, name)}) && legacyMetadataChanged(old, next, "phase_records."+name) {
				ok = true
			}
		}
	case AttemptPhaseForward, AttemptPhaseSafety:
		safety = e == AttemptPhaseSafety
		fields := []string{"transport_started", "create_claimed", "create_returned", "resource_identity", "start_claimed", "start_returned", "started_observed"}
		if safety {
			fields = []string{"create_returned", "create_ambiguous", "resource_identity", "start_returned", "started_observed", "exit_code", "stopped_observed", "removal_observed", "transport_close_confirmed", "transport_close_uncertain"}
		}
		for _, name := range []string{"workload", "checker"} {
			a, b := old.PhaseRecords.Workload, next.PhaseRecords.Workload
			if name == "checker" {
				a, b = old.PhaseRecords.Checker, next.PhaseRecords.Checker
			}
			if a == nil || b == nil {
				continue
			}
			for _, f := range fields {
				paths := []string{"phase_records." + name + "." + f}
				if f == "exit_code" {
					paths = append(paths, "phase_records."+name+".finished_at")
				}
				if f == "started_observed" {
					if a.StartedObserved || !b.StartedObserved {
						continue
					}
					paths = append(paths, "phase_records."+name+".started_at")
				}
				if legacyMetadataChanged(old, next, paths...) {
					ok = true
				}
			}
		}
	case AttemptForward:
		ok = (old.State == "claimed" && next.State == "preparing" || old.State == "preparing" && next.State == "running" || old.State == "running" && next.State == "checking") && legacyMetadataChanged(old, next, "state")
	case AttemptReaderClosed:
		safety = true
		ok = old.ReaderStarted && !old.ReaderCloseConfirmed && next.ReaderCloseConfirmed && old.GlobalCloseReady() && legacyMetadataChanged(old, next, "reader_close_confirmed")
	case AttemptReaderUncertain:
		safety = true
		ok = old.ReaderStarted && !old.ReaderCloseUncertain && next.ReaderCloseUncertain && legacyMetadataChanged(old, next, "reader_close_uncertain")
	case AttemptCollectorClosed:
		safety = true
		ok = old.CollectorStarted && !old.CollectorCloseConfirmed && next.CollectorCloseConfirmed && old.GlobalCloseReady() && legacyMetadataChanged(old, next, "collector_close_confirmed")
	case AttemptCollectorUncertain:
		safety = true
		ok = old.CollectorStarted && !old.CollectorCloseUncertain && next.CollectorCloseUncertain && legacyMetadataChanged(old, next, "collector_close_uncertain")
	case AttemptDeny:
		safety = true
		ok = !old.DenyLatched && next.DenyLatched && old.FailureCode == "none" && next.FailureCode != "none" && legacyMetadataChanged(old, next, "deny_latched", "failure_code")
	case AttemptFinish:
		safety = true
		ok = (one(old.State, "claimed", "preparing", "running", "checking") && one(next.State, "finishing", "held") || old.State == "finishing" && one(next.State, "held", "terminal") || old.State == "held" && next.State == "terminal") && legacyMetadataChanged(old, next, "state", "outcome")
	case AttemptSettle:
		safety = true
		ok = old.AccountingUnresolved && !next.AccountingUnresolved && !old.ComputeCleanupObserved && next.ComputeCleanupObserved && next.EffectsClosed() && one(next.State, "held", "terminal") && legacyMetadataChanged(old, next, "compute", "accounting_unresolved", "compute_cleanup_observed")
	case AttemptMetrics:
		safety = !forward
		ok = eq(old.Metrics, UnknownMeasurements()) && old.MeasurementDigest == nil && next.MeasurementDigest != nil && legacyMetadataChanged(old, next, "metrics", "measurement_digest")
	case AttemptQuality:
		safety = !forward
		ok = old.Quality.Status == "unknown" && next.Quality.Status != "unknown" && legacyMetadataChanged(old, next, "quality")
	case AttemptObservationEvent:
		safety = true
		ok = legacyMetadataAppendAttemptObservation(old, next) && legacyMetadataChanged(old, next, "cleanup_observations", "observations_saturated")
	}
	if !safety && !forward {
		ok = false
	}
	// Newly initiating a start requires the definitive prior create, while a
	// later uncertainty latch remains representable without undoing history.
	for _, pair := range [][2]*PhaseRecord{{old.PhaseRecords.Workload, next.PhaseRecords.Workload}, {old.PhaseRecords.Checker, next.PhaseRecords.Checker}} {
		if pair[1] != nil && pair[1].StartClaimed && (pair[0] == nil || !pair[0].StartClaimed) && (pair[0] == nil || pair[0].CreateAmbiguous || !pair[0].CreateReturned || pair[0].ResourceIdentity == nil) {
			ok = false
		}
	}
	if old.PhaseRecords.Checker == nil && next.PhaseRecords.Checker != nil && (old.PhaseRecords.Workload == nil || !phaseFinal(old.PhaseRecords.Workload)) {
		ok = false
	}
	if next.State == "terminal" && old.State != "terminal" && (!next.OwnershipCloseConfirmed || next.OwnershipCloseUncertain) {
		ok = false
	}
	if next.Outcome == "succeeded" && old.Outcome != "succeeded" && (!next.EffectsClosed() || next.DenyLatched || next.PhaseRecords.Workload == nil || next.PhaseRecords.Workload.ExitCode == nil || *next.PhaseRecords.Workload.ExitCode != 0) {
		ok = false
	}
	if !legacyMetadataPhaseMonotone(old.PhaseRecords.Workload, next.PhaseRecords.Workload) || !legacyMetadataPhaseMonotone(old.PhaseRecords.Checker, next.PhaseRecords.Checker) {
		ok = false
	}
	return ok, safety
}

var legacyMetadataAttemptEvents = []Event{AttemptOwnershipClosed, AttemptOwnershipUncertain, AttemptReader, AttemptCollector, AttemptPhase, AttemptPhaseForward, AttemptForward, AttemptPhaseSafety, AttemptReaderClosed, AttemptReaderUncertain, AttemptCollectorClosed, AttemptCollectorUncertain, AttemptDeny, AttemptFinish, AttemptSettle, AttemptMetrics, AttemptQuality, AttemptObservationEvent}

func legacyMetadataAttemptTransition(old, next AssessmentAttempt, s ExecutionScope) error {
	if eq(old, next) {
		return nil
	}
	if next.Validate(s) != nil || next.Generation != old.Generation+1 || next.WallHighWater < old.WallHighWater || next.TickHighWaterMS < old.TickHighWaterMS {
		return ErrInvalid
	}
	for _, event := range legacyMetadataAttemptEvents {
		ok, safety := legacyMetadataAttemptMask(old, next, event)
		if ok && legacyMetadataAdvances(old.NormalTransitionsUsed, old.SafetyTransitionsUsed, next.NormalTransitionsUsed, next.SafetyTransitionsUsed, safety) {
			return nil
		}
	}
	return ErrInvalid
}
func legacyMetadataAdvanceAttempt(old AssessmentAttempt, s ExecutionScope, event Event, at string, tick int64, mutate func(*AssessmentAttempt)) (AssessmentAttempt, error) {
	next := Clone(old)
	mutate(&next)
	if eq(old, next) {
		return old, nil
	}
	ok, safety := legacyMetadataAttemptMask(old, next, event)
	if !ok {
		return old, ErrInvalid
	}
	if at > next.WallHighWater {
		next.WallHighWater = at
	}
	if tick > next.TickHighWaterMS {
		next.TickHighWaterMS = tick
	}
	if safety {
		next.SafetyTransitionsUsed++
	} else {
		next.NormalTransitionsUsed++
	}
	next.Generation++
	if legacyMetadataAttemptTransition(old, next, s) != nil {
		return old, ErrInvalid
	}
	return next, nil
}
func legacyMetadataAddAttemptObservation(old AssessmentAttempt, s ExecutionScope, o AttemptCleanupObservation) (AssessmentAttempt, error) {
	if old.ObservationsSaturated {
		return old, nil
	}
	for _, prior := range old.CleanupObservations {
		if legacyMetadataAttemptObservationKey(prior) == legacyMetadataAttemptObservationKey(o) {
			return old, nil
		}
	}
	return legacyMetadataAdvanceAttempt(old, s, AttemptObservationEvent, o.ObservedAt, old.TickHighWaterMS, func(a *AssessmentAttempt) {
		if len(a.CleanupObservations) == MaxObservations {
			a.ObservationsSaturated = true
		} else {
			o.AttemptGeneration = a.Generation + 1
			a.CleanupObservations = append(a.CleanupObservations, o)
		}
	})
}

func metadataPathSets() [][]string {
	sets := [][]string{{}, {"state"}, {"state", "outcome"}, {"watch_started", "watch_epoch"}, {"upload_complete", "sealed_length_bytes"}, {"deny_latched", "execution_deny_latched", "reason", "state"}, {"observations", "observations_saturated"}, {"compute", "accounting_unresolved", "compute_cleanup_observed"}, {"metrics", "measurement_digest"}, {"quality"}, {"cleanup_observations", "observations_saturated"}, {"deny_latched", "failure_code"}}
	for _, name := range []string{"scope", "writer_lease", "watch_lease", "execution_lease", "work_root", "output_root", "content"} {
		sets = append(sets, []string{"filesystem_identity." + name})
	}
	for _, name := range []string{"writer_started", "prepare_ownership_close_confirmed", "writer_ownership_close_confirmed", "seal_started", "seal_ownership_close_confirmed", "watch_ownership_close_confirmed", "cleanup_owner_epoch", "cleanup_ownership_close_confirmed", "lease_close_uncertain", "content_close_confirmed", "content_close_uncertain", "ownership_close_confirmed", "ownership_close_uncertain", "reader_started", "collector_started", "reader_close_confirmed", "reader_close_uncertain", "collector_close_confirmed", "collector_close_uncertain"} {
		sets = append(sets, []string{name})
	}
	sets = append(sets, []string{"cleanup_ownership_close_confirmed", "observations", "observations_saturated"})
	for _, phase := range []string{"workload", "checker"} {
		root := "phase_records." + phase
		sets = append(sets, []string{root})
		for _, field := range []string{"transport_started", "create_claimed", "create_returned", "create_ambiguous", "resource_identity", "start_claimed", "start_returned", "stopped_observed", "removal_observed", "transport_close_confirmed", "transport_close_uncertain"} {
			sets = append(sets, []string{root + "." + field})
		}
		sets = append(sets, []string{root + ".started_observed", root + ".started_at"}, []string{root + ".exit_code", root + ".finished_at"})
	}
	return sets
}
func metadataSetPath(m map[string]any, path string, value any) {
	parts := strings.SplitN(path, ".", 2)
	if len(parts) == 1 {
		m[parts[0]] = value
		return
	}
	child, ok := m[parts[0]].(map[string]any)
	if !ok {
		child = map[string]any{}
		m[parts[0]] = child
	}
	metadataSetPath(child, parts[1], value)
}
func metadataComparisonQuery(t *testing.T, a, b any, sets [][]string) {
	t.Helper()
	inputA, _ := json.Marshal(a)
	inputB, _ := json.Marshal(b)
	c := maskComparison{old: a, next: b}
	c.prepare()
	baseA, _ := json.Marshal(c.a)
	baseB, _ := json.Marshal(c.b)
	for _, paths := range sets {
		if c.only(paths...) != legacyMetadataOnly(a, b, paths...) || c.changed(paths...) != legacyMetadataChanged(a, b, paths...) {
			t.Fatal("JSON comparison differed from independent baseline")
		}
		gotA, _ := json.Marshal(c.a)
		gotB, _ := json.Marshal(c.b)
		if string(gotA) != string(baseA) || string(gotB) != string(baseB) {
			t.Fatal("candidate erased an immutable comparison base")
		}
		gotA, _ = json.Marshal(a)
		gotB, _ = json.Marshal(b)
		if string(gotA) != string(inputA) || string(gotB) != string(inputB) {
			t.Fatal("comparison mutated caller values")
		}
	}
}
func TestMetadataComparisonMatchesIndependentLegacy(t *testing.T) {
	sets := metadataPathSets()
	for _, paths := range sets {
		a, b := map[string]any{}, map[string]any{}
		for _, p := range paths {
			metadataSetPath(a, p, json.Number("9007199254740992"))
			metadataSetPath(b, p, json.Number("9007199254740993"))
		}
		metadataComparisonQuery(t, a, b, sets)
		b["unrelated_sibling"] = true
		metadataComparisonQuery(t, a, b, sets)
	}
	queries := [][]string{{"a.x", "a.y"}, {"a.x"}, {"a.y"}, {}, {"a", "a.x"}, {"a.x", "a"}, {"a.x", "a.x"}, {"a.b"}, {"array.0"}, {"a."}, {".x"}, {""}}
	for _, pair := range [][2]any{
		{map[string]any{"a": map[string]any{"x": 1, "y": 2}}, map[string]any{"a": map[string]any{"x": 3, "y": 4}}},
		{map[string]any{"a": map[string]any{"x": 1, "y": 2}}, map[string]any{"a": map[string]any{"x": 1, "y": 3}}},
		{map[string]any{"a.b": 1}, map[string]any{"a.b": 2}},
		{map[string]any{"a": nil}, map[string]any{"a": map[string]any{}}},
		{map[string]any(nil), map[string]any{}},
		{map[string]any{"array": []any(nil)}, map[string]any{"array": []any{}}},
		{map[string]any{"array": []any{1}}, map[string]any{"array": []any{2}}},
		{map[string]any{"a": json.Number("1")}, map[string]any{"a": json.Number("1e0")}},
		{map[string]any{"a": json.Number("1.0")}, map[string]any{"a": true}},
		{map[string]any{"a": map[string]any(nil)}, map[string]any{"a": map[string]any{"x": nil}}},
		{map[string]any{"generation": 1, "tick_high_water_ms": 0}, map[string]any{"generation": 2, "tick_high_water_ms": 9007199254740991}},
		{struct {
			N int `json:"n,omitempty"`
		}{}, struct {
			N int `json:"n,omitempty"`
		}{0}},
	} {
		metadataComparisonQuery(t, pair[0], pair[1], queries)
	}
}

func metadataCompareScopeStep(t *testing.T, old ExecutionScope, event Event, fn func(*ExecutionScope)) ExecutionScope {
	t.Helper()
	at, tick := "2026-10-20T00:00:01.000000Z", int64(1)
	before, _ := json.Marshal(old)
	want, we := legacyMetadataAdvanceScope(old, event, at, tick, fn)
	got, ge := AdvanceScope(old, event, at, tick, fn)
	if ge != we || !reflect.DeepEqual(got, want) || ge != nil {
		t.Fatal("scope Advance changed baseline state, clock, counters or errors")
	}
	for _, candidate := range legacyMetadataScopeEvents {
		wb, ws := legacyMetadataScopeMask(old, want, candidate, event == ScopeSlotClaim)
		gb, gs := scopeMask(old, want, candidate, event == ScopeSlotClaim)
		if wb != gb || ws != gs {
			t.Fatal("scope candidate acceptance or safety classification changed")
		}
	}
	if ScopeTransition(old, got, false) != legacyMetadataScopeTransition(old, want, false) {
		t.Fatal("scope transition classification changed")
	}
	bad := Clone(want)
	bad.ProjectID = strings.Repeat("a", 32)
	if ScopeTransition(old, bad, false) != legacyMetadataScopeTransition(old, bad, false) {
		t.Fatal("scope identity negative changed")
	}
	after, _ := json.Marshal(old)
	if string(before) != string(after) {
		t.Fatal("scope classification mutated input")
	}
	return got
}
func metadataCompareAttemptStep(t *testing.T, s ExecutionScope, old AssessmentAttempt, event Event, fn func(*AssessmentAttempt)) AssessmentAttempt {
	t.Helper()
	at, tick := "2026-10-20T00:00:01.000000Z", int64(1)
	before, _ := json.Marshal(old)
	want, we := legacyMetadataAdvanceAttempt(old, s, event, at, tick, fn)
	got, ge := AdvanceAttempt(old, s, event, at, tick, fn)
	if ge != we || !reflect.DeepEqual(got, want) || ge != nil {
		t.Fatal("attempt Advance changed baseline state, clock, counters or errors")
	}
	for _, candidate := range legacyMetadataAttemptEvents {
		wb, ws := legacyMetadataAttemptMask(old, want, candidate)
		gb, gs := attemptMask(old, want, candidate)
		if wb != gb || ws != gs {
			t.Fatal("attempt candidate acceptance or safety classification changed")
		}
	}
	if AttemptTransition(old, got, s) != legacyMetadataAttemptTransition(old, want, s) {
		t.Fatal("attempt transition classification changed")
	}
	bad := Clone(want)
	bad.Binding.ProjectID = strings.Repeat("a", 32)
	if AttemptTransition(old, bad, s) != legacyMetadataAttemptTransition(old, bad, s) {
		t.Fatal("attempt identity negative changed")
	}
	after, _ := json.Marshal(old)
	if string(before) != string(after) {
		t.Fatal("attempt classification mutated input")
	}
	return got
}
func TestMetadataTransitionsAndAdvanceMatchIndependentLegacy(t *testing.T) {
	s := executionFixture(t)
	if s.Validate() != nil {
		t.Fatal("initial unit scope invalid")
	}
	s = metadataCompareScopeStep(t, s, ScopeDeny, func(v *ExecutionScope) {
		v.DenyLatched = true
		v.ExecutionDenyLatched = true
		v.State = "cleaning"
		v.Reason = "cancelled"
	})
	s = metadataCompareScopeStep(t, s, ScopeUnresolved, func(v *ExecutionScope) { v.State = "cleanup_unresolved" })
	s = metadataCompareScopeStep(t, s, ScopeLeaseUncertain, func(v *ExecutionScope) { v.LeaseCloseUncertain = true })
	metadataCompareScopeStep(t, s, ScopeLeaseUncertain, func(v *ExecutionScope) { v.LeaseCloseUncertain = true })
	// Initial claims and legal negative traces are pure unit metadata; they
	// do not fabricate positive resource/descriptor completion or engine proof.
	s = executionFixture(t)
	a := AssessmentAttempt{Schema: AttemptSchema, Binding: s.Binding(), Slot: s.Slots[0], HandoffNonce: strings.Repeat("a", 32), ClaimEpoch: strings.Repeat("b", 32), ClaimedAt: s.CreatedAt, Deadline: "2026-10-20T00:30:00Z", ClockEpoch: s.ClockEpoch, DeadlineTickMS: 1800000, Generation: 1, State: "claimed", Outcome: "unknown", FailureCode: "none", AccountingUnresolved: true, Compute: Compute{ReservedCPUSeconds: 1800, Provenance: "reserved"}, Metrics: UnknownMeasurements(), Quality: Quality{Status: "unknown", Source: "none", Checker: s.Capability.Checker}, CleanupObservations: []AttemptCleanupObservation{}, WallHighWater: s.CreatedAt}
	a.BindingDigest = AttemptBindingDigest(a.Binding, a.Slot, a.HandoffNonce)
	if a.Validate(s) != nil {
		t.Fatal("initial unit attempt invalid")
	}
	metadataAdvanceFailures(t, s, a)
	a = metadataCompareAttemptStep(t, s, a, AttemptForward, func(v *AssessmentAttempt) { v.State = "preparing" })
	a = metadataCompareAttemptStep(t, s, a, AttemptReader, func(v *AssessmentAttempt) { v.ReaderStarted = true })
	a = metadataCompareAttemptStep(t, s, a, AttemptCollector, func(v *AssessmentAttempt) { v.CollectorStarted = true })
	a = metadataCompareAttemptStep(t, s, a, AttemptPhase, func(v *AssessmentAttempt) {
		v.PhaseRecords.Workload = &PhaseRecord{Phase: "workload", OwnerName: ResourceName(a.Binding.BrokerAuthorityID, a.Slot.AttemptID, "workload")}
	})
	a = metadataCompareAttemptStep(t, s, a, AttemptPhaseForward, func(v *AssessmentAttempt) { v.PhaseRecords.Workload.TransportStarted = true })
	// One phase-forward classifier tries several earlier field candidates
	// before reaching create_claimed. COW state must not leak across them.
	a = metadataCompareAttemptStep(t, s, a, AttemptPhaseForward, func(v *AssessmentAttempt) { v.PhaseRecords.Workload.CreateClaimed = true })
	metadataPhaseAlternatives(t, s, a)
	a = metadataCompareAttemptStep(t, s, a, AttemptDeny, func(v *AssessmentAttempt) { v.DenyLatched = true; v.FailureCode = "cancelled" })
	a = metadataCompareAttemptStep(t, s, a, AttemptFinish, func(v *AssessmentAttempt) { v.State = "finishing" })
	a = metadataCompareAttemptStep(t, s, a, AttemptReaderUncertain, func(v *AssessmentAttempt) { v.ReaderCloseUncertain = true })
	a = metadataCompareAttemptStep(t, s, a, AttemptCollectorUncertain, func(v *AssessmentAttempt) { v.CollectorCloseUncertain = true })
	a = metadataCompareAttemptStep(t, s, a, AttemptOwnershipUncertain, func(v *AssessmentAttempt) { v.OwnershipCloseUncertain = true })
	a = metadataCompareAttemptStep(t, s, a, AttemptFinish, func(v *AssessmentAttempt) { v.State = "held" })
	metadataCompareAttemptStep(t, s, a, AttemptFinish, func(v *AssessmentAttempt) {})
}

func metadataAdvanceFailures(t *testing.T, s ExecutionScope, a AssessmentAttempt) {
	t.Helper()
	// These are bounded representation/counter boundary fixtures, not claims
	// that a real operational trace has reached every capacity form.
	for _, c := range []struct {
		at   string
		tick int64
		kind string
	}{
		{"2026-10-20T00:00:01.000000Z", 1, "identity"},
		{"2026-10-20T00:00:01.000000Z", 1, "counter"},
		{"2026-10-20T00:00:01.000000Z", 1, "saturated"},
		{"z", 1, "clock"},
		{"2026-10-20T00:00:01.000000Z", 9007199254740992, "tick"},
	} {
		oldS, oldA := Clone(s), Clone(a)
		if c.kind == "saturated" {
			oldS.SafetyTransitionsUsed = ScopeSafetyCapacity
			oldS.Generation = 1 + ScopeSafetyCapacity
			oldA.SafetyTransitionsUsed = AttemptSafetyCapacity
			oldA.Generation = 1 + AttemptSafetyCapacity
		}
		if oldS.Validate() != nil || oldA.Validate(s) != nil {
			t.Fatal("counter-boundary metadata fixture invalid")
		}
		beforeS, _ := json.Marshal(oldS)
		beforeA, _ := json.Marshal(oldA)
		fs := func(v *ExecutionScope) {
			v.DenyLatched = true
			v.ExecutionDenyLatched = true
			v.State = "cleaning"
			v.Reason = "cancelled"
			if c.kind == "identity" {
				v.ProjectID = strings.Repeat("a", 32)
			}
			if c.kind == "counter" {
				v.NormalTransitionsUsed++
			}
		}
		fa := func(v *AssessmentAttempt) {
			v.DenyLatched = true
			v.FailureCode = "cancelled"
			if c.kind == "identity" {
				v.ClaimEpoch = strings.Repeat("c", 32)
			}
			if c.kind == "counter" {
				v.NormalTransitionsUsed++
			}
		}
		ws, we := legacyMetadataAdvanceScope(oldS, ScopeDeny, c.at, c.tick, fs)
		gs, ge := AdvanceScope(oldS, ScopeDeny, c.at, c.tick, fs)
		if we != ErrInvalid || ge != we || !reflect.DeepEqual(ws, gs) || !reflect.DeepEqual(gs, oldS) {
			t.Fatal("scope counter/clock/identity refusal or returned old state changed")
		}
		wa, we := legacyMetadataAdvanceAttempt(oldA, s, AttemptDeny, c.at, c.tick, fa)
		ga, ge := AdvanceAttempt(oldA, s, AttemptDeny, c.at, c.tick, fa)
		if we != ErrInvalid || ge != we || !reflect.DeepEqual(wa, ga) || !reflect.DeepEqual(ga, oldA) {
			t.Fatal("attempt counter/clock/identity refusal or returned old state changed")
		}
		afterS, _ := json.Marshal(oldS)
		afterA, _ := json.Marshal(oldA)
		if string(beforeS) != string(afterS) || string(beforeA) != string(afterA) {
			t.Fatal("refused Advance mutated source")
		}
	}
	// A backwards supplied sample is clamped exactly as before, rather than
	// incorrectly manufacturing a stricter clock rejection.
	fs := func(v *ExecutionScope) { v.LeaseCloseUncertain = true }
	ws, we := legacyMetadataAdvanceScope(s, ScopeLeaseUncertain, "2020-01-01T00:00:00.000000Z", -1, fs)
	gs, ge := AdvanceScope(s, ScopeLeaseUncertain, "2020-01-01T00:00:00.000000Z", -1, fs)
	if we != nil || ge != we || !reflect.DeepEqual(ws, gs) || gs.WallHighWater != s.WallHighWater || gs.TickHighWaterMS != s.TickHighWaterMS {
		t.Fatal("clamped clock semantics changed")
	}
}
func metadataPhaseAlternatives(t *testing.T, s ExecutionScope, a AssessmentAttempt) {
	t.Helper()
	for _, mutate := range []func(*AssessmentAttempt){
		func(v *AssessmentAttempt) { v.PhaseRecords.Workload.CreateAmbiguous = true },
		func(v *AssessmentAttempt) { v.PhaseRecords.Workload.TransportCloseUncertain = true },
		func(v *AssessmentAttempt) { v.PhaseRecords.Workload.CreateClaimed = false },
		func(v *AssessmentAttempt) { v.PhaseRecords.Workload.StartClaimed = true },
		func(v *AssessmentAttempt) {
			v.PhaseRecords.Workload.CreateAmbiguous = true
			v.PhaseRecords.Workload.TransportCloseUncertain = true
		},
		func(v *AssessmentAttempt) {
			v.PhaseRecords.Checker = &PhaseRecord{Phase: "checker", OwnerName: ResourceName(a.Binding.BrokerAuthorityID, a.Slot.AttemptID, "checker")}
		},
	} {
		before, _ := json.Marshal(a)
		for _, event := range legacyMetadataAttemptEvents {
			want, we := legacyMetadataAdvanceAttempt(a, s, event, "2026-10-20T00:00:01.000000Z", 1, mutate)
			got, ge := AdvanceAttempt(a, s, event, "2026-10-20T00:00:01.000000Z", 1, mutate)
			if ge != we || !reflect.DeepEqual(want, got) {
				t.Fatal("phase alternative candidate/order/error semantics changed")
			}
			if AttemptTransition(a, got, s) != legacyMetadataAttemptTransition(a, want, s) {
				t.Fatal("phase alternative transition semantics changed")
			}
			after, _ := json.Marshal(a)
			if string(before) != string(after) {
				t.Fatal("phase alternative mutated immutable input")
			}
		}
	}
}
