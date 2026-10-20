package assessmentexec

import (
	"encoding/json"
	"reflect"
	"strings"
)

// Events are fixed semantic masks, not a caller-selected arbitrary mutation.
// The ledger independently reclassifies every before/after pair. Counters are
// committed with the data, before any filesystem sync/acknowledgement returns.
type Event string

const (
	ScopePrepareClosed        Event = "scope_prepare_closed"
	ScopeWriterClosed         Event = "scope_writer_closed"
	ScopeSealBegin            Event = "scope_seal_begin"
	ScopeSealClosed           Event = "scope_seal_closed"
	ScopeWatchClosed          Event = "scope_watch_closed"
	ScopeCleanupBegin         Event = "scope_cleanup_begin"
	ScopeCleanupClosed        Event = "scope_cleanup_closed"
	ScopeLeaseUncertain       Event = "scope_lease_uncertain"
	AttemptOwnershipClosed    Event = "attempt_ownership_closed"
	AttemptOwnershipUncertain Event = "attempt_ownership_uncertain"
	ScopeIdentity             Event = "scope_identity"
	ScopeWatchClaim           Event = "scope_watch_claim"
	ScopeWatchReady           Event = "scope_watch_ready"
	ScopeWriterClaim          Event = "scope_writer_claim"
	ScopeContentIdentity      Event = "scope_content_identity"
	ScopeContentClosed        Event = "scope_content_closed"
	ScopeUploadComplete       Event = "scope_upload_complete"
	ScopeSeal                 Event = "scope_seal"
	ScopeSlotClaim            Event = "scope_slot_claim"
	ScopeDeny                 Event = "scope_deny"
	ScopeContentUncertain     Event = "scope_content_uncertain"
	ScopeUnresolved           Event = "scope_unresolved"
	ScopeObservationEvent     Event = "scope_observation"
	AttemptReader             Event = "attempt_reader"
	AttemptCollector          Event = "attempt_collector"
	AttemptPhase              Event = "attempt_phase"
	AttemptPhaseForward       Event = "attempt_phase_forward"
	AttemptForward            Event = "attempt_forward"
	AttemptPhaseSafety        Event = "attempt_phase_safety"
	AttemptReaderClosed       Event = "attempt_reader_closed"
	AttemptReaderUncertain    Event = "attempt_reader_uncertain"
	AttemptCollectorClosed    Event = "attempt_collector_closed"
	AttemptCollectorUncertain Event = "attempt_collector_uncertain"
	AttemptDeny               Event = "attempt_deny"
	AttemptFinish             Event = "attempt_finish"
	AttemptSettle             Event = "attempt_settle"
	AttemptMetrics            Event = "attempt_metrics"
	AttemptQuality            Event = "attempt_quality"
	AttemptObservationEvent   Event = "attempt_observation"
)

func erasePath(m map[string]any, path string) {
	parts := strings.SplitN(path, ".", 2)
	if len(parts) == 1 {
		delete(m, parts[0])
		return
	}
	if child, ok := m[parts[0]].(map[string]any); ok {
		erasePath(child, parts[1])
	}
}

// A comparison belongs to one immutable old/next classification. Nested
// validation after Advance changes counters/clock constructs its own pair.
type maskComparison struct {
	old, next any
	ready     bool
	a, b      map[string]any
	different bool
}

func (c *maskComparison) prepare() {
	if c.ready {
		return
	}
	c.ready = true
	ab, _ := json.Marshal(c.old)
	bb, _ := json.Marshal(c.next)
	d := json.NewDecoder(strings.NewReader(string(ab)))
	d.UseNumber()
	_ = d.Decode(&c.a)
	d = json.NewDecoder(strings.NewReader(string(bb)))
	d.UseNumber()
	_ = d.Decode(&c.b)
	for _, p := range []string{"generation", "normal_transitions_used", "safety_transitions_used", "wall_high_water", "tick_high_water_ms"} {
		erasePath(c.a, p)
		erasePath(c.b, p)
	}
	c.different = !eq(c.a, c.b)
}
func copyJSONObject(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
func eraseCandidatePath(m map[string]any, path string) {
	parts := strings.SplitN(path, ".", 2)
	if len(parts) == 1 {
		delete(m, parts[0])
		return
	}
	if child, ok := m[parts[0]].(map[string]any); ok {
		// Copy the candidate's current ancestor, never the original base:
		// repeated/sibling paths must not restore an earlier erased field.
		child = copyJSONObject(child)
		m[parts[0]] = child
		eraseCandidatePath(child, parts[1])
	}
}
func (c *maskComparison) only(paths ...string) bool {
	c.prepare()
	if len(paths) == 0 {
		return !c.different
	}
	a, b := copyJSONObject(c.a), copyJSONObject(c.b)
	for _, p := range paths {
		eraseCandidatePath(a, p)
		eraseCandidatePath(b, p)
	}
	return eq(a, b)
}
func (c *maskComparison) changed(paths ...string) bool {
	c.prepare()
	return c.different && c.only(paths...)
}
func only(a, b any, paths ...string) bool {
	c := maskComparison{old: a, next: b}
	return c.only(paths...)
}
func changed(a, b any, paths ...string) bool {
	c := maskComparison{old: a, next: b}
	return c.changed(paths...)
}
func advances(oldN, oldS, newN, newS int64, safety bool) bool {
	if safety {
		return newN == oldN && newS == oldS+1
	}
	return newN == oldN+1 && newS == oldS
}
func scopeObservationKey(o ScopeObservation) string {
	o.ObservationID = ""
	o.ObservedAt = ""
	o.ScopeGeneration = 0
	return Digest(o)
}
func attemptObservationKey(o AttemptCleanupObservation) string {
	o.ObservationID = ""
	o.ObservedAt = ""
	o.AttemptGeneration = 0
	return Digest(o)
}
func appendScopeObservation(old, next ExecutionScope) bool {
	if old.ObservationsSaturated {
		return false
	}
	if len(old.Observations) == MaxObservations {
		return next.ObservationsSaturated && eq(old.Observations, next.Observations)
	}
	return !next.ObservationsSaturated && len(next.Observations) == len(old.Observations)+1 && eq(old.Observations, next.Observations[:len(old.Observations)])
}
func appendAttemptObservation(old, next AssessmentAttempt) bool {
	if old.ObservationsSaturated {
		return false
	}
	if len(old.CleanupObservations) == MaxObservations {
		return next.ObservationsSaturated && eq(old.CleanupObservations, next.CleanupObservations)
	}
	return !next.ObservationsSaturated && len(next.CleanupObservations) == len(old.CleanupObservations)+1 && eq(old.CleanupObservations, next.CleanupObservations[:len(old.CleanupObservations)])
}

func scopeMask(old, next ExecutionScope, e Event, slotClaim bool) (bool, bool) {
	c := maskComparison{old: old, next: next}
	return scopeMaskCompared(old, next, e, slotClaim, &c)
}
func scopeMaskCompared(old, next ExecutionScope, e Event, slotClaim bool, comparison *maskComparison) (bool, bool) {
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
			if a.FieldByName(f).IsNil() && !b.FieldByName(f).IsNil() && comparison.changed("filesystem_identity."+jsonFields[i]) {
				ok = true
			}
		}
		safety = old.DenyLatched
		if e == ScopeContentIdentity {
			ok = ok && old.WriterStarted && !old.ContentCloseConfirmed && !old.ContentCloseUncertain
		}
	case ScopePrepareClosed:
		safety = true
		ok = !old.PrepareOwnershipCloseConfirmed && next.PrepareOwnershipCloseConfirmed && comparison.changed("prepare_ownership_close_confirmed")
	case ScopeWriterClosed:
		safety = true
		ok = old.WriterStarted && !old.WriterOwnershipCloseConfirmed && next.WriterOwnershipCloseConfirmed && comparison.changed("writer_ownership_close_confirmed")
	case ScopeSealBegin:
		ok = old.State == "accepting" && old.UploadComplete && old.WriterOwnershipCloseConfirmed && !old.SealStarted && next.SealStarted && ScopeNormalCapacity-old.NormalTransitionsUsed >= 2 && comparison.changed("seal_started")
	case ScopeSealClosed:
		safety = true
		ok = old.SealStarted && !old.SealOwnershipCloseConfirmed && next.SealOwnershipCloseConfirmed && comparison.changed("seal_ownership_close_confirmed")
	case ScopeWatchClosed:
		safety = true
		ok = old.WatchStarted && !old.WatchOwnershipCloseConfirmed && next.WatchOwnershipCloseConfirmed && comparison.changed("watch_ownership_close_confirmed")
	case ScopeCleanupBegin:
		safety = true
		ok = old.DenyLatched && old.CleanupOwnerEpoch == nil && next.CleanupOwnerEpoch != nil && comparison.changed("cleanup_owner_epoch")
	case ScopeCleanupClosed:
		safety = true
		ok = old.CleanupOwnerEpoch != nil && !old.CleanupOwnershipCloseConfirmed && next.CleanupOwnershipCloseConfirmed && (eq(old.Observations, next.Observations) && old.ObservationsSaturated == next.ObservationsSaturated || appendScopeObservation(old, next)) && comparison.changed("cleanup_ownership_close_confirmed", "observations", "observations_saturated")
	case ScopeLeaseUncertain:
		safety = true
		ok = !old.LeaseCloseUncertain && next.LeaseCloseUncertain && comparison.changed("lease_close_uncertain")
	case ScopeWatchClaim:
		ok = !old.WatchStarted && next.WatchStarted && next.WatchEpoch != nil && comparison.changed("watch_started", "watch_epoch")
	case ScopeWatchReady:
		ok = old.State == "preparing" && next.State == "accepting" && comparison.changed("state")
	case ScopeWriterClaim:
		ok = old.State == "accepting" && old.PrepareOwnershipCloseConfirmed && !old.LeaseCloseUncertain && !old.WriterStarted && next.WriterStarted && ScopeNormalCapacity-old.NormalTransitionsUsed >= 6 && comparison.changed("writer_started")
	case ScopeContentClosed:
		safety = old.DenyLatched
		ok = old.WriterStarted && !old.ContentCloseConfirmed && next.ContentCloseConfirmed && comparison.changed("content_close_confirmed")
	case ScopeUploadComplete:
		ok = !old.UploadComplete && next.UploadComplete && old.WriterOwnershipCloseConfirmed && old.ContentCloseConfirmed && !old.ContentCloseUncertain && old.SealedLengthBytes == nil && next.SealedLengthBytes != nil && comparison.changed("upload_complete", "sealed_length_bytes")
	case ScopeSeal:
		ok = old.State == "accepting" && next.State == "sealed" && old.UploadComplete && old.SealStarted && old.SealOwnershipCloseConfirmed && !old.LeaseCloseUncertain && comparison.changed("state")
	case ScopeSlotClaim:
		ok = slotClaim && old.State == "sealed" && comparison.only() && ScopeNormalCapacity-old.NormalTransitionsUsed >= 1
	case ScopeDeny:
		safety = true
		ok = !old.DenyLatched && next.DenyLatched && next.ExecutionDenyLatched && next.State == "cleaning" && comparison.changed("deny_latched", "execution_deny_latched", "reason", "state")
	case ScopeContentUncertain:
		safety = true
		ok = old.WriterStarted && old.DenyLatched && !old.ContentCloseUncertain && next.ContentCloseUncertain && comparison.changed("content_close_uncertain")
	case ScopeUnresolved:
		safety = true
		ok = old.State == "cleaning" && next.State == "cleanup_unresolved" && comparison.changed("state")
	case ScopeObservationEvent:
		safety = true
		ok = old.DenyLatched && appendScopeObservation(old, next) && comparison.changed("observations", "observations_saturated")
	}
	if !safety && (old.DenyLatched || old.ExecutionDenyLatched) {
		ok = false
	}
	return ok, safety
}

var scopeEvents = []Event{ScopePrepareClosed, ScopeWriterClosed, ScopeSealBegin, ScopeSealClosed, ScopeWatchClosed, ScopeCleanupBegin, ScopeCleanupClosed, ScopeLeaseUncertain, ScopeIdentity, ScopeWatchClaim, ScopeWatchReady, ScopeWriterClaim, ScopeContentIdentity, ScopeContentClosed, ScopeUploadComplete, ScopeSeal, ScopeSlotClaim, ScopeDeny, ScopeContentUncertain, ScopeUnresolved, ScopeObservationEvent}

func ScopeTransition(old, next ExecutionScope, slotClaim bool) error {
	if eq(old, next) {
		if slotClaim {
			return ErrInvalid
		}
		return nil
	}
	if next.Validate() != nil || next.Generation != old.Generation+1 || next.WallHighWater < old.WallHighWater || next.TickHighWaterMS < old.TickHighWaterMS {
		return ErrInvalid
	}
	comparison := maskComparison{old: old, next: next}
	for _, event := range scopeEvents {
		if slotClaim && event != ScopeSlotClaim {
			continue
		}
		ok, safety := scopeMaskCompared(old, next, event, slotClaim, &comparison)
		if ok && advances(old.NormalTransitionsUsed, old.SafetyTransitionsUsed, next.NormalTransitionsUsed, next.SafetyTransitionsUsed, safety) {
			return nil
		}
	}
	return ErrInvalid
}
func AdvanceScope(old ExecutionScope, event Event, at string, tick int64, mutate func(*ExecutionScope)) (ExecutionScope, error) {
	next := Clone(old)
	mutate(&next)
	if eq(old, next) {
		return old, nil
	}
	ok, safety := scopeMask(old, next, event, event == ScopeSlotClaim)
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
	if ScopeTransition(old, next, event == ScopeSlotClaim) != nil {
		return old, ErrInvalid
	}
	return next, nil
}

// Slot claims are the only valid semantic event with no changed scope field:
// the ledger independently requires one newly admitted child in that transaction.
func CountSlotClaim(old ExecutionScope, at string, tick int64) (ExecutionScope, error) {
	next := Clone(old)
	next.NormalTransitionsUsed++
	next.Generation++
	if at > next.WallHighWater {
		next.WallHighWater = at
	}
	if tick > next.TickHighWaterMS {
		next.TickHighWaterMS = tick
	}
	if ScopeTransition(old, next, true) != nil {
		return old, ErrInvalid
	}
	return next, nil
}
func AddScopeObservation(old ExecutionScope, o ScopeObservation) (ExecutionScope, error) {
	if old.ObservationsSaturated {
		return old, nil
	}
	for _, prior := range old.Observations {
		if scopeObservationKey(prior) == scopeObservationKey(o) {
			return old, nil
		}
	}
	return AdvanceScope(old, ScopeObservationEvent, o.ObservedAt, old.TickHighWaterMS, func(s *ExecutionScope) {
		if len(s.Observations) == MaxObservations {
			s.ObservationsSaturated = true
		} else {
			o.ScopeGeneration = s.Generation + 1
			s.Observations = append(s.Observations, o)
		}
	})
}

func phaseMonotone(old, next *PhaseRecord) bool {
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
func attemptMask(old, next AssessmentAttempt, e Event) (bool, bool) {
	c := maskComparison{old: old, next: next}
	return attemptMaskCompared(old, next, e, &c)
}
func attemptMaskCompared(old, next AssessmentAttempt, e Event, comparison *maskComparison) (bool, bool) {
	safety := false
	ok := false
	forward := one(old.State, "claimed", "preparing", "running", "checking") && !old.DenyLatched && !old.OwnershipCloseConfirmed && !old.OwnershipCloseUncertain && !old.ReaderCloseConfirmed && !old.CollectorCloseConfirmed && !old.ReaderCloseUncertain && !old.CollectorCloseUncertain
	switch e {
	case AttemptOwnershipClosed:
		safety = true
		ok = !old.OwnershipCloseConfirmed && next.OwnershipCloseConfirmed && one(old.State, "finishing", "held", "terminal") && comparison.changed("ownership_close_confirmed")
	case AttemptOwnershipUncertain:
		safety = true
		ok = !old.OwnershipCloseUncertain && next.OwnershipCloseUncertain && comparison.changed("ownership_close_uncertain")
	case AttemptReader:
		ok = !old.ReaderStarted && next.ReaderStarted && comparison.changed("reader_started")
	case AttemptCollector:
		ok = !old.CollectorStarted && next.CollectorStarted && comparison.changed("collector_started")
	case AttemptPhase:
		for i, p := range []*PhaseRecord{next.PhaseRecords.Workload, next.PhaseRecords.Checker} {
			name := "workload"
			prior := old.PhaseRecords.Workload
			if i == 1 {
				name = "checker"
				prior = old.PhaseRecords.Checker
			}
			if old.ReaderStarted && old.CollectorStarted && prior == nil && p != nil && eq(*p, PhaseRecord{Phase: name, OwnerName: ResourceName(old.Binding.BrokerAuthorityID, old.Slot.AttemptID, name)}) && comparison.changed("phase_records."+name) {
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
				if comparison.changed(paths...) {
					ok = true
				}
			}
		}
	case AttemptForward:
		ok = (old.State == "claimed" && next.State == "preparing" || old.State == "preparing" && next.State == "running" || old.State == "running" && next.State == "checking") && comparison.changed("state")
	case AttemptReaderClosed:
		safety = true
		ok = old.ReaderStarted && !old.ReaderCloseConfirmed && next.ReaderCloseConfirmed && old.GlobalCloseReady() && comparison.changed("reader_close_confirmed")
	case AttemptReaderUncertain:
		safety = true
		ok = old.ReaderStarted && !old.ReaderCloseUncertain && next.ReaderCloseUncertain && comparison.changed("reader_close_uncertain")
	case AttemptCollectorClosed:
		safety = true
		ok = old.CollectorStarted && !old.CollectorCloseConfirmed && next.CollectorCloseConfirmed && old.GlobalCloseReady() && comparison.changed("collector_close_confirmed")
	case AttemptCollectorUncertain:
		safety = true
		ok = old.CollectorStarted && !old.CollectorCloseUncertain && next.CollectorCloseUncertain && comparison.changed("collector_close_uncertain")
	case AttemptDeny:
		safety = true
		ok = !old.DenyLatched && next.DenyLatched && old.FailureCode == "none" && next.FailureCode != "none" && comparison.changed("deny_latched", "failure_code")
	case AttemptFinish:
		safety = true
		ok = (one(old.State, "claimed", "preparing", "running", "checking") && one(next.State, "finishing", "held") || old.State == "finishing" && one(next.State, "held", "terminal") || old.State == "held" && next.State == "terminal") && comparison.changed("state", "outcome")
	case AttemptSettle:
		safety = true
		ok = old.AccountingUnresolved && !next.AccountingUnresolved && !old.ComputeCleanupObserved && next.ComputeCleanupObserved && next.EffectsClosed() && one(next.State, "held", "terminal") && comparison.changed("compute", "accounting_unresolved", "compute_cleanup_observed")
	case AttemptMetrics:
		safety = !forward
		ok = eq(old.Metrics, UnknownMeasurements()) && old.MeasurementDigest == nil && next.MeasurementDigest != nil && comparison.changed("metrics", "measurement_digest")
	case AttemptQuality:
		safety = !forward
		ok = old.Quality.Status == "unknown" && next.Quality.Status != "unknown" && comparison.changed("quality")
	case AttemptObservationEvent:
		safety = true
		ok = appendAttemptObservation(old, next) && comparison.changed("cleanup_observations", "observations_saturated")
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
	if !phaseMonotone(old.PhaseRecords.Workload, next.PhaseRecords.Workload) || !phaseMonotone(old.PhaseRecords.Checker, next.PhaseRecords.Checker) {
		ok = false
	}
	return ok, safety
}

var attemptEvents = []Event{AttemptOwnershipClosed, AttemptOwnershipUncertain, AttemptReader, AttemptCollector, AttemptPhase, AttemptPhaseForward, AttemptForward, AttemptPhaseSafety, AttemptReaderClosed, AttemptReaderUncertain, AttemptCollectorClosed, AttemptCollectorUncertain, AttemptDeny, AttemptFinish, AttemptSettle, AttemptMetrics, AttemptQuality, AttemptObservationEvent}

func AttemptTransition(old, next AssessmentAttempt, s ExecutionScope) error {
	if eq(old, next) {
		return nil
	}
	if next.Validate(s) != nil || next.Generation != old.Generation+1 || next.WallHighWater < old.WallHighWater || next.TickHighWaterMS < old.TickHighWaterMS {
		return ErrInvalid
	}
	comparison := maskComparison{old: old, next: next}
	for _, event := range attemptEvents {
		ok, safety := attemptMaskCompared(old, next, event, &comparison)
		if ok && advances(old.NormalTransitionsUsed, old.SafetyTransitionsUsed, next.NormalTransitionsUsed, next.SafetyTransitionsUsed, safety) {
			return nil
		}
	}
	return ErrInvalid
}
func AdvanceAttempt(old AssessmentAttempt, s ExecutionScope, event Event, at string, tick int64, mutate func(*AssessmentAttempt)) (AssessmentAttempt, error) {
	next := Clone(old)
	mutate(&next)
	if eq(old, next) {
		return old, nil
	}
	ok, safety := attemptMask(old, next, event)
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
	if AttemptTransition(old, next, s) != nil {
		return old, ErrInvalid
	}
	return next, nil
}
func AddAttemptObservation(old AssessmentAttempt, s ExecutionScope, o AttemptCleanupObservation) (AssessmentAttempt, error) {
	if old.ObservationsSaturated {
		return old, nil
	}
	for _, prior := range old.CleanupObservations {
		if attemptObservationKey(prior) == attemptObservationKey(o) {
			return old, nil
		}
	}
	return AdvanceAttempt(old, s, AttemptObservationEvent, o.ObservedAt, old.TickHighWaterMS, func(a *AssessmentAttempt) {
		if len(a.CleanupObservations) == MaxObservations {
			a.ObservationsSaturated = true
		} else {
			o.AttemptGeneration = a.Generation + 1
			a.CleanupObservations = append(a.CleanupObservations, o)
		}
	})
}
