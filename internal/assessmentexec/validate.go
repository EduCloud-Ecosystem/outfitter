package assessmentexec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"regexp"
	"strconv"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/assessment"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

var (
	ErrInvalid   = errors.New("ASSESSMENT_INVALID")
	ErrDisabled  = errors.New("ASSESSMENT_DISABLED")
	ErrDenied    = errors.New("ASSESSMENT_DENIED")
	ErrHeld      = errors.New("ASSESSMENT_UNRESOLVED")
	ErrCollision = errors.New("ASSESSMENT_COLLISION")
	ErrCapacity  = errors.New("ASSESSMENT_METADATA_LIMIT")
	ErrMissing   = errors.New("ASSESSMENT_MISSING")
	ErrStale     = errors.New("ASSESSMENT_STALE")
	ErrBusy      = errors.New("ASSESSMENT_BUSY")
)

var integerToken = regexp.MustCompile(`^(0|[1-9][0-9]{0,15})$`)
var codeID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$`)
var profileID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)
var versionText = regexp.MustCompile(`^[A-Za-z0-9._+-]{1,64}$`)
var pinnedImage = regexp.MustCompile(`^(sha256:[a-f0-9]{64}|[a-zA-Z0-9][a-zA-Z0-9._:/-]*@sha256:[a-f0-9]{64})$`)

// DecodeClosed checks the complete wire shape, including duplicate, missing,
// null and case-aliased keys. Integer lexical rules are checked before Go's
// decoder can normalize an exponent, negative zero or a rounded float.
func DecodeClosed(raw []byte, out any, limit int) error {
	if CheckIntegerDepthJSON(raw, limit) != nil || owneddata.ClosedJSON(raw, out, limit) != nil {
		return ErrInvalid
	}
	return nil
}

// CheckIntegerDepthJSON applies only the existing raw integer/depth rules.
// It does not establish closed shape, UTF8, uniqueness, a single top-level
// value, authorization or ownership. Its callers must retain those checks.
func CheckIntegerDepthJSON(raw []byte, limit int) error {
	if len(raw) == 0 || len(raw) > limit {
		return ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	depth := 0
	for {
		t, e := d.Token()
		if e == io.EOF {
			break
		}
		if e != nil {
			return ErrInvalid
		}
		switch v := t.(type) {
		case json.Delim:
			if v == '{' || v == '[' {
				depth++
				if depth > 24 {
					return ErrInvalid
				}
			} else {
				depth--
			}
		case json.Number:
			if !integerToken.MatchString(string(v)) {
				return ErrInvalid
			}
			n, e := strconv.ParseInt(string(v), 10, 64)
			if e != nil || !owneddata.Uint(n) {
				return ErrInvalid
			}
		}
	}
	if depth != 0 {
		return ErrInvalid
	}
	return nil
}

func Digest(v any) string {
	b, e := assessment.Canonical(v)
	if e != nil {
		return ""
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// Clone removes map/slice/pointer aliases before a transaction callback edits
// metadata. A caller's reference must not mutate the captured pre-write guard.
func Clone[T any](v T) T                  { b, _ := json.Marshal(v); var out T; _ = json.Unmarshal(b, &out); return out }
func eq(a, b any) bool                    { return reflect.DeepEqual(a, b) }
func one(s string, values ...string) bool { return owneddata.OneOf(s, values...) }
func id(s string) bool                    { return owneddata.ID(s) }
func digest(s string) bool                { return owneddata.Hex(s, 32) }
func uintn(n, min, max int64) bool        { return n >= min && n <= max }
func stamp(s string, micro bool) bool     { _, e := owneddata.Time(s, micro); return e == nil }
func profile(p ProfileRef) bool           { return p == (ProfileRef{ID: "cpu-python-r-bounded", Version: 1}) }

func (c CodeBinding) Validate() error {
	if !codeID.MatchString(c.RecipeID) || !digest(c.CodeSHA256) || !codeID.MatchString(c.EnvironmentID) || !pinnedImage.MatchString(c.Image) || len(c.Image) > 512 || !codeID.MatchString(c.InterpreterID) || !versionText.MatchString(c.InterpreterVersion) || !digest(c.InterpreterIdentitySHA256) {
		return ErrInvalid
	}
	return nil
}
func (l Limits) Validate() error {
	if l.CPUCountPerAttempt != 1 || !uintn(l.TimeoutSecondsPerAttempt, 1, 1800) || !uintn(l.MemoryBytes, 32<<20, 1<<30) || l.PIDsLimit != 64 || !uintn(l.InputBudgetBytes, 1, 5000000000) || !uintn(l.InputRows, 1, 1000000) || !uintn(l.OutputBudgetBytes, 1, 65536) || !uintn(l.WorkBudgetBytes, 0, 9999999998) || !uintn(l.ScratchBudgetBytes, 2, 10000000000) || l.InputBudgetBytes+l.OutputBudgetBytes+l.WorkBudgetBytes != l.ScratchBudgetBytes || !uintn(l.MaxAttempts, 1, MaxSlots) {
		return ErrInvalid
	}
	return nil
}
func LimitsFor(m assessment.ManifestV2) Limits {
	c := m.Ceilings
	return Limits{1, c.TimeoutSecondsPerAttempt, c.MemoryBytes, 64, c.InputBytes, c.InputRows, c.OutputBytes, c.ScratchBytes - c.InputBytes - c.OutputBytes, c.ScratchBytes, c.MaxAttempts}
}
func (c CapabilityMapping) Validate() error {
	if c.Schema != CapabilitySchema || !profileID.MatchString(c.ID) || !uintn(c.Version, 1, owneddata.MaxInteger) || !profile(c.ExecutionProfile) || !one(c.Architecture, "arm64", "amd64") || c.Workload.Validate() != nil || c.Checker.Validate() != nil || c.Limits.Validate() != nil || !eq(c.PhaseOrder, []string{"workload", "checker"}) || c.MaxRunningResources != 1 || !eq(c.DataClassifications, []string{"synthetic"}) {
		return ErrInvalid
	}
	for _, argv := range [][]string{c.WorkloadArgv, c.CheckerArgv} {
		if len(argv) < 1 || len(argv) > 16 {
			return ErrInvalid
		}
		total := 0
		for _, a := range argv {
			total += len(a)
			if len(a) < 1 || len(a) > 256 {
				return ErrInvalid
			}
			for _, r := range a {
				if r < 32 || r > 126 {
					return ErrInvalid
				}
			}
		}
		if total > 2048 {
			return ErrInvalid
		}
	}
	for _, i := range []ImplementationBinding{c.Provider, c.Storage, c.Collector, c.Quality} {
		if !profileID.MatchString(i.ID) || !uintn(i.Version, 1, owneddata.MaxInteger) || !digest(i.SourceSHA256) {
			return ErrInvalid
		}
	}
	return nil
}
func (b ScopeBinding) Validate() error {
	if !id(b.BrokerAuthorityID) || !id(b.WaypointAuthorityID) || !id(b.ScopeID) || !id(b.AssessmentID) || !id(b.ProjectID) || b.ManifestSchema != assessment.ManifestSchemaV2 || !digest(b.ManifestSHA256) || !profile(b.ExecutionProfile) || !profileID.MatchString(b.Capability.ID) || !uintn(b.Capability.Version, 1, owneddata.MaxInteger) || !digest(b.Capability.SHA256) || b.Workload.Validate() != nil || b.Checker.Validate() != nil {
		return ErrInvalid
	}
	return nil
}
func (s ExecutionScope) Binding() ScopeBinding {
	return ScopeBinding{s.BrokerAuthorityID, s.WaypointAuthorityID, s.ScopeID, s.AssessmentID, s.ProjectID, s.ManifestSchema, s.ManifestSHA256, s.ExecutionProfile, CapabilityRef{s.Capability.ID, s.Capability.Version, s.CapabilitySHA256}, s.Capability.Workload, s.Capability.Checker}
}
func validSlot(s Slot) bool {
	return id(s.AttemptID) && id(s.ReservationID) && s.AttemptID != s.ReservationID && s.StageID == "analysis" && one(s.SampleRole, "fitting", "held_out") && uintn(s.SampleSize, 1, 1000000) && uintn(s.Repetition, 1, 3)
}

// CompletePlan deliberately narrows only this execution branch. The accepted
// pure v2 codec continues accepting structural IDs and partial reservation sets.
func CompletePlan(m assessment.ManifestV2) bool {
	if len(m.Reservations) != int(m.Ceilings.MaxAttempts) {
		return false
	}
	i := 0
	seen := map[string]bool{}
	sizes := append(append([]int64{}, m.Design.FittingSizes...), m.Design.HeldOutSize)
	for j, n := range sizes {
		role := "fitting"
		if j == len(sizes)-1 {
			role = "held_out"
		}
		for rep := int64(1); rep <= m.Design.Repetitions; rep++ {
			if i >= len(m.Reservations) {
				return false
			}
			s := m.Reservations[i]
			i++
			if !validSlot(s) || s.SampleRole != role || s.SampleSize != n || s.Repetition != rep || seen[s.AttemptID] || seen[s.ReservationID] {
				return false
			}
			seen[s.AttemptID] = true
			seen[s.ReservationID] = true
		}
	}
	return i == len(m.Reservations)
}

// OwnedProjection is a validation/filesystem compatibility view only. It is
// never placed in a v6 journal or used as an authority or budget reservation.
func (s ExecutionScope) OwnedProjection() owneddata.Scope {
	f := s.FilesystemIdentity
	obs := make([]owneddata.Observation, 0, len(s.Observations))
	for _, o := range s.Observations {
		reason := o.Reason
		if one(reason, "reader_active", "collector_active") {
			reason = "writer_active"
		}
		obs = append(obs, owneddata.Observation{Schema: owneddata.ObservationSchema, ObservationID: o.ObservationID, ScopeID: o.ScopeID, ScopeGeneration: o.ScopeGeneration, ObservedAt: o.ObservedAt, Method: "synthetic_local_scope_v1", Assurance: o.Assurance, NewAdmission: o.NewAdmission, WritersDrained: o.WritersDrained, ScopeAccessRevocation: o.ScopeAccessRevocation, OwnedRemoval: o.OwnedRemoval, Reason: reason, CleanupTrusted: o.CleanupTrusted, ActualResourceAccepted: o.ActualResourceAccepted})
	}
	return owneddata.Scope{ScopeID: s.ScopeID, AssessmentID: s.AssessmentID, ProjectID: s.ProjectID, WaypointAuthorityID: s.WaypointAuthorityID, ManifestSHA256: s.ManifestSHA256, ManifestExpiresAt: s.ManifestExpiresAt, BoundaryID: s.BoundaryID, CreatedAt: s.CreatedAt, ExpiresAt: s.ExpiresAt, WallHighWater: s.WallHighWater, Generation: s.Generation, State: s.State, DenyLatched: s.DenyLatched, Reason: s.Reason, WriterStarted: s.WriterStarted, WriterEpoch: s.WriterEpoch, WatchEpoch: s.WatchEpoch, WatchStarted: s.WatchStarted, UploadComplete: s.UploadComplete, ContentCloseConfirmed: s.ContentCloseConfirmed, ContentCloseUncertain: s.ContentCloseUncertain, Assurance: s.Assurance, Observations: obs, ObservationsSaturated: s.ObservationsSaturated, DeclaredUploadCeilingBytes: s.DeclaredUploadCeilingBytes, AdmittingOwnerEpoch: s.AdmittingOwnerEpoch, ClockEpoch: s.ClockEpoch, DeadlineTickMS: s.DeadlineTickMS, TickHighWaterMS: s.TickHighWaterMS, FilesystemIdentity: owneddata.FilesystemBinding{Root: f.Root, Scope: f.Scope, WriterLease: f.WriterLease, WatchLease: f.WatchLease, Content: f.Content, OwnerRead: f.OwnerRead, WatchRead: f.WatchRead, WatchWrite: f.WatchWrite}}
}
func (s ExecutionScope) Validate() error {
	if s.CleanupOwnerEpoch != nil && !id(*s.CleanupOwnerEpoch) || s.CleanupOwnershipCloseConfirmed && s.CleanupOwnerEpoch == nil || s.WriterOwnershipCloseConfirmed && !s.WriterStarted || s.SealOwnershipCloseConfirmed && !s.SealStarted || s.WatchOwnershipCloseConfirmed && !s.WatchStarted {
		return ErrInvalid
	}

	if s.Schema != ScopeSchema || s.Binding().Validate() != nil || s.OwnedProjection().Validate() != nil || s.ManifestSchema != s.Manifest.Schema || s.Manifest.AssessmentID != s.AssessmentID || s.Manifest.ProjectID != s.ProjectID || Digest(s.Manifest) != s.ManifestSHA256 || s.Manifest.ExpiresAt != s.ManifestExpiresAt || s.Manifest.DataClassification != "synthetic" || !eq(s.Manifest.ExecutionProfile, s.ExecutionProfile) || !eq(s.Manifest.Reservations, s.Slots) || !CompletePlan(s.Manifest) || s.Capability.Validate() != nil || Digest(s.Capability) != s.CapabilitySHA256 || s.Limits != LimitsFor(s.Manifest) || s.Limits != s.Capability.Limits || s.Capability.Architecture != s.Manifest.Hardware.Architecture {
		return ErrInvalid
	}
	b, e := json.Marshal(s.Manifest)
	if e != nil {
		return ErrInvalid
	}
	if _, e = assessment.DecodeManifestV2(b); e != nil {
		return ErrInvalid
	}
	w := s.Manifest.Workload
	c := s.Capability.Workload
	if w.RecipeID != c.RecipeID || w.RecipeSHA256 != c.CodeSHA256 || w.EnvironmentID != c.EnvironmentID || w.Image != c.Image || s.DeclaredUploadCeilingBytes > s.Limits.InputBudgetBytes || !uintn(s.NormalTransitionsUsed, 0, ScopeNormalCapacity) || !uintn(s.SafetyTransitionsUsed, 0, ScopeSafetyCapacity) || s.Generation != 1+s.NormalTransitionsUsed+s.SafetyTransitionsUsed || s.DenyLatched != s.ExecutionDenyLatched {
		return ErrInvalid
	}
	if s.SealedLengthBytes != nil && (!uintn(*s.SealedLengthBytes, 1, s.DeclaredUploadCeilingBytes) || !s.UploadComplete) || s.State == "sealed" && (s.SealedLengthBytes == nil || s.ContentCloseUncertain) {
		return ErrInvalid
	}
	f := s.FilesystemIdentity
	for _, i := range []*FilesystemIdentity{f.ExecutionLease, f.WorkRoot, f.OutputRoot} {
		if i != nil && !i.Valid() {
			return ErrInvalid
		}
	}
	if one(s.State, "accepting", "sealed") && (f.ExecutionLease == nil || f.WorkRoot == nil || f.OutputRoot == nil) {
		return ErrInvalid
	}
	seen := map[string]bool{}
	for _, o := range s.Observations {
		if o.Schema != ScopeObservationSchema || o.Method != "synthetic_execution_scope_v1" || !one(o.ReadersDrained, "observed", "failed", "unknown") || !one(o.CollectorDrained, "observed", "failed", "unknown") || o.OwnedRemoval == "observed_absent" && (o.ReadersDrained != "observed" || o.CollectorDrained != "observed") {
			return ErrInvalid
		}
		k := scopeObservationKey(o)
		if seen[k] {
			return ErrInvalid
		}
		seen[k] = true
	}
	if s.ObservationsSaturated && len(s.Observations) != MaxObservations {
		return ErrInvalid
	}
	return nil
}

func phaseFinal(p *PhaseRecord) bool {
	return p == nil || (!p.CreateClaimed || p.CreateReturned && p.ResourceIdentity != nil && !p.CreateAmbiguous && (!p.StartClaimed || p.StartReturned) && p.StoppedObserved && p.RemovalObserved) && (!p.TransportStarted || p.TransportCloseConfirmed) && !p.TransportCloseUncertain
}

// GlobalCloseReady requires definitive phase resource, callback and transport
// closure after all further phases have been irreversibly suppressed. It does
// not infer global reader or collector closure from those phase observations.
func (a AssessmentAttempt) GlobalCloseReady() bool {
	return one(a.State, "finishing", "held", "terminal") && phaseFinal(a.PhaseRecords.Workload) && phaseFinal(a.PhaseRecords.Checker)
}

func (a AssessmentAttempt) EffectsClosed() bool {
	return phaseFinal(a.PhaseRecords.Workload) && phaseFinal(a.PhaseRecords.Checker) && (!a.ReaderStarted || a.ReaderCloseConfirmed) && !a.ReaderCloseUncertain && (!a.CollectorStarted || a.CollectorCloseConfirmed) && !a.CollectorCloseUncertain
}

// TransientOwnershipClosed is metadata-only. A healthy original watcher may
// stay live during admission; cleanup always requires its checked completion.
func (s ExecutionScope) TransientOwnershipClosed(cleanup bool) bool {
	return s.PrepareOwnershipCloseConfirmed && !s.LeaseCloseUncertain && (!s.WriterStarted || s.WriterOwnershipCloseConfirmed) && (!s.SealStarted || s.SealOwnershipCloseConfirmed) && (!s.WatchStarted || s.WatchOwnershipCloseConfirmed || !cleanup && !s.DenyLatched)
}

func (a AssessmentAttempt) SerialClear() bool {
	return a.State == "terminal" && a.OwnershipCloseConfirmed && !a.OwnershipCloseUncertain && !a.AccountingUnresolved && a.ComputeCleanupObserved && a.EffectsClosed()
}
func (p PhaseRecord) Validate(a AssessmentAttempt, phase string) error {
	if p.CreateClaimed && p.RemovalObserved && p.ResourceIdentity == nil {
		return ErrInvalid
	}
	if p.Phase != phase || p.OwnerName != ResourceName(a.Binding.BrokerAuthorityID, a.Slot.AttemptID, phase) || p.CreateClaimed && !p.TransportStarted || p.CreateReturned && !p.CreateClaimed || p.CreateAmbiguous && !p.CreateClaimed || (p.StoppedObserved || p.RemovalObserved) && !p.CreateClaimed || p.StartClaimed && (!p.CreateReturned || p.ResourceIdentity == nil) || p.StartReturned && !p.StartClaimed || p.StartedObserved && !p.StartClaimed || p.RemovalObserved && !p.StoppedObserved || (p.TransportCloseConfirmed || p.TransportCloseUncertain) && !p.TransportStarted {
		return ErrInvalid
	}
	if p.ExitCode != nil && (!uintn(*p.ExitCode, 0, 255) || !p.StartedObserved || p.FinishedAt == nil) || p.FinishedAt != nil && (p.ExitCode == nil || !stamp(*p.FinishedAt, true)) || p.StartedAt != nil && (!p.StartedObserved || !stamp(*p.StartedAt, true)) {
		return ErrInvalid
	}
	if p.StartedAt != nil && (*p.StartedAt < a.ClaimedAt || *p.StartedAt > a.WallHighWater) || p.FinishedAt != nil && (*p.FinishedAt < a.ClaimedAt || *p.FinishedAt > a.WallHighWater || p.StartedAt != nil && *p.FinishedAt < *p.StartedAt) {
		return ErrInvalid
	}
	if r := p.ResourceIdentity; r != nil {
		if !owneddata.Hex(r.ContainerID, 32) || r.BrokerOwner != a.Binding.BrokerAuthorityID || r.AttemptID != a.Slot.AttemptID || r.Phase != phase || r.ScopeID != a.Binding.ScopeID || r.CapabilitySHA256 != a.Binding.Capability.SHA256 || !p.CreateReturned {
			return ErrInvalid
		}
	}
	return nil
}
func ResourceName(owner, attempt, phase string) string {
	return "outfitter-assess-" + Digest([]string{owner, attempt, phase})[:32]
}
func AttemptBindingDigest(b ScopeBinding, s Slot, nonce string) string {
	return Digest(struct {
		Binding ScopeBinding `json:"binding"`
		Slot    Slot         `json:"slot"`
		Nonce   string       `json:"handoff_nonce"`
	}{b, s, nonce})
}
func (a AssessmentAttempt) Validate(s ExecutionScope) error {
	if a.OwnershipCloseConfirmed && !one(a.State, "finishing", "held", "terminal") {
		return ErrInvalid
	}
	if a.Schema != AttemptSchema || !eq(a.Binding, s.Binding()) || !validSlot(a.Slot) || a.BindingDigest != AttemptBindingDigest(a.Binding, a.Slot, a.HandoffNonce) || !id(a.HandoffNonce) || !id(a.ClaimEpoch) || a.ClockEpoch != s.ClockEpoch || !stamp(a.ClaimedAt, true) || !stamp(a.Deadline, false) || !stamp(a.WallHighWater, true) || !uintn(a.DeadlineTickMS, 1, s.DeadlineTickMS) || !owneddata.Uint(a.TickHighWaterMS) || !uintn(a.NormalTransitionsUsed, 0, AttemptNormalCapacity) || !uintn(a.SafetyTransitionsUsed, 0, AttemptSafetyCapacity) || a.Generation != 1+a.NormalTransitionsUsed+a.SafetyTransitionsUsed || !one(a.State, "claimed", "preparing", "running", "checking", "finishing", "held", "terminal") || !one(a.Outcome, "unknown", "succeeded", "failed", "cancelled") || !one(a.FailureCode, "none", "cancelled", "expired", "access_ended", "restart", "clock_rollback", "owner_mismatch", "io_uncertain", "execution_failed", "output_limit", "quality_failed", "measurement_unknown", "cleanup_unknown", "create_ambiguous") {
		return ErrInvalid
	}
	found := false
	for _, slot := range s.Slots {
		if slot == a.Slot {
			found = true
		}
	}
	if !found {
		return ErrInvalid
	}
	claimed, _ := owneddata.Time(a.ClaimedAt, true)
	deadline, _ := owneddata.Time(a.Deadline, false)
	high, _ := owneddata.Time(a.WallHighWater, true)
	expires, _ := owneddata.Time(s.ExpiresAt, false)
	if !deadline.Equal(claimed.Truncate(time.Second).Add(time.Duration(s.Limits.TimeoutSecondsPerAttempt)*time.Second)) || deadline.After(expires) || high.Before(claimed) || a.Compute.ReservedCPUSeconds != s.Limits.TimeoutSecondsPerAttempt {
		return ErrInvalid
	}
	if a.AccountingUnresolved {
		if a.Compute.ChargedCPUSeconds != 0 || a.Compute.Provenance != "reserved" {
			return ErrInvalid
		}
	} else if a.Compute.ChargedCPUSeconds != a.Compute.ReservedCPUSeconds || a.Compute.Provenance != "estimated_upper_bound" || !a.ComputeCleanupObserved || !one(a.State, "held", "terminal") {
		return ErrInvalid
	}
	if a.ComputeCleanupObserved && !a.EffectsClosed() {
		// A later uncertain close cannot undo a previously committed full charge.
		// The transition guard verifies that this was previously settled.
		if a.AccountingUnresolved {
			return ErrInvalid
		}
	}
	if (a.ReaderCloseConfirmed || a.ReaderCloseUncertain) && !a.ReaderStarted || (a.CollectorCloseConfirmed || a.CollectorCloseUncertain) && !a.CollectorStarted {
		return ErrInvalid
	}
	if (a.ReaderCloseConfirmed || a.CollectorCloseConfirmed) && !one(a.State, "finishing", "held", "terminal") {
		return ErrInvalid
	}
	if p := a.PhaseRecords.Workload; p != nil {
		if p.Validate(a, "workload") != nil {
			return ErrInvalid
		}
	}
	if p := a.PhaseRecords.Checker; p != nil {
		if p.Validate(a, "checker") != nil || a.PhaseRecords.Workload == nil {
			return ErrInvalid
		}
	}
	if a.Outcome == "succeeded" && (a.AccountingUnresolved || a.Quality.Status != "passed" || a.PhaseRecords.Checker == nil || a.PhaseRecords.Checker.ExitCode == nil || *a.PhaseRecords.Checker.ExitCode != 0) {
		return ErrInvalid
	}
	if a.Metrics.Validate() != nil || a.Quality.Checker != s.Capability.Checker || !one(a.Quality.Status, "unknown", "passed", "failed") || a.Quality.Status == "unknown" && a.Quality.Source != "none" || a.Quality.Status != "unknown" && a.Quality.Source != "unverified_checker" {
		return ErrInvalid
	}
	if a.MeasurementDigest != nil && (!digest(*a.MeasurementDigest) || *a.MeasurementDigest != Digest(a.Metrics)) {
		return ErrInvalid
	}
	if a.CleanupObservations == nil || len(a.CleanupObservations) > MaxObservations || a.ObservationsSaturated && len(a.CleanupObservations) != MaxObservations {
		return ErrInvalid
	}
	seen := map[string]bool{}
	ids := map[string]bool{}
	for _, o := range a.CleanupObservations {
		if o.Schema != AttemptObservationSchema || !id(o.ObservationID) || ids[o.ObservationID] || o.AttemptID != a.Slot.AttemptID || o.ScopeID != s.ScopeID || !uintn(o.AttemptGeneration, 1, a.Generation) || !stamp(o.ObservedAt, true) || !one(o.WorkloadRemoval, "observed_absent", "failed", "unknown", "not_created") || !one(o.CheckerRemoval, "observed_absent", "failed", "unknown", "not_created") || !one(o.ReadersDrained, "observed", "failed", "unknown") || !one(o.CollectorDrained, "observed", "failed", "unknown") || !one(o.Reason, "none", "resource_active", "late_create_possible", "reader_active", "collector_active", "owner_mismatch", "io_uncertain", "record_uncertain", "clock_uncertain") || o.Assurance != "synthetic" || o.CleanupTrusted || o.ActualResourceAccepted {
			return ErrInvalid
		}
		ids[o.ObservationID] = true
		k := attemptObservationKey(o)
		if seen[k] {
			return ErrInvalid
		}
		seen[k] = true
	}
	return nil
}
