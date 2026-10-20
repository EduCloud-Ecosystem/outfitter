package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"reflect"
	"sort"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

func ownedID() (string, error) {
	var value [16]byte
	if _, e := rand.Read(value[:]); e != nil {
		return "", owneddata.ErrUnresolved
	}
	return hex.EncodeToString(value[:]), nil
}

func (b *Broker) ownedRead(c owneddata.Control) (owneddata.Scope, string, error) {
	var s owneddata.Scope
	var authority string
	if !owneddata.ID(c.ScopeID) || !owneddata.ID(c.ExpectedBrokerAuthorityID) || !owneddata.ID(c.WaypointAuthorityID) {
		return s, authority, owneddata.ErrInvalid
	}
	e := b.Store.View(func(d ledger.Data) error {
		if d.Version != 6 || d.OwnedDataIdentity == nil || d.Authority.ID != c.ExpectedBrokerAuthorityID || d.OwnedDataIdentity.WaypointAuthorityID != c.WaypointAuthorityID {
			return owneddata.ErrDenied
		}
		if b.owned != nil && b.owned.checkState(b.Store.Dir, d.OwnedDataIdentity.Directory) != nil {
			return owneddata.ErrUnresolved
		}
		authority = d.Authority.ID
		var exists bool
		s, exists = (*d.OwnedDataScopes)[c.ScopeID]
		if !exists {
			return owneddata.ErrMissing
		}
		return nil
	})
	return s, authority, ownedError(e)
}
func ownedError(e error) error {
	if e == nil {
		return nil
	}
	for _, known := range []error{owneddata.ErrInvalid, owneddata.ErrDisabled, owneddata.ErrDenied, owneddata.ErrMissing, owneddata.ErrCollision, owneddata.ErrStale, owneddata.ErrBusy} {
		if errors.Is(e, known) {
			return known
		}
	}
	return owneddata.ErrUnresolved
}
func ownedStatus(s owneddata.Scope, authority string) owneddata.Status {
	code := "OK"
	if s.DenyLatched {
		code = "DENIED"
	}
	if s.State == "cleanup_unresolved" {
		code = "UNRESOLVED"
	}
	return owneddata.Status{Schema: owneddata.StatusSchema, ScopeID: s.ScopeID, AuthorityID: authority, WaypointAuthorityID: s.WaypointAuthorityID, ProjectID: s.ProjectID, ManifestSHA256: s.ManifestSHA256, DeclaredUploadCeilingBytes: s.DeclaredUploadCeilingBytes, Generation: s.Generation, State: s.State, DenyLatched: s.DenyLatched, ExpiresAt: s.ExpiresAt, WriterEpoch: s.WriterEpoch, WatchEpoch: s.WatchEpoch, AdmittingOwnerEpoch: s.AdmittingOwnerEpoch, Code: code, Observations: s.Observations, TestPermission: "none", Liveness: "not_observed"}
}
func (b *Broker) ownedChange(c owneddata.Control, point string, fn func(*owneddata.Scope) error) (owneddata.Scope, error) {
	var out owneddata.Scope
	if b.owned != nil && b.owned.fail("before-journal:"+point) != nil {
		return out, owneddata.ErrUnresolved
	}
	e := b.Store.Update(func(d *ledger.Data) error {
		if d.Version != 6 || d.Authority.ID != c.ExpectedBrokerAuthorityID || d.OwnedDataIdentity.WaypointAuthorityID != c.WaypointAuthorityID {
			return owneddata.ErrDenied
		}
		s, ok := (*d.OwnedDataScopes)[c.ScopeID]
		if !ok {
			return owneddata.ErrMissing
		}
		if s.Generation >= owneddata.MaxInteger {
			return owneddata.ErrUnresolved
		}
		before := s
		if e := fn(&s); e != nil {
			return e
		}
		if !reflect.DeepEqual(before, s) {
			s.Generation++
			(*d.OwnedDataScopes)[c.ScopeID] = s
		}
		out = s
		return nil
	})
	if e == nil && b.owned != nil && b.owned.fail("after-journal:"+point) != nil {
		return out, owneddata.ErrUnresolved
	}
	return out, ownedError(e)
}
func (b *Broker) ownedNow() (ownedClock, error) {
	if b.owned == nil || b.owned.clock == nil {
		return ownedClock{}, owneddata.ErrDisabled
	}
	n, e := b.owned.clock()
	n.Wall = n.Wall.UTC().Truncate(time.Microsecond)
	if e != nil || !owneddata.ID(n.Epoch) || !owneddata.Uint(n.TickMS) || n.Wall.IsZero() {
		return ownedClock{}, owneddata.ErrUnresolved
	}
	return n, nil
}
func ownedClockReason(s owneddata.Scope, n ownedClock) string {
	high, _ := owneddata.Time(s.WallHighWater, true)
	expires, _ := owneddata.Time(s.ExpiresAt, false)
	if n.Epoch != s.ClockEpoch || n.Wall.Before(high) || n.TickMS < s.TickHighWaterMS {
		return "clock_rollback"
	}
	if !n.Wall.Before(expires) || n.TickMS >= s.DeadlineTickMS {
		return "expired"
	}
	return ""
}
func applyOwnedClock(s *owneddata.Scope, n ownedClock) {
	high, _ := owneddata.Time(s.WallHighWater, true)
	if !n.Wall.Before(high) {
		s.WallHighWater = owneddata.Stamp(n.Wall)
	}
	if n.Epoch == s.ClockEpoch && n.TickMS > s.TickHighWaterMS {
		s.TickHighWaterMS = n.TickMS
	}
}
func (b *Broker) ownedDeny(c owneddata.Control, reason string) (owneddata.Scope, error) {
	if reason == "ipc_uncertain" {
		reason = "io_uncertain"
	}
	if !owneddata.OneOf(reason, "cancelled", "access_ended", "expired", "restart", "clock_rollback", "incomplete_upload", "io_uncertain", "owner_mismatch", "observations_full") {
		return owneddata.Scope{}, owneddata.ErrInvalid
	}
	n, clockError := b.ownedNow()
	return b.ownedChange(c, "deny", func(s *owneddata.Scope) error {
		if !s.DenyLatched {
			s.DenyLatched = true
			s.Reason = reason
			s.State = "cleaning"
		}
		if clockError == nil {
			applyOwnedClock(s, n)
		}
		return nil
	})
}

// OwnedData is the single existing-broker entrypoint. Production construction
// leaves b.owned nil. No command can install the test-only boundary.
func (b *Broker) OwnedData(ctx context.Context, c owneddata.Control, body io.Reader) (owneddata.Status, error) {
	if owneddata.OneOf(c.Operation, "prepare", "ingest", "seal") && b.owned == nil {
		return owneddata.Status{}, owneddata.ErrDisabled
	}
	s, authority, e := b.ownedRead(c)
	if c.Operation == "prepare" {
		return b.ownedPrepare(ctx, c, s, authority, e)
	}
	if e != nil {
		return owneddata.Status{}, e
	}
	switch c.Operation {
	case "status":
		return ownedStatus(s, authority), nil
	case "cancel":
		if _, e = b.ownedDeny(c, c.Reason); e != nil {
			return owneddata.Status{}, e
		}
		return b.ownedCleanup(c)
	case "reconcile":
		if _, e = b.ownedDeny(c, "restart"); e != nil {
			return owneddata.Status{}, e
		}
		return b.ownedCleanup(c)
	case "ingest":
		return b.ownedIngest(ctx, c, body)
	case "seal":
		return b.ownedSeal(c)
	default:
		return owneddata.Status{}, owneddata.ErrInvalid
	}
}

func (b *Broker) ownedPrepare(ctx context.Context, c owneddata.Control, existing owneddata.Scope, authority string, lookup error) (owneddata.Status, error) {
	if lookup != nil && !errors.Is(lookup, owneddata.ErrMissing) {
		return owneddata.Status{}, lookup
	}
	if lookup == nil {
		if existing.ProjectID != c.ProjectID || existing.AssessmentID != c.AssessmentID || existing.ManifestSHA256 != c.ManifestSHA256 || existing.ManifestExpiresAt != c.ManifestExpiresAt || existing.ExpiresAt != c.ExpiresAt || existing.AdmittingOwnerEpoch != c.AdmittingOwnerEpoch || existing.DeclaredUploadCeilingBytes != c.DeclaredUploadCeilingBytes {
			return owneddata.Status{}, owneddata.ErrCollision
		}
		return ownedStatus(existing, authority), nil // lookup-only: never respawn/reopen
	}
	x := b.owned
	if x == nil || x.checkRoot() != nil || x.policy == nil || x.spawnWatch == nil || !owneddata.ID(c.ProjectID) || !owneddata.ID(c.AdmittingOwnerEpoch) || !owneddata.Hex(c.ManifestSHA256, 32) || !owneddata.Uint(c.DeclaredUploadCeilingBytes) || c.DeclaredUploadCeilingBytes == 0 || c.DeclaredUploadCeilingBytes > c.ManifestScratchBytes {
		return owneddata.Status{}, owneddata.ErrDenied
	}
	policy, e := x.policy(c.ProjectID, c.AdmittingOwnerEpoch)
	if e != nil || !owneddata.Uint(policy.MaxUploadBytes) || c.DeclaredUploadCeilingBytes > policy.MaxUploadBytes {
		return owneddata.Status{}, owneddata.ErrDenied
	}
	n, e := b.ownedNow()
	if e != nil {
		return owneddata.Status{}, e
	}
	expiry, e := owneddata.Time(c.ManifestExpiresAt, false)
	if e != nil {
		return owneddata.Status{}, owneddata.ErrInvalid
	}
	for _, cap := range []time.Time{policy.ProjectEnd, policy.AuthorizationExpiry, policy.PolicyExpiry} {
		if cap.IsZero() {
			return owneddata.Status{}, owneddata.ErrDenied
		}
		if cap.Before(expiry) {
			expiry = cap
		}
	}
	expiry = expiry.UTC().Truncate(time.Second)
	if expiry.Format(owneddata.SecondUTC) != c.ExpiresAt || !expiry.After(n.Wall) || expiry.Sub(n.Wall)/time.Millisecond <= 0 {
		return owneddata.Status{}, owneddata.ErrDenied
	}
	delta := int64(expiry.Sub(n.Wall) / time.Millisecond)
	if delta > owneddata.MaxInteger-n.TickMS {
		return owneddata.Status{}, owneddata.ErrDenied
	}
	ownerID, e := pipeIdentity(x.ownerRead, false)
	if e != nil || livePipe(x.ownerRead, ownerID) != "observed_current" {
		return owneddata.Status{}, owneddata.ErrDenied
	}
	watchRead, e := pipeIdentity(x.watchRead, false)
	if e != nil {
		return owneddata.Status{}, owneddata.ErrDenied
	}
	watchWrite, e := pipeIdentity(x.watchWrite, true)
	if e != nil {
		return owneddata.Status{}, owneddata.ErrDenied
	}
	writer, e := ownedID()
	if e != nil {
		return owneddata.Status{}, e
	}
	s := owneddata.Scope{ScopeID: c.ScopeID, AssessmentID: c.AssessmentID, ProjectID: c.ProjectID, WaypointAuthorityID: c.WaypointAuthorityID, ManifestSHA256: c.ManifestSHA256, ManifestExpiresAt: c.ManifestExpiresAt, BoundaryID: x.id, CreatedAt: owneddata.Stamp(n.Wall), ExpiresAt: c.ExpiresAt, WallHighWater: owneddata.Stamp(n.Wall), Generation: 1, State: "preparing", Reason: "none", WriterEpoch: writer, Assurance: "synthetic", Observations: []owneddata.Observation{}, DeclaredUploadCeilingBytes: c.DeclaredUploadCeilingBytes, AdmittingOwnerEpoch: c.AdmittingOwnerEpoch, ClockEpoch: n.Epoch, DeadlineTickMS: n.TickMS + delta, TickHighWaterMS: n.TickMS, FilesystemIdentity: owneddata.FilesystemBinding{Root: x.rootIdentity, OwnerRead: ownerID, WatchRead: watchRead, WatchWrite: watchWrite}}
	if s.Validate() != nil {
		return owneddata.Status{}, owneddata.ErrInvalid
	}
	e = b.Store.Update(func(d *ledger.Data) error {
		if d.Version != 6 || d.Authority.ID != c.ExpectedBrokerAuthorityID || d.OwnedDataIdentity.WaypointAuthorityID != c.WaypointAuthorityID {
			return owneddata.ErrDenied
		}
		if _, ok := d.Projects[c.ProjectID]; !ok {
			return owneddata.ErrDenied
		}
		if _, ok := (*d.OwnedDataScopes)[c.ScopeID]; ok {
			return owneddata.ErrCollision
		}
		if len(d.Runs)+len(d.NativeRuns)+len(*d.OwnedDataScopes) >= ledger.MaxRecords {
			return owneddata.ErrDenied
		}
		(*d.OwnedDataScopes)[c.ScopeID] = s
		return nil
	})
	if e != nil {
		return owneddata.Status{}, ownedError(e)
	}
	fail := func(reason string) (owneddata.Status, error) {
		_, _ = b.ownedDeny(c, reason)
		_, _ = b.ownedCleanup(c)
		return owneddata.Status{}, owneddata.ErrUnresolved
	}
	if x.fail("after-journal:prepare") != nil {
		return fail("io_uncertain")
	}
	if pairedWatchPipe(x.watchRead, x.watchWrite) != nil {
		return fail("owner_mismatch")
	}
	fs, e := x.createScope(s, authority)
	if e != nil {
		return fail("io_uncertain")
	}
	s, e = b.ownedChange(c, "scope-identity", func(v *owneddata.Scope) error {
		if v.DenyLatched {
			return owneddata.ErrDenied
		}
		v.FilesystemIdentity = fs
		return nil
	})
	if e != nil {
		return fail("io_uncertain")
	}
	epoch, e := ownedID()
	if e != nil {
		return fail("io_uncertain")
	}
	s, e = b.ownedChange(c, "watch-claim", func(v *owneddata.Scope) error {
		if v.WatchStarted || v.DenyLatched {
			return owneddata.ErrDenied
		}
		v.WatchStarted = true
		v.WatchEpoch = &epoch
		return nil
	})
	if e != nil {
		return fail("io_uncertain")
	}
	ack, e := x.spawnWatch(s, x.ownerRead, x.watchWrite)
	// The child must now own the only watch-write reference. Never keep a copy
	// alive to turn a dead watcher into a falsely live pipe.
	_ = x.watchWrite.Close()
	x.watchWrite = nil
	if e != nil {
		return fail("restart")
	}
	defer ack.Close()
	ready, e := readOwnedReady(ctx, ack)
	if e != nil {
		return fail("io_uncertain")
	}
	current, authority, e := b.ownedRead(c)
	if e != nil || !readyMatches(ready, current, authority) {
		return fail("io_uncertain")
	}
	if _, e = b.ownedCurrent(c, current, true); e != nil {
		return fail("restart")
	}
	status := ownedStatus(current, authority)
	status.TestPermission = "synthetic_ingest_only"
	status.Liveness = "observed_current"
	return status, nil
}

func (b *Broker) ownedCurrent(c owneddata.Control, s owneddata.Scope, ownerAndWatch bool) (ownedClock, error) {
	n, reason, e := b.ownedFresh(c, s, ownerAndWatch)
	if reason != "" {
		_, _ = b.ownedDeny(c, reason)
	}
	return n, e
}

// ownedFresh never updates the journal. It is safe to use after acquisition of
// the short journal lock, so a waiting transition cannot reuse stale liveness.
func (b *Broker) ownedFresh(c owneddata.Control, s owneddata.Scope, readWatch bool) (ownedClock, string, error) {
	if s.DenyLatched || c.AdmittingOwnerEpoch != s.AdmittingOwnerEpoch || c.WriterEpoch != "" && c.WriterEpoch != s.WriterEpoch {
		return ownedClock{}, "", owneddata.ErrDenied
	}
	if s.Generation >= owneddata.MaxInteger-16 {
		return ownedClock{}, "observations_full", owneddata.ErrDenied
	}
	n, e := b.ownedNow()
	if e != nil {
		return n, "io_uncertain", e
	}
	if reason := ownedClockReason(s, n); reason != "" {
		return n, reason, owneddata.ErrDenied
	}
	x := b.owned
	if x.checkRoot() != nil || x.id != s.BoundaryID || x.rootIdentity != s.FilesystemIdentity.Root {
		return n, "owner_mismatch", owneddata.ErrUnresolved
	}
	if livePipe(x.ownerRead, s.FilesystemIdentity.OwnerRead) != "observed_current" {
		return n, "restart", owneddata.ErrDenied
	}
	if readWatch {
		if livePipe(x.watchRead, s.FilesystemIdentity.WatchRead) != "observed_current" {
			return n, "restart", owneddata.ErrDenied
		}
	} else {
		id, e := pipeIdentity(x.watchWrite, true)
		if e != nil || id != s.FilesystemIdentity.WatchWrite {
			return n, "restart", owneddata.ErrDenied
		}
	}
	return n, "", nil
}

func (b *Broker) ownedCommit(c owneddata.Control, point string, readWatch bool, previous ownedClock, fn func(*owneddata.Scope, ownedClock) error) (owneddata.Scope, error) {
	var refused error
	s, e := b.ownedChange(c, point, func(v *owneddata.Scope) error {
		n, reason, probeErr := b.ownedFresh(c, *v, readWatch)
		if probeErr == nil && (n.Epoch != previous.Epoch || n.Wall.Before(previous.Wall) || n.TickMS < previous.TickMS) {
			reason = "clock_rollback"
			probeErr = owneddata.ErrDenied
		}
		if probeErr != nil {
			refused = probeErr
			if reason != "" && !v.DenyLatched {
				v.DenyLatched = true
				v.State = "cleaning"
				v.Reason = reason
			}
			if !n.Wall.IsZero() {
				applyOwnedClock(v, n)
			}
			return nil // Persist denial in this same transaction; no recursive lock.
		}
		return fn(v, n)
	})
	if e == nil && refused != nil {
		return s, refused
	}
	return s, e
}

func (b *Broker) ownedCleanup(c owneddata.Control) (owneddata.Status, error) {
	s, authority, e := b.ownedRead(c)
	if e != nil {
		return owneddata.Status{}, e
	}
	if !s.DenyLatched {
		return owneddata.Status{}, owneddata.ErrDenied
	}
	observation := owneddata.Observation{Schema: owneddata.ObservationSchema, ScopeID: s.ScopeID, Method: "synthetic_local_scope_v1", Assurance: "synthetic", NewAdmission: "denied", WritersDrained: "unknown", ScopeAccessRevocation: "unknown", OwnedRemoval: "unknown", Reason: "lease_uncertain", ObservedAt: s.WallHighWater}
	observation.ObservationID, e = ownedID()
	if e != nil {
		return owneddata.Status{}, e
	}
	if n, e := b.ownedNow(); e == nil {
		high, _ := owneddata.Time(s.WallHighWater, true)
		if !n.Wall.Before(high) {
			observation.ObservedAt = owneddata.Stamp(n.Wall)
		}
	}
	if b.owned != nil {
		release, leaseErr := b.owned.lease(s, "writer", true)
		if leaseErr == nil {
			defer release()
			// Re-read denial after acquiring the stable exclusive lease.
			current, _, readErr := b.ownedRead(c)
			if readErr == nil && current.DenyLatched {
				if current.Reason == "io_uncertain" || current.ContentCloseUncertain || current.WriterStarted && !current.ContentCloseConfirmed {
					observation.Reason = "record_uncertain"
				} else {
					observation.WritersDrained = "observed"
					observation.ScopeAccessRevocation = "observed"
					if b.owned.removeOwned(current, authority) == nil {
						observation.OwnedRemoval = "observed_absent"
						observation.Reason = "none"
					} else {
						observation.OwnedRemoval = "unknown"
						observation.Reason = "owner_mismatch"
					}
				}
			} else {
				observation.Reason = "record_uncertain"
			}
		} else if errors.Is(leaseErr, owneddata.ErrBusy) {
			observation.WritersDrained = "failed"
			observation.Reason = "writer_active"
		}
	}
	s, e = b.ownedChange(c, "observation", func(v *owneddata.Scope) error {
		if !v.DenyLatched {
			return owneddata.ErrDenied
		}
		v.State = "cleanup_unresolved"
		if len(v.Observations) >= owneddata.MaxObservations {
			v.ObservationsSaturated = true
			return nil
		}
		observation.ScopeGeneration = v.Generation + 1
		v.Observations = append(v.Observations, observation)
		return nil
	})
	if e != nil {
		return owneddata.Status{}, owneddata.ErrUnresolved
	}
	return ownedStatus(s, authority), nil
}

func (b *Broker) OwnedReap() ([]owneddata.Status, error) {
	var controls []owneddata.Control
	e := b.Store.View(func(d ledger.Data) error {
		if d.Version != 6 {
			return owneddata.ErrDenied
		}
		for id, s := range *d.OwnedDataScopes {
			controls = append(controls, owneddata.Control{Operation: "reconcile", ScopeID: id, WaypointAuthorityID: s.WaypointAuthorityID, ExpectedBrokerAuthorityID: d.Authority.ID})
		}
		return nil
	})
	if e != nil {
		return nil, ownedError(e)
	}
	sort.Slice(controls, func(i, j int) bool { return controls[i].ScopeID < controls[j].ScopeID })
	out := []owneddata.Status{}
	for _, c := range controls {
		s, _, e := b.ownedRead(c)
		if e != nil {
			return out, e
		}
		reason := ""
		if s.DenyLatched {
			reason = s.Reason
		} else if n, e := b.ownedNow(); e != nil {
			reason = "restart"
		} else {
			reason = ownedClockReason(s, n)
		}
		if reason == "" && b.owned != nil {
			release, e := b.owned.lease(s, "watch", true)
			if e == nil {
				release()
				reason = "restart"
			} else if !errors.Is(e, owneddata.ErrBusy) {
				reason = "owner_mismatch"
			}
		}
		if reason == "" {
			continue
		}
		if _, e := b.ownedDeny(c, reason); e != nil {
			return out, e
		}
		status, e := b.ownedCleanup(c)
		if e != nil {
			return out, e
		}
		out = append(out, status)
	}
	return out, nil
}
