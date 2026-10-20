// Package owneddata defines content-free assessment ownership metadata. It has
// no storage, process, provider, admission or boundary-construction functions.
package owneddata

import (
	"encoding/hex"
	"errors"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	ControlSchema           = "outfitter.owned-data-control/v1"
	StatusSchema            = "outfitter.owned-data-status/v1"
	ErrorSchema             = "outfitter.owned-data-error/v1"
	ReadySchema             = "outfitter.owned-data-ready/v1"
	ObservationSchema       = "outfitter.owned-data-observation/v1"
	MaxControlBytes         = 128 << 10
	MaxReplyBytes           = 64 << 10
	MaxChunkBytes           = 64 << 10
	MaxObservations         = 8
	MaxInteger        int64 = 9007199254740991
	MicroUTC                = "2006-01-02T15:04:05.000000Z"
	SecondUTC               = "2006-01-02T15:04:05Z"
)

var ErrInvalid = errors.New("ASSESSMENT_INVALID")
var ErrDisabled = errors.New("ASSESSMENT_UPLOAD_DISABLED")
var ErrUnresolved = errors.New("ASSESSMENT_UNRESOLVED")
var ErrDenied = errors.New("ASSESSMENT_DENIED")
var ErrMissing = errors.New("ASSESSMENT_MISSING")
var ErrCollision = errors.New("ASSESSMENT_COLLISION")
var ErrStale = errors.New("ASSESSMENT_STALE")
var ErrBusy = errors.New("ASSESSMENT_WRITER_ACTIVE")

func OneOf(s string, values ...string) bool { return slices.Contains(values, s) }
func Uint(n int64) bool                     { return n >= 0 && n <= MaxInteger }
func Hex(s string, bytes int) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == bytes && s == strings.ToLower(s)
}
func ID(s string) bool { return Hex(s, 16) }
func Time(s string, micro bool) (time.Time, error) {
	f := SecondUTC
	if micro {
		f = MicroUTC
	}
	t, e := time.Parse(f, s)
	if e != nil || t.Format(f) != s {
		return time.Time{}, ErrInvalid
	}
	return t, nil
}
func Stamp(t time.Time) string { return t.UTC().Truncate(time.Microsecond).Format(MicroUTC) }

// FilesystemIdentity is private operational identity from an opened descriptor,
// never caller input, a content hash, a filesystem path or public status data.
type FilesystemIdentity struct {
	Device string `json:"device"`
	Inode  string `json:"inode"`
}

var decimal = regexp.MustCompile(`^(0|[1-9][0-9]{0,19})$`)

func (f FilesystemIdentity) Valid() bool {
	for _, s := range []string{f.Device, f.Inode} {
		if !decimal.MatchString(s) {
			return false
		}
		if _, e := strconv.ParseUint(s, 10, 64); e != nil {
			return false
		}
	}
	return f.Inode != "0"
}

type FilesystemBinding struct {
	Root        FilesystemIdentity  `json:"root"`
	Scope       *FilesystemIdentity `json:"scope"`
	WriterLease *FilesystemIdentity `json:"writer_lease"`
	WatchLease  *FilesystemIdentity `json:"watch_lease"`
	Content     *FilesystemIdentity `json:"content"`
	OwnerRead   FilesystemIdentity  `json:"owner_read"`
	WatchRead   FilesystemIdentity  `json:"watch_read"`
	WatchWrite  FilesystemIdentity  `json:"watch_write"`
}

type Observation struct {
	Schema                 string `json:"schema"`
	ObservationID          string `json:"observation_id"`
	ScopeID                string `json:"scope_id"`
	ScopeGeneration        int64  `json:"scope_generation"`
	ObservedAt             string `json:"observed_at"`
	Method                 string `json:"method"`
	Assurance              string `json:"assurance"`
	NewAdmission           string `json:"new_admission"`
	WritersDrained         string `json:"writers_drained"`
	ScopeAccessRevocation  string `json:"scope_access_revocation"`
	OwnedRemoval           string `json:"owned_removal"`
	Reason                 string `json:"reason"`
	CleanupTrusted         bool   `json:"cleanup_trusted"`
	ActualResourceAccepted bool   `json:"actual_resource_accepted"`
}

type Scope struct {
	ScopeID                    string            `json:"scope_id"`
	AssessmentID               string            `json:"assessment_id"`
	ProjectID                  string            `json:"project_id"`
	WaypointAuthorityID        string            `json:"waypoint_authority_id"`
	ManifestSHA256             string            `json:"manifest_sha256"`
	ManifestExpiresAt          string            `json:"manifest_expires_at"`
	BoundaryID                 string            `json:"boundary_id"`
	CreatedAt                  string            `json:"created_at"`
	ExpiresAt                  string            `json:"expires_at"`
	WallHighWater              string            `json:"wall_high_water"`
	Generation                 int64             `json:"generation"`
	State                      string            `json:"state"`
	DenyLatched                bool              `json:"deny_latched"`
	Reason                     string            `json:"reason"`
	WriterStarted              bool              `json:"writer_started"`
	WriterEpoch                string            `json:"writer_epoch"`
	WatchEpoch                 *string           `json:"watch_epoch"`
	WatchStarted               bool              `json:"watch_started"`
	UploadComplete             bool              `json:"upload_complete"`
	ContentCloseConfirmed      bool              `json:"content_close_confirmed"`
	ContentCloseUncertain      bool              `json:"content_close_uncertain"`
	Assurance                  string            `json:"assurance"`
	Observations               []Observation     `json:"observations"`
	ObservationsSaturated      bool              `json:"observations_saturated"`
	DeclaredUploadCeilingBytes int64             `json:"declared_upload_ceiling_bytes"`
	AdmittingOwnerEpoch        string            `json:"admitting_owner_epoch"`
	ClockEpoch                 string            `json:"clock_epoch"`
	DeadlineTickMS             int64             `json:"deadline_tick_ms"`
	TickHighWaterMS            int64             `json:"tick_high_water_ms"`
	FilesystemIdentity         FilesystemBinding `json:"filesystem_identity"`
}

type Status struct {
	Schema                     string        `json:"schema"`
	ScopeID                    string        `json:"scope_id"`
	AuthorityID                string        `json:"authority_id"`
	WaypointAuthorityID        string        `json:"waypoint_authority_id"`
	ProjectID                  string        `json:"project_id"`
	ManifestSHA256             string        `json:"manifest_sha256"`
	DeclaredUploadCeilingBytes int64         `json:"declared_upload_ceiling_bytes"`
	Generation                 int64         `json:"generation"`
	State                      string        `json:"state"`
	DenyLatched                bool          `json:"deny_latched"`
	ExpiresAt                  string        `json:"expires_at"`
	WriterEpoch                string        `json:"writer_epoch"`
	WatchEpoch                 *string       `json:"watch_epoch"`
	AdmittingOwnerEpoch        string        `json:"admitting_owner_epoch"`
	Code                       string        `json:"code"`
	Observations               []Observation `json:"observations"`
	TestPermission             string        `json:"test_permission"`
	Liveness                   string        `json:"liveness"`
	UploadAuthorized           bool          `json:"upload_authorized"`
	ExecutionAuthorized        bool          `json:"execution_authorized"`
	CleanupTrusted             bool          `json:"cleanup_trusted"`
	ActualResourceAccepted     bool          `json:"actual_resource_accepted"`
}

// Ready is an internal one-line, bounded readiness-pipe acknowledgement. It
// cannot be imported to establish a live owner/watch or authorize content.
type Ready struct {
	Schema                     string `json:"schema"`
	ScopeID                    string `json:"scope_id"`
	AuthorityID                string `json:"authority_id"`
	WaypointAuthorityID        string `json:"waypoint_authority_id"`
	ProjectID                  string `json:"project_id"`
	ManifestSHA256             string `json:"manifest_sha256"`
	DeclaredUploadCeilingBytes int64  `json:"declared_upload_ceiling_bytes"`
	AdmittingOwnerEpoch        string `json:"admitting_owner_epoch"`
	WriterEpoch                string `json:"writer_epoch"`
	WatchEpoch                 string `json:"watch_epoch"`
	Generation                 int64  `json:"generation"`
	ExpiresAt                  string `json:"expires_at"`
	ClockEpoch                 string `json:"clock_epoch"`
	DeadlineTickMS             int64  `json:"deadline_tick_ms"`
}

type ErrorReply struct {
	Schema string `json:"schema"`
	Code   string `json:"code"`
}

// Control is an internal checked command, not an alternative JSON decoder.
// The control codec derives manifest fields from the unchanged strict manifest.
type Control struct {
	Operation, ScopeID, WaypointAuthorityID                    string
	ExpectedBrokerAuthorityID                                  string
	ProjectID, AssessmentID, ManifestSHA256, ManifestExpiresAt string
	DeclaredUploadCeilingBytes, ManifestScratchBytes           int64
	AdmittingOwnerEpoch, ExpiresAt                             string
	ExpectedGeneration                                         int64
	WriterEpoch                                                string
	ContentLength                                              int64
	Reason                                                     string
}

var subject = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$`)

func (o Observation) Validate() error {
	_, e := Time(o.ObservedAt, true)
	if o.Schema != ObservationSchema || !ID(o.ObservationID) || !ID(o.ScopeID) || !Uint(o.ScopeGeneration) || o.ScopeGeneration == 0 || e != nil || o.Method != "synthetic_local_scope_v1" || o.Assurance != "synthetic" || o.CleanupTrusted || o.ActualResourceAccepted || !OneOf(o.NewAdmission, "denied", "unknown") || !OneOf(o.WritersDrained, "observed", "failed", "unknown") || !OneOf(o.ScopeAccessRevocation, "observed", "failed", "unknown") || !OneOf(o.OwnedRemoval, "observed_absent", "failed", "unknown") || !OneOf(o.Reason, "none", "writer_active", "owner_mismatch", "io_uncertain", "lease_uncertain", "record_uncertain", "clock_uncertain") {
		return ErrInvalid
	}
	if o.OwnedRemoval == "observed_absent" && (o.NewAdmission != "denied" || o.WritersDrained != "observed" || o.ScopeAccessRevocation != "observed") {
		return ErrInvalid
	}
	return nil
}
func (s Scope) Validate() error {
	created, e1 := Time(s.CreatedAt, true)
	expires, e2 := Time(s.ExpiresAt, false)
	high, e3 := Time(s.WallHighWater, true)
	manifestExpiry, e4 := Time(s.ManifestExpiresAt, false)
	if !ID(s.ScopeID) || !subject.MatchString(s.AssessmentID) || !ID(s.ProjectID) || !ID(s.WaypointAuthorityID) || !Hex(s.ManifestSHA256, 32) || !ID(s.BoundaryID) || !ID(s.WriterEpoch) || !ID(s.AdmittingOwnerEpoch) || !ID(s.ClockEpoch) || e1 != nil || e2 != nil || e3 != nil || e4 != nil || !expires.After(created) || expires.After(manifestExpiry) || high.Before(created) || !Uint(s.Generation) || s.Generation == 0 || !Uint(s.DeclaredUploadCeilingBytes) || s.DeclaredUploadCeilingBytes == 0 || !Uint(s.DeadlineTickMS) || !Uint(s.TickHighWaterMS) || s.Assurance != "synthetic" || s.Observations == nil || len(s.Observations) > MaxObservations || !s.FilesystemIdentity.Root.Valid() {
		return ErrInvalid
	}
	if !OneOf(s.State, "preparing", "accepting", "sealed", "cleaning", "cleanup_unresolved") || !OneOf(s.Reason, "none", "cancelled", "access_ended", "expired", "restart", "clock_rollback", "incomplete_upload", "io_uncertain", "owner_mismatch", "observations_full") {
		return ErrInvalid
	}
	denied := OneOf(s.State, "cleaning", "cleanup_unresolved")
	if denied != s.DenyLatched || (s.Reason != "none") != denied || s.UploadComplete && !s.WriterStarted || s.State == "sealed" && !s.UploadComplete || s.WatchStarted != (s.WatchEpoch != nil) || s.WatchEpoch != nil && !ID(*s.WatchEpoch) || s.ObservationsSaturated && !denied {
		return ErrInvalid
	}
	for _, f := range []*FilesystemIdentity{s.FilesystemIdentity.Scope, s.FilesystemIdentity.WriterLease, s.FilesystemIdentity.WatchLease, s.FilesystemIdentity.Content} {
		if f != nil && !f.Valid() {
			return ErrInvalid
		}
	}
	if !s.FilesystemIdentity.OwnerRead.Valid() || !s.FilesystemIdentity.WatchRead.Valid() || !s.FilesystemIdentity.WatchWrite.Valid() || s.UploadComplete && s.FilesystemIdentity.Content == nil {
		return ErrInvalid
	}
	if OneOf(s.State, "accepting", "sealed") && (!s.WatchStarted || s.FilesystemIdentity.Scope == nil || s.FilesystemIdentity.WriterLease == nil || s.FilesystemIdentity.WatchLease == nil) {
		return ErrInvalid
	}
	if s.WriterStarted && !s.WatchStarted {
		return ErrInvalid
	}
	if (s.ContentCloseConfirmed || s.ContentCloseUncertain) && !s.WriterStarted || s.UploadComplete && !s.ContentCloseConfirmed || s.ContentCloseUncertain && !s.DenyLatched {
		return ErrInvalid
	}
	ids := map[string]bool{}
	for _, o := range s.Observations {
		at, _ := Time(o.ObservedAt, true)
		if o.Validate() != nil || o.ScopeID != s.ScopeID || o.ScopeGeneration > s.Generation || ids[o.ObservationID] || at.Before(created) || !denied {
			return ErrInvalid
		}
		ids[o.ObservationID] = true
	}
	return nil
}
