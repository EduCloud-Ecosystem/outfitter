package ledger

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"syscall"

	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

var ErrOwnedDataOnly = errors.New("ASSESSMENT_ONLY_AUTHORITY")

// ReadOnlyVersion probes an existing journal without locks, initialization,
// migration, or writes. The ordinary Open operation retains its legacy behavior.
func ReadOnlyVersion(dir string) (int, error) {
	p, e := filepath.Abs(dir)
	if e != nil {
		return 0, e
	}
	p, e = filepath.EvalSymlinks(p)
	if e != nil {
		return 0, e
	}
	d, e := (&Store{Dir: p}).read()
	if e != nil {
		return 0, e
	}
	return d.Version, nil
}

// OwnedDataIdentity is internal identity of the fresh test-only authority. It
// never grants upload permission and is not accepted by any control operation.
type OwnedDataIdentity struct {
	WaypointAuthorityID string                       `json:"waypoint_authority_id"`
	Directory           owneddata.FilesystemIdentity `json:"directory"`
	JournalLease        owneddata.FilesystemIdentity `json:"journal_lease"`
	ExecutionLease      owneddata.FilesystemIdentity `json:"execution_lease"`
}

func FileIdentity(st os.FileInfo) (owneddata.FilesystemIdentity, error) {
	x, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return owneddata.FilesystemIdentity{}, owneddata.ErrUnresolved
	}
	f := owneddata.FilesystemIdentity{Device: strconv.FormatUint(uint64(x.Dev), 10), Inode: strconv.FormatUint(uint64(x.Ino), 10)}
	if !f.Valid() {
		return owneddata.FilesystemIdentity{}, owneddata.ErrUnresolved
	}
	return f, nil
}

func samePrivateObject(path string, expected owneddata.FilesystemIdentity, directory bool) error {
	st, e := os.Lstat(path)
	if e != nil || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm()&0077 != 0 || directory && !st.IsDir() || !directory && !st.Mode().IsRegular() {
		return owneddata.ErrUnresolved
	}
	id, e := FileIdentity(st)
	if e != nil || id != expected {
		return owneddata.ErrUnresolved
	}
	return nil
}
func (s *Store) checkOwnedIdentity(i *OwnedDataIdentity) error {
	if i == nil || s.ownedIdentity != nil && *s.ownedIdentity != *i {
		return owneddata.ErrUnresolved
	}
	for _, x := range []struct {
		path string
		id   owneddata.FilesystemIdentity
		dir  bool
	}{{s.Dir, i.Directory, true}, {filepath.Join(s.Dir, "ledger.lock"), i.JournalLease, false}, {filepath.Join(s.Dir, "execution.lock"), i.ExecutionLease, false}} {
		if samePrivateObject(x.path, x.id, x.dir) != nil {
			return owneddata.ErrUnresolved
		}
	}
	return nil
}

func (s *Store) ownedLease(name string, expected owneddata.FilesystemIdentity, mode int) (func(), error) {
	if s.checkOwnedIdentity(s.ownedIdentity) != nil {
		return nil, owneddata.ErrUnresolved
	}
	path := filepath.Join(s.Dir, name)
	fd, e := syscall.Open(path, syscall.O_RDWR|syscall.O_NOFOLLOW|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0)
	if e != nil {
		return nil, owneddata.ErrUnresolved
	}
	f := os.NewFile(uintptr(fd), name)
	check := func() error {
		st, e := f.Stat()
		if e != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
			return owneddata.ErrUnresolved
		}
		id, e := FileIdentity(st)
		if e != nil || id != expected {
			return owneddata.ErrUnresolved
		}
		if s.checkOwnedIdentity(s.ownedIdentity) != nil || samePrivateObject(path, expected, false) != nil {
			return owneddata.ErrUnresolved
		}
		return nil
	}
	if check() != nil {
		f.Close()
		return nil, owneddata.ErrUnresolved
	}
	if e := syscall.Flock(fd, mode); e != nil {
		f.Close()
		return nil, owneddata.ErrUnresolved
	}
	release := func() { _ = syscall.Flock(fd, syscall.LOCK_UN); _ = f.Close() }
	if check() != nil {
		release()
		return nil, owneddata.ErrUnresolved
	}
	return release, nil
}
func (s *Store) ownedLock(fn func() error) error {
	release, e := s.ownedLease("ledger.lock", s.ownedIdentity.JournalLease, syscall.LOCK_EX)
	if e != nil {
		return e
	}
	defer release()
	return fn()
}

func validateOwnedData(d Data) error {
	if d.Version != 6 {
		if d.OwnedDataScopes != nil || d.OwnedDataIdentity != nil {
			return owneddata.ErrInvalid
		}
		return nil
	}
	i := d.OwnedDataIdentity
	if i == nil || d.OwnedDataScopes == nil || *d.OwnedDataScopes == nil || !owneddata.ID(i.WaypointAuthorityID) || !i.Directory.Valid() || !i.JournalLease.Valid() || !i.ExecutionLease.Valid() || d.Authority.Restore != nil || d.Authority.Fence != nil || d.Authority.Admission != "active" {
		return owneddata.ErrInvalid
	}
	for id, s := range *d.OwnedDataScopes {
		if id != s.ScopeID || s.Validate() != nil || s.WaypointAuthorityID != i.WaypointAuthorityID {
			return owneddata.ErrInvalid
		}
		if _, ok := d.Projects[s.ProjectID]; !ok {
			return owneddata.ErrInvalid
		}
		if _, ok := d.Runs[id]; ok {
			return owneddata.ErrInvalid
		}
		if _, ok := d.NativeRuns[id]; ok {
			return owneddata.ErrInvalid
		}
	}
	// Frozen preexisting diagnostic references may remain, but owned-data writes
	// must never create or rewrite a diagnostic sidecar from legacy strings.
	for _, r := range d.Runs {
		if r.Stdout != "" || r.Stderr != "" {
			return owneddata.ErrInvalid
		}
	}
	return nil
}

func ownedCount(d Data) int {
	if d.OwnedDataScopes == nil {
		return 0
	}
	return len(*d.OwnedDataScopes)
}

type ownedGuard struct {
	version    int
	accounting []byte
	scopes     map[string]owneddata.Scope
}

func frozenOwnedAccounting(d Data) []byte {
	d.OwnedDataScopes = nil
	b, _ := json.Marshal(d)
	return b
}
func captureOwnedGuard(d Data) ownedGuard {
	g := ownedGuard{version: d.Version}
	if d.Version == 6 {
		g.accounting = frozenOwnedAccounting(d)
		b, _ := json.Marshal(d.OwnedDataScopes)
		_ = json.Unmarshal(b, &g.scopes)
	}
	return g
}
func preserveOwnedGuard(g ownedGuard, d Data) error {
	if g.version != 6 {
		if d.Version == 6 {
			return ErrOwnedDataOnly
		}
		return nil
	}
	if d.Version != 6 || !bytes.Equal(g.accounting, frozenOwnedAccounting(d)) || d.OwnedDataScopes == nil {
		return ErrOwnedDataOnly
	}
	for id, old := range g.scopes {
		next, ok := (*d.OwnedDataScopes)[id]
		if !ok {
			return owneddata.ErrInvalid
		}
		if reflect.DeepEqual(old, next) {
			continue
		}
		if next.Generation != old.Generation+1 || next.Generation > owneddata.MaxInteger || old.DenyLatched && !next.DenyLatched || old.WriterStarted && !next.WriterStarted || old.WatchStarted && !next.WatchStarted || old.UploadComplete && !next.UploadComplete || old.ObservationsSaturated && !next.ObservationsSaturated || len(next.Observations) < len(old.Observations) {
			return owneddata.ErrInvalid
		}
		if old.ContentCloseConfirmed && !next.ContentCloseConfirmed || old.ContentCloseUncertain && !next.ContentCloseUncertain {
			return owneddata.ErrInvalid
		}
		for j, o := range old.Observations {
			if !reflect.DeepEqual(o, next.Observations[j]) {
				return owneddata.ErrInvalid
			}
		}
		if old.WatchEpoch != nil && (next.WatchEpoch == nil || *old.WatchEpoch != *next.WatchEpoch) {
			return owneddata.ErrInvalid
		}
		for _, pair := range [][2]*owneddata.FilesystemIdentity{{old.FilesystemIdentity.Scope, next.FilesystemIdentity.Scope}, {old.FilesystemIdentity.WriterLease, next.FilesystemIdentity.WriterLease}, {old.FilesystemIdentity.WatchLease, next.FilesystemIdentity.WatchLease}, {old.FilesystemIdentity.Content, next.FilesystemIdentity.Content}} {
			if pair[0] != nil && (pair[1] == nil || *pair[0] != *pair[1]) {
				return owneddata.ErrInvalid
			}
		}
		if old.State == "sealed" && !owneddata.OneOf(next.State, "sealed", "cleaning", "cleanup_unresolved") || old.DenyLatched && !owneddata.OneOf(next.State, "cleaning", "cleanup_unresolved") {
			return owneddata.ErrInvalid
		}
		oldHigh, _ := owneddata.Time(old.WallHighWater, true)
		newHigh, _ := owneddata.Time(next.WallHighWater, true)
		if newHigh.Before(oldHigh) || next.TickHighWaterMS < old.TickHighWaterMS {
			return owneddata.ErrInvalid
		}
		// Erase only explicitly mutable lifecycle fields before exact comparison.
		a, b := old, next
		for _, s := range []*owneddata.Scope{&a, &b} {
			s.WallHighWater = ""
			s.TickHighWaterMS = 0
			s.Generation = 0
			s.State = ""
			s.DenyLatched = false
			s.Reason = ""
			s.WriterStarted = false
			s.WatchStarted = false
			s.WatchEpoch = nil
			s.UploadComplete = false
			s.ContentCloseConfirmed = false
			s.ContentCloseUncertain = false
			s.Observations = nil
			s.ObservationsSaturated = false
			s.FilesystemIdentity.Scope = nil
			s.FilesystemIdentity.WriterLease = nil
			s.FilesystemIdentity.WatchLease = nil
			s.FilesystemIdentity.Content = nil
		}
		if !reflect.DeepEqual(a, b) {
			return owneddata.ErrInvalid
		}
	}
	return nil
}

// ownedMaximumForm reserves metadata for denial, every ownership binding and
// all eight observations before any scope effects. The plan is never persisted.
func ownedMaximumForm(s owneddata.Scope) owneddata.Scope {
	s.Generation = owneddata.MaxInteger
	s.State = "cleanup_unresolved"
	s.DenyLatched = true
	s.Reason = "observations_full"
	// false encodes one byte longer than true. These three latches may remain
	// false in a valid denied scope with all identities and eight observations.
	s.WriterStarted = false
	s.WatchStarted = true
	s.UploadComplete = false
	s.ContentCloseConfirmed = false
	s.ContentCloseUncertain = false
	s.ObservationsSaturated = false
	epoch := "ffffffffffffffffffffffffffffffff"
	s.WatchEpoch = &epoch
	identity := owneddata.FilesystemIdentity{Device: "18446744073709551615", Inode: "18446744073709551615"}
	s.FilesystemIdentity.Scope = &identity
	s.FilesystemIdentity.WriterLease = &identity
	s.FilesystemIdentity.WatchLease = &identity
	s.FilesystemIdentity.Content = &identity
	s.TickHighWaterMS = owneddata.MaxInteger
	s.Observations = make([]owneddata.Observation, owneddata.MaxObservations)
	for j := range s.Observations {
		s.Observations[j] = owneddata.Observation{Schema: owneddata.ObservationSchema, ObservationID: epoch[:31] + strconv.Itoa(j), ScopeID: s.ScopeID, ScopeGeneration: owneddata.MaxInteger, ObservedAt: s.CreatedAt, Method: "synthetic_local_scope_v1", Assurance: "synthetic", NewAdmission: "unknown", WritersDrained: "observed", ScopeAccessRevocation: "observed", OwnedRemoval: "observed_absent", Reason: "record_uncertain"}
		s.Observations[j].NewAdmission = "denied"
	}
	return s
}

func ownedMaximumData(d Data) Data {
	if d.Version != 6 {
		return d
	}
	scopes := make(map[string]owneddata.Scope, len(*d.OwnedDataScopes))
	for id, s := range *d.OwnedDataScopes {
		scopes[id] = ownedMaximumForm(s)
	}
	d.OwnedDataScopes = &scopes
	return d
}
