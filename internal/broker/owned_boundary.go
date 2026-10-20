package broker

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
)

type ownedClock struct {
	Wall   time.Time
	TickMS int64
	Epoch  string
}
type ownedPolicy struct {
	MaxUploadBytes                                int64
	ProjectEnd, AuthorizationExpiry, PolicyExpiry time.Time
}

// There is intentionally no production constructor or exported boundary
// factory. Only broker _test.go code constructs this synthetic seam. A later
// selected-host implementation requires separate source review/qualification.
type ownedBoundary struct {
	root, stateRoot                              *os.Root
	id                                           string
	rootIdentity                                 owneddata.FilesystemIdentity
	clock                                        func() (ownedClock, error)
	policy                                       func(projectID, ownerEpoch string) (ownedPolicy, error)
	ownerRead, watchRead, watchWrite, readyWrite *os.File
	spawnWatch                                   func(scope owneddata.Scope, ownerRead, watchWrite *os.File) (io.ReadCloser, error)
	fault                                        func(string) error
}

func (x *ownedBoundary) fail(point string) error {
	if x.fault != nil && x.fault(point) != nil {
		return owneddata.ErrUnresolved
	}
	return nil
}

func (x *ownedBoundary) closeContent(f *os.File, phase string) error {
	// The negative test seam returns before Close, retaining a genuinely open
	// descriptor. It does not mistake a post-close injected error for failure.
	if x.fail("content-close:"+phase) != nil {
		return owneddata.ErrUnresolved
	}
	return f.Close()
}
func openedIdentity(f *os.File, kind string) (owneddata.FilesystemIdentity, error) {
	if f == nil {
		return owneddata.FilesystemIdentity{}, owneddata.ErrUnresolved
	}
	st, e := f.Stat()
	if e != nil {
		return owneddata.FilesystemIdentity{}, owneddata.ErrUnresolved
	}
	if kind == "directory" && !st.IsDir() || kind == "file" && (!st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0) || kind == "pipe" && st.Mode()&os.ModeNamedPipe == 0 {
		return owneddata.FilesystemIdentity{}, owneddata.ErrUnresolved
	}
	if raw, ok := st.Sys().(*syscall.Stat_t); !ok || kind == "file" && raw.Nlink != 1 || kind == "pipe" && raw.Nlink != 0 {
		return owneddata.FilesystemIdentity{}, owneddata.ErrUnresolved
	}
	return ledger.FileIdentity(st)
}
func pipeIdentity(f *os.File, write bool) (owneddata.FilesystemIdentity, error) {
	id, e := openedIdentity(f, "pipe")
	if e != nil {
		return id, e
	}
	raw, e := f.SyscallConn()
	if e != nil {
		return id, owneddata.ErrUnresolved
	}
	valid := false
	e = raw.Control(func(fd uintptr) {
		flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFL, 0)
		want := uintptr(syscall.O_RDONLY)
		if write {
			want = syscall.O_WRONLY
		}
		valid = errno == 0 && flags&syscall.O_ACCMODE == want
		syscall.CloseOnExec(int(fd))
	})
	if e != nil || !valid {
		return id, owneddata.ErrUnresolved
	}
	return id, nil
}
func livePipe(f *os.File, expected owneddata.FilesystemIdentity) string {
	id, e := pipeIdentity(f, false)
	if e != nil || id != expected {
		return "unknown"
	}
	raw, e := f.SyscallConn()
	if e != nil {
		return "unknown"
	}
	result := "unknown"
	e = raw.Control(func(fd uintptr) {
		if syscall.SetNonblock(int(fd), true) != nil {
			return
		}
		var one [1]byte
		n, e := syscall.Read(int(fd), one[:])
		switch {
		case e == syscall.EAGAIN || e == syscall.EWOULDBLOCK:
			result = "observed_current"
		case e == nil && n == 0:
			result = "lost"
		}
	})
	if e != nil {
		return "unknown"
	}
	return result
}

// A bounded random bootstrap exchange proves this read endpoint actually
// receives this write endpoint. Endpoint inode equality is not portable.
func pairedWatchPipe(read, write *os.File) error {
	if _, e := pipeIdentity(read, false); e != nil {
		return e
	}
	if _, e := pipeIdentity(write, true); e != nil {
		return e
	}
	var challenge [32]byte
	if _, e := rand.Read(challenge[:]); e != nil {
		return owneddata.ErrUnresolved
	}
	if e := write.SetWriteDeadline(time.Now().Add(500 * time.Millisecond)); e != nil {
		return owneddata.ErrUnresolved
	}
	n, e := write.Write(challenge[:])
	_ = write.SetWriteDeadline(time.Time{})
	if e != nil || n != len(challenge) {
		return owneddata.ErrUnresolved
	}
	if e := read.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); e != nil {
		return owneddata.ErrUnresolved
	}
	var got [32]byte
	_, e = io.ReadFull(read, got[:])
	_ = read.SetReadDeadline(time.Time{})
	if e != nil || got != challenge {
		return owneddata.ErrUnresolved
	}
	return nil
}

func (x *ownedBoundary) checkRoot() error {
	if x == nil || x.root == nil || x.stateRoot == nil || !owneddata.ID(x.id) || !x.rootIdentity.Valid() {
		return owneddata.ErrDisabled
	}
	f, e := x.root.Open(".")
	if e != nil {
		return owneddata.ErrUnresolved
	}
	defer f.Close()
	id, e := openedIdentity(f, "directory")
	if e != nil || id != x.rootIdentity {
		return owneddata.ErrUnresolved
	}
	st, e := os.Lstat(x.root.Name())
	if e != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm()&0077 != 0 {
		return owneddata.ErrUnresolved
	}
	pathID, e := ledger.FileIdentity(st)
	if e != nil || pathID != id {
		return owneddata.ErrUnresolved
	}
	return nil
}
func sameRootEntry(root *os.Root, name string, id owneddata.FilesystemIdentity, directory bool) error {
	st, e := root.Lstat(name)
	if e != nil || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm()&0077 != 0 || directory && !st.IsDir() || !directory && !st.Mode().IsRegular() {
		return owneddata.ErrUnresolved
	}
	actual, e := ledger.FileIdentity(st)
	if e != nil || actual != id {
		return owneddata.ErrUnresolved
	}
	return nil
}
func syncRoot(root *os.Root) error {
	f, e := root.Open(".")
	if e != nil {
		return owneddata.ErrUnresolved
	}
	defer f.Close()
	if f.Sync() != nil {
		return owneddata.ErrUnresolved
	}
	return nil
}
func leaseName(id, kind string) string { return "owned-" + id + "-" + kind + ".lock" }

func (x *ownedBoundary) createLease(id, kind string) (*owneddata.FilesystemIdentity, error) {
	f, e := x.stateRoot.OpenFile(leaseName(id, kind), os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if e != nil {
		return nil, owneddata.ErrUnresolved
	}
	defer f.Close()
	identity, e := openedIdentity(f, "file")
	if e != nil || f.Sync() != nil || syncRoot(x.stateRoot) != nil {
		return nil, owneddata.ErrUnresolved
	}
	return &identity, nil
}
func (x *ownedBoundary) lease(s owneddata.Scope, kind string, exclusive bool) (func(), error) {
	if x.checkRoot() != nil || x.checkLeases(s) != nil {
		return nil, owneddata.ErrUnresolved
	}
	expected := s.FilesystemIdentity.WriterLease
	if kind == "watch" {
		expected = s.FilesystemIdentity.WatchLease
	}
	if expected == nil {
		return nil, owneddata.ErrUnresolved
	}
	name := leaseName(s.ScopeID, kind)
	f, e := x.stateRoot.OpenFile(name, os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if e != nil {
		return nil, owneddata.ErrUnresolved
	}
	check := func() error {
		id, e := openedIdentity(f, "file")
		if e != nil || id != *expected || sameRootEntry(x.stateRoot, name, *expected, false) != nil || x.checkRoot() != nil || x.checkLeases(s) != nil {
			return owneddata.ErrUnresolved
		}
		return nil
	}
	if check() != nil {
		f.Close()
		return nil, owneddata.ErrUnresolved
	}
	mode := syscall.LOCK_SH
	if exclusive {
		mode = syscall.LOCK_EX
	}
	if e := syscall.Flock(int(f.Fd()), mode|syscall.LOCK_NB); e != nil {
		f.Close()
		if e == syscall.EWOULDBLOCK || e == syscall.EAGAIN {
			return nil, owneddata.ErrBusy
		}
		return nil, owneddata.ErrUnresolved
	}
	release := func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }
	if x.fail("after-lease-acquire") != nil || check() != nil {
		release()
		return nil, owneddata.ErrUnresolved
	}
	return release, nil
}

// Observe both stable lease objects without acquiring a conflicting watch
// flock. The real watcher may already own that lock; identity is a separate
// prerequisite from whichever exclusion this operation acquires.
func (x *ownedBoundary) checkLeases(s owneddata.Scope) error {
	for _, binding := range []struct {
		kind     string
		expected *owneddata.FilesystemIdentity
	}{{"writer", s.FilesystemIdentity.WriterLease}, {"watch", s.FilesystemIdentity.WatchLease}} {
		if binding.expected == nil {
			return owneddata.ErrUnresolved
		}
		name := leaseName(s.ScopeID, binding.kind)
		f, e := x.stateRoot.OpenFile(name, os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if e != nil {
			return owneddata.ErrUnresolved
		}
		id, e := openedIdentity(f, "file")
		ce := f.Close()
		if e != nil || ce != nil || id != *binding.expected || sameRootEntry(x.stateRoot, name, *binding.expected, false) != nil {
			return owneddata.ErrUnresolved
		}
	}
	return nil
}
func markerBytes(s owneddata.Scope, authority string) []byte {
	b, _ := json.Marshal(struct{ ScopeID, AuthorityID, WaypointAuthorityID, BoundaryID, WriterEpoch string }{s.ScopeID, authority, s.WaypointAuthorityID, s.BoundaryID, s.WriterEpoch})
	return b
}
func (x *ownedBoundary) createScope(s owneddata.Scope, authority string) (owneddata.FilesystemBinding, error) {
	identity := s.FilesystemIdentity
	if x.checkRoot() != nil || x.fail("before-scope-create") != nil {
		return identity, owneddata.ErrUnresolved
	}
	var e error
	identity.WriterLease, e = x.createLease(s.ScopeID, "writer")
	if e != nil {
		return identity, e
	}
	identity.WatchLease, e = x.createLease(s.ScopeID, "watch")
	if e != nil {
		return identity, e
	}
	if e = x.root.Mkdir(s.ScopeID, 0700); e != nil {
		return identity, owneddata.ErrUnresolved
	}
	scope, e := x.root.OpenRoot(s.ScopeID)
	if e != nil {
		return identity, owneddata.ErrUnresolved
	}
	defer scope.Close()
	f, e := scope.Open(".")
	if e != nil {
		return identity, owneddata.ErrUnresolved
	}
	id, e := openedIdentity(f, "directory")
	f.Close()
	if e != nil {
		return identity, e
	}
	identity.Scope = &id
	marker, e := scope.OpenFile("scope-owner.json", os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if e != nil {
		return identity, owneddata.ErrUnresolved
	}
	b := markerBytes(s, authority)
	n, e := marker.Write(b)
	se := marker.Sync()
	ce := marker.Close()
	if e != nil || se != nil || ce != nil || n != len(b) || syncRoot(scope) != nil || syncRoot(x.root) != nil || x.fail("after-scope-create") != nil {
		return identity, owneddata.ErrUnresolved
	}
	return identity, nil
}

func (x *ownedBoundary) openScope(s owneddata.Scope, authority string) (*os.Root, error) {
	if x.checkRoot() != nil || s.BoundaryID != x.id || s.FilesystemIdentity.Root != x.rootIdentity || s.FilesystemIdentity.Scope == nil || sameRootEntry(x.root, s.ScopeID, *s.FilesystemIdentity.Scope, true) != nil {
		return nil, owneddata.ErrUnresolved
	}
	r, e := x.root.OpenRoot(s.ScopeID)
	if e != nil {
		return nil, owneddata.ErrUnresolved
	}
	f, e := r.Open(".")
	if e != nil {
		r.Close()
		return nil, owneddata.ErrUnresolved
	}
	id, e := openedIdentity(f, "directory")
	f.Close()
	if e != nil || id != *s.FilesystemIdentity.Scope || sameRootEntry(x.root, s.ScopeID, id, true) != nil {
		r.Close()
		return nil, owneddata.ErrUnresolved
	}
	marker, e := r.OpenFile("scope-owner.json", os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if e != nil {
		r.Close()
		return nil, owneddata.ErrUnresolved
	}
	_, ie := openedIdentity(marker, "file")
	b, re := io.ReadAll(io.LimitReader(marker, 1025))
	marker.Close()
	if ie != nil || re != nil || !bytes.Equal(b, markerBytes(s, authority)) {
		r.Close()
		return nil, owneddata.ErrUnresolved
	}
	return r, nil
}
func (x *ownedBoundary) openContent(s owneddata.Scope, authority string) (*os.File, owneddata.FilesystemIdentity, error) {
	var empty owneddata.FilesystemIdentity
	r, e := x.openScope(s, authority)
	if e != nil {
		return nil, empty, e
	}
	defer r.Close()
	f, e := r.OpenFile("payload", os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if e != nil {
		return nil, empty, owneddata.ErrUnresolved
	}
	id, e := openedIdentity(f, "file")
	if e != nil || syncRoot(r) != nil || x.fail("after-content-create") != nil {
		f.Close()
		return nil, empty, owneddata.ErrUnresolved
	}
	return f, id, nil
}
func (x *ownedBoundary) removeOwned(s owneddata.Scope, authority string) error {
	if x.checkRoot() != nil || x.id != s.BoundaryID || x.rootIdentity != s.FilesystemIdentity.Root || x.checkLeases(s) != nil {
		return owneddata.ErrUnresolved
	}
	if _, e := x.root.Lstat(s.ScopeID); os.IsNotExist(e) && s.FilesystemIdentity.Scope != nil {
		return x.checkRoot()
	}
	r, e := x.openScope(s, authority)
	if e != nil {
		return e
	}
	defer r.Close()
	dir, e := r.Open(".")
	if e != nil {
		return owneddata.ErrUnresolved
	}
	entries, e := dir.ReadDir(3)
	dir.Close()
	if e != nil && e != io.EOF {
		return owneddata.ErrUnresolved
	}
	for _, entry := range entries {
		if !owneddata.OneOf(entry.Name(), "payload", "scope-owner.json") {
			return owneddata.ErrUnresolved
		}
	}
	if x.fail("before-owned-removal") != nil {
		return owneddata.ErrUnresolved
	}
	if x.checkLeases(s) != nil {
		return owneddata.ErrUnresolved
	}
	check, e := x.openScope(s, authority)
	if e != nil {
		return e
	}
	check.Close()
	if _, e := r.Lstat("payload"); e == nil {
		if s.FilesystemIdentity.Content == nil {
			return owneddata.ErrUnresolved
		}
		f, e := r.OpenFile("payload", os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if e != nil {
			return owneddata.ErrUnresolved
		}
		id, e := openedIdentity(f, "file")
		f.Close()
		if e != nil || id != *s.FilesystemIdentity.Content || sameRootEntry(r, "payload", id, false) != nil {
			return owneddata.ErrUnresolved
		}
		if r.Remove("payload") != nil {
			return owneddata.ErrUnresolved
		}
	} else if !os.IsNotExist(e) {
		return owneddata.ErrUnresolved
	}
	if syncRoot(r) != nil || sameRootEntry(x.root, s.ScopeID, *s.FilesystemIdentity.Scope, true) != nil {
		return owneddata.ErrUnresolved
	}
	if r.Remove("scope-owner.json") != nil || syncRoot(r) != nil || x.root.Remove(s.ScopeID) != nil || syncRoot(x.root) != nil || x.checkRoot() != nil || x.checkLeases(s) != nil || x.fail("after-owned-removal") != nil {
		return owneddata.ErrUnresolved
	}
	return nil
}

// Current state-root identity is checked separately by the existing Store's
// authority/lease pins. No path here originates in control JSON or upload data.
func (x *ownedBoundary) checkState(path string, expected owneddata.FilesystemIdentity) error {
	if x == nil || x.stateRoot == nil || filepath.Clean(x.stateRoot.Name()) != path {
		return owneddata.ErrUnresolved
	}
	f, e := x.stateRoot.Open(".")
	if e != nil {
		return owneddata.ErrUnresolved
	}
	defer f.Close()
	id, e := openedIdentity(f, "directory")
	if e != nil || id != expected {
		return owneddata.ErrUnresolved
	}
	return sameRootEntry(x.stateRoot, ".", expected, true)
}
