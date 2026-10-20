package ledger

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"syscall"
	"time"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
)

// Restricted data handle: no Fd/Root/raw descriptor adoption API. Close routes
// through the same shared registry and never retries a failed close.
type AssessmentFile struct {
	state  *assessmentOperationState
	handle *assessmentHandle
}

func (f *AssessmentFile) Read(p []byte) (int, error)         { return f.handle.file.Read(p) }
func (f *AssessmentFile) Write(p []byte) (int, error)        { return f.handle.file.Write(p) }
func (f *AssessmentFile) Seek(n int64, w int) (int64, error) { return f.handle.file.Seek(n, w) }
func (f *AssessmentFile) Sync() error                        { return f.handle.file.Sync() }
func (f *AssessmentFile) Close() error                       { return f.CloseWithPoint("") }
func (f *AssessmentFile) CloseWithPoint(point string) error {
	v := f.state
	v.mu.Lock()
	defer v.mu.Unlock()
	h := f.handle
	if h.attempted {
		return h.result
	}
	if point != "" && v.fail(point) != nil {
		h.attempted = true
		h.result = x.ErrHeld
		v.uncertain = true
		return h.result
	}
	return v.release(h)
}
func assessmentMarker(s x.ExecutionScope) []byte {
	b, _ := json.Marshal(struct{ ScopeID, AuthorityID, WaypointAuthorityID, BoundaryID, WriterEpoch string }{s.ScopeID, s.BrokerAuthorityID, s.WaypointAuthorityID, s.BoundaryID, s.WriterEpoch})
	return b
}
func (v *assessmentOperationState) identity(f *os.File, expected *x.FilesystemIdentity, directory bool) (x.FilesystemIdentity, error) {
	st, e := f.Stat()
	if e != nil {
		return x.FilesystemIdentity{}, e
	}
	id, e := FileIdentity(st)
	raw, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !directory && raw.Nlink != 1 {
		return id, x.ErrHeld
	}
	if e != nil || st.Mode().Perm()&0077 != 0 || directory && !st.IsDir() || !directory && !st.Mode().IsRegular() || expected != nil && id != *expected {
		return id, x.ErrHeld
	}
	return id, nil
}
func (v *assessmentOperationState) syncRoot(root *os.Root) error {
	f, e := root.Open(".")
	h, e := v.file(f, e)
	if e != nil {
		return e
	}
	se := f.Sync()
	ce := v.release(h)
	if se != nil || ce != nil {
		return x.ErrHeld
	}
	return nil
}
func (v *assessmentOperationState) openScope() (*os.Root, error) {
	if v.roots.Data == nil || v.scope.FilesystemIdentity.Scope == nil {
		return nil, x.ErrHeld
	}
	r, e := v.roots.Data.OpenRoot(v.scope.ScopeID)
	_, e = v.root(r, e)
	if e != nil {
		return nil, e
	}
	f, e := r.Open(".")
	h, e := v.file(f, e)
	if e != nil {
		return nil, e
	}
	_, ie := v.identity(f, v.scope.FilesystemIdentity.Scope, true)
	ce := v.release(h)
	if ie != nil || ce != nil {
		return nil, x.ErrHeld
	}
	f, e = r.OpenFile("scope-owner.json", os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	h, e = v.file(f, e)
	if e != nil {
		return nil, e
	}
	_, markerIdentityErr := v.identity(f, nil, false)
	if markerIdentityErr != nil {
		_ = v.release(h)
		return nil, x.ErrHeld
	}
	b, re := io.ReadAll(io.LimitReader(f, 1025))
	ce = v.release(h)
	if re != nil || ce != nil || !bytes.Equal(b, assessmentMarker(v.scope)) {
		return nil, x.ErrHeld
	}
	return r, nil
}

// CreateSetup performs the fixed setup file/root constructors. Known partial
// identities are returned; descriptor ownership remains registered on error.
func (o *AssessmentOperation) CreateSetup() (x.ExecutionFilesystemBinding, error) {
	v, e := o.valid()
	if e != nil {
		return x.ExecutionFilesystemBinding{}, e
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	fs := v.scope.FilesystemIdentity
	if v.role != "prepare" || v.acquisitionStarted || v.closeAttempted || v.roots.State == nil || v.roots.Data == nil {
		return fs, x.ErrHeld
	}
	v.acquisitionStarted = true
	for _, kind := range []string{"writer", "watch", "assessment-execution"} {
		name := "owned-" + v.scope.ScopeID + "-" + kind + ".lock"
		f, e := v.roots.State.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
		h, e := v.file(f, e)
		if e != nil {
			return fs, e
		}
		id, ie := v.identity(f, nil, false)
		se := f.Sync()
		ce := v.release(h)
		if ie == nil {
			switch kind {
			case "writer":
				fs.WriterLease = &id
			case "watch":
				fs.WatchLease = &id
			default:
				fs.ExecutionLease = &id
			}
		}
		if ie != nil || se != nil || ce != nil {
			return fs, x.ErrHeld
		}
	}
	if v.syncRoot(v.roots.State) != nil || v.roots.Data.Mkdir(v.scope.ScopeID, 0700) != nil {
		return fs, x.ErrHeld
	}
	r, e := v.roots.Data.OpenRoot(v.scope.ScopeID)
	_, e = v.root(r, e)
	if e != nil {
		return fs, e
	}
	f, e := r.Open(".")
	h, e := v.file(f, e)
	if e != nil {
		return fs, e
	}
	id, ie := v.identity(f, nil, true)
	ce := v.release(h)
	if ie == nil {
		fs.Scope = &id
	}
	if ie != nil || ce != nil {
		return fs, x.ErrHeld
	}
	f, e = r.OpenFile("scope-owner.json", os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	h, e = v.file(f, e)
	if e != nil {
		return fs, e
	}
	b := assessmentMarker(v.scope)
	n, we := f.Write(b)
	se := f.Sync()
	ce = v.release(h)
	if n != len(b) || we != nil || se != nil || ce != nil {
		return fs, x.ErrHeld
	}
	for _, name := range []string{"work", "output"} {
		if r.Mkdir(name, 0700) != nil {
			return fs, x.ErrHeld
		}
		f, e := r.Open(name)
		h, e = v.file(f, e)
		if e != nil {
			return fs, e
		}
		id, ie := v.identity(f, nil, true)
		ce := v.release(h)
		if ie == nil {
			if name == "work" {
				fs.WorkRoot = &id
			} else {
				fs.OutputRoot = &id
			}
		}
		if ie != nil || ce != nil {
			return fs, x.ErrHeld
		}
	}
	if v.syncRoot(r) != nil || v.syncRoot(v.roots.Data) != nil {
		return fs, x.ErrHeld
	}
	v.acquired = true
	v.fresh = true
	v.scope.FilesystemIdentity = fs
	return fs, nil
}
func (o *AssessmentOperation) OpenContent() (*AssessmentFile, x.FilesystemIdentity, error) {
	v, e := o.valid()
	if e != nil {
		return nil, x.FilesystemIdentity{}, e
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.role != "writer" || !v.acquired || !v.fresh || v.closeAttempted {
		return nil, x.FilesystemIdentity{}, x.ErrHeld
	}
	r, e := v.openScope()
	if e != nil {
		return nil, x.FilesystemIdentity{}, e
	}
	f, e := r.OpenFile("payload", os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	h, e := v.file(f, e)
	if e != nil {
		return nil, x.FilesystemIdentity{}, e
	}
	id, e := v.identity(f, nil, false)
	if e != nil || v.syncRoot(r) != nil {
		return nil, id, x.ErrHeld
	}
	return &AssessmentFile{v, h}, id, nil
}
func (o *AssessmentOperation) OpenInput() (*AssessmentFile, error) {
	v, e := o.valid()
	if e != nil {
		return nil, e
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.role != "execute" || !v.acquired || !v.fresh || v.closeAttempted {
		return nil, x.ErrHeld
	}
	r, e := v.openScope()
	if e != nil {
		return nil, e
	}
	f, e := r.OpenFile("payload", os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	h, e := v.file(f, e)
	if e != nil {
		return nil, e
	}
	_, ie := v.identity(f, v.scope.FilesystemIdentity.Content, false)
	st, se := f.Stat()
	if ie != nil || se != nil || v.scope.SealedLengthBytes == nil || st.Size() != *v.scope.SealedLengthBytes {
		return nil, x.ErrHeld
	}
	return &AssessmentFile{v, h}, nil
}

// ReserveWatch consumes the shared reservation before first dup/pipe/open.
func (o *AssessmentOperation) ReserveWatch(watchWrite *os.File, guard AssessmentBeginGuard) (*AssessmentOperation, error) {
	v, e := o.valid()
	if e != nil {
		return nil, e
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.role != "prepare" || v.watchReserved || !v.acquired || v.closeAttempted {
		return nil, x.ErrHeld
	}
	v.watchReserved = true
	var row x.ExecutionScope
	e = v.store.Update(func(d *Data) error {
		s := d.AssessmentExecution.Scopes[v.scope.ScopeID]
		if guard == nil {
			return x.ErrHeld
		}
		n, e := guard(*d)
		if e != nil {
			return e
		}
		epoch, e := randomID()
		if e != nil {
			return e
		}
		row, e = x.AdvanceScope(s, x.ScopeWatchClaim, n.Wall.UTC().Format("2006-01-02T15:04:05.000000Z"), n.TickMS, func(n *x.ExecutionScope) { n.WatchStarted = true; n.WatchEpoch = &epoch })
		if e == nil {
			d.AssessmentExecution.Scopes[s.ScopeID] = row
		}
		return e
	})
	if e != nil {
		return nil, e
	}
	child := newAssessmentOperation(v.store, row, nil, "watch", v.roots, guard)
	cv := child.state
	v.watchChild = cv
	st, e := watchWrite.Stat()
	if e != nil {
		return nil, x.ErrHeld
	}
	id, e := FileIdentity(st)
	if e != nil || id != row.FilesystemIdentity.WatchWrite || st.Mode()&os.ModeNamedPipe == 0 {
		return nil, x.ErrHeld
	}
	// The original parent write endpoint is explicitly setup-owned, distinct
	// from the duplicate. There is no general-purpose descriptor registration.
	ph, _ := v.file(watchWrite, nil)
	fd, e := syscall.Dup(int(watchWrite.Fd()))
	if e != nil {
		return nil, e
	}
	write := os.NewFile(uintptr(fd), "v7-watch-write")
	_, e = cv.file(write, nil)
	if e != nil {
		return nil, e
	}
	if v.fail("watch-after-dup") != nil {
		return nil, x.ErrHeld
	}
	r, w, e := os.Pipe()
	if r != nil {
		v.file(r, nil)
	}
	if w != nil {
		cv.file(w, nil)
	}
	if e != nil {
		cv.uncertain = true
		return nil, e
	}
	if v.fail("watch-after-pipe") != nil {
		return nil, x.ErrHeld
	}
	v.readyRead = r
	cv.readyWrite = w
	cv.watchWrite = write
	v.watchChild = cv
	v.scope = row
	_ = ph
	return child, nil
}
func (o *AssessmentOperation) LaunchWatch(run func(*AssessmentOperation)) error {
	v, e := o.valid()
	if e != nil {
		return e
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.role != "watch" || v.watchLaunched || v.aborted || run == nil || v.closeAttempted {
		return x.ErrHeld
	}
	v.watchLaunched = true
	go run(o)
	return nil
}
func (o *AssessmentOperation) PublishWatchReady(guard AssessmentBeginGuard) error {
	v, e := o.valid()
	if e != nil {
		return e
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.role != "watch" || !v.watchLaunched || v.acquisitionStarted || v.watchWrite == nil || v.readyWrite == nil {
		return x.ErrHeld
	}
	if e = assessmentBeforeJournalV7(v.roots, string(x.ScopeWatchReady)); e != nil {
		return e
	}
	v.acquisitionStarted = true
	if v.scope.FilesystemIdentity.WatchLease == nil {
		return x.ErrHeld
	}
	if e = v.lease(v.roots.State, "owned-"+v.scope.ScopeID+"-watch.lock", *v.scope.FilesystemIdentity.WatchLease, syscall.LOCK_EX); e != nil {
		return e
	}
	var s x.ExecutionScope
	refused := false
	e = v.store.Update(func(d *Data) error {
		row := d.AssessmentExecution.Scopes[v.scope.ScopeID]
		if guard == nil {
			return x.ErrHeld
		}
		n, e := guard(*d)
		if e != nil {
			s, e = denyAssessmentForwardV7(d, row, n)
			refused = true
			return e
		}
		s, e = x.AdvanceScope(row, x.ScopeWatchReady, n.Wall.UTC().Format("2006-01-02T15:04:05.000000Z"), n.TickMS, func(n *x.ExecutionScope) { n.State = "accepting" })
		if e == nil {
			d.AssessmentExecution.Scopes[s.ScopeID] = s
		}
		return e
	})
	if e != nil {
		return e
	}
	if refused {
		return x.ErrDenied
	}
	ready := x.Ready{Schema: x.ReadySchema, Binding: s.Binding(), Generation: s.Generation, ExpiresAt: s.ExpiresAt, DeclaredUploadCeilingBytes: s.DeclaredUploadCeilingBytes, AdmittingOwnerEpoch: s.AdmittingOwnerEpoch, WriterEpoch: s.WriterEpoch, WatchEpoch: *s.WatchEpoch, ClockEpoch: s.ClockEpoch, DeadlineTickMS: s.DeadlineTickMS, Assurance: "synthetic"}
	raw, e := x.EncodeReply(ready)
	if e != nil {
		return e
	}
	if v.readyWrite.SetWriteDeadline(time.Now().Add(time.Second)) != nil {
		return x.ErrHeld
	}
	n, we := v.readyWrite.Write(raw)
	var ce error
	for _, h := range v.handles {
		if h.file == v.readyWrite {
			ce = v.release(h)
		}
	}
	if we != nil || n != len(raw) || ce != nil {
		return x.ErrHeld
	}
	v.acquired = true
	v.fresh = true
	return nil
}
func (o *AssessmentOperation) ReadAndValidateReady(ctx context.Context) error {
	v, e := o.valid()
	if e != nil {
		return e
	}
	v.mu.Lock()
	if v.role != "prepare" || v.readyRead == nil || v.closeAttempted {
		v.mu.Unlock()
		return x.ErrHeld
	}
	f := v.readyRead
	child := v.watchChild
	s := v.scope
	v.mu.Unlock()
	if f.SetReadDeadline(time.Now().Add(3*time.Second)) != nil {
		return x.ErrHeld
	}
	stop := context.AfterFunc(ctx, func() { _ = f.SetReadDeadline(time.Now()) })
	defer stop()
	raw, e := io.ReadAll(io.LimitReader(f, x.MaxReplyBytes+1))
	if e != nil || len(raw) == 0 || len(raw) > x.MaxReplyBytes || raw[len(raw)-1] != '\n' {
		return x.ErrHeld
	}
	var ready x.Ready
	if x.DecodeClosed(raw[:len(raw)-1], &ready, x.MaxReplyBytes) != nil || ready.Schema != x.ReadySchema || ready.Binding != s.Binding() || ready.Generation != s.Generation+1 || ready.ExpiresAt != s.ExpiresAt || ready.DeclaredUploadCeilingBytes != s.DeclaredUploadCeilingBytes || ready.AdmittingOwnerEpoch != s.AdmittingOwnerEpoch || ready.WriterEpoch != s.WriterEpoch || s.WatchEpoch == nil || ready.WatchEpoch != *s.WatchEpoch || ready.ClockEpoch != s.ClockEpoch || ready.DeadlineTickMS != s.DeadlineTickMS || ready.Assurance != "synthetic" || child == nil {
		return x.ErrHeld
	}
	child.mu.Lock()
	launched := child.watchLaunched && child.acquired && child.fresh
	child.mu.Unlock()
	v.mu.Lock()
	defer v.mu.Unlock()
	if !launched || v.closeAttempted {
		return x.ErrHeld
	}
	v.ready = true
	return nil
}

// Removal is performed here, never asserted by a caller boolean/receipt.
func (o *AssessmentOperation) RemoveOwnedScopeAndClose() error {
	v, e := o.valid()
	if e != nil {
		return e
	}
	v.mu.Lock()
	if v.role != "cleanup" || !v.acquired || !v.fresh || v.closeAttempted {
		v.mu.Unlock()
		return x.ErrHeld
	}
	if e = v.store.View(func(d Data) error {
		s := d.AssessmentExecution.Scopes[v.scope.ScopeID]
		if s.CleanupOwnerEpoch == nil || *s.CleanupOwnerEpoch != v.epoch || !cleanupQuiescentV7(d.AssessmentExecution, s) {
			return x.ErrHeld
		}
		return nil
	}); e != nil {
		v.mu.Unlock()
		_ = o.CloseAndCommitOwnership()
		return e
	}
	s := v.scope
	r, e := v.openScope()
	if e != nil {
		v.mu.Unlock()
		_ = o.CloseAndCommitOwnership()
		return e
	}
	for _, part := range []struct {
		name string
		id   *x.FilesystemIdentity
	}{{"work", s.FilesystemIdentity.WorkRoot}, {"output", s.FilesystemIdentity.OutputRoot}} {
		f, e := r.Open(part.name)
		h, e := v.file(f, e)
		if e != nil {
			v.mu.Unlock()
			_ = o.CloseAndCommitOwnership()
			return e
		}
		_, ie := v.identity(f, part.id, true)
		ce := v.release(h)
		if ie != nil || ce != nil || r.Remove(part.name) != nil {
			v.mu.Unlock()
			_ = o.CloseAndCommitOwnership()
			return x.ErrHeld
		}
	}
	f, e := r.Open(".")
	h, e := v.file(f, e)
	if e != nil {
		v.mu.Unlock()
		_ = o.CloseAndCommitOwnership()
		return e
	}
	entries, re := f.ReadDir(3)
	ce := v.release(h)
	if re != nil && re != io.EOF || ce != nil {
		v.mu.Unlock()
		_ = o.CloseAndCommitOwnership()
		return x.ErrHeld
	}
	for _, entry := range entries {
		if entry.Name() != "payload" && entry.Name() != "scope-owner.json" {
			v.mu.Unlock()
			_ = o.CloseAndCommitOwnership()
			return x.ErrHeld
		}
	}
	if _, e := r.Lstat("payload"); e == nil {
		f, e := r.OpenFile("payload", os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		h, e := v.file(f, e)
		if e != nil {
			v.mu.Unlock()
			_ = o.CloseAndCommitOwnership()
			return e
		}
		_, ie := v.identity(f, s.FilesystemIdentity.Content, false)
		ce := v.release(h)
		if ie != nil || ce != nil || r.Remove("payload") != nil {
			v.mu.Unlock()
			_ = o.CloseAndCommitOwnership()
			return x.ErrHeld
		}
	} else if !os.IsNotExist(e) {
		v.mu.Unlock()
		_ = o.CloseAndCommitOwnership()
		return x.ErrHeld
	}
	if v.syncRoot(r) != nil || r.Remove("scope-owner.json") != nil || v.syncRoot(r) != nil || v.roots.Data.Remove(s.ScopeID) != nil || v.syncRoot(v.roots.Data) != nil {
		v.mu.Unlock()
		_ = o.CloseAndCommitOwnership()
		return x.ErrHeld
	}
	if _, e := v.roots.Data.Lstat(s.ScopeID); !os.IsNotExist(e) {
		v.mu.Unlock()
		_ = o.CloseAndCommitOwnership()
		return x.ErrHeld
	}
	v.removed = true
	if v.fail("ownership-after-removal") != nil {
		v.uncertain = true
	}
	v.mu.Unlock()
	return o.CloseAndCommitOwnership()
}
