//go:build linux

package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	x "github.com/EduCloud-Ecosystem/outfitter/internal/assessmentexec"
	"github.com/EduCloud-Ecosystem/outfitter/internal/ledger"
	"github.com/EduCloud-Ecosystem/outfitter/internal/owneddata"
	"github.com/EduCloud-Ecosystem/outfitter/internal/provider"
)

func executionSeedSupported() bool { return true }

type executionSeedHandle struct {
	file      *os.File
	attempted bool
	result    error
}
type executionSeedFS struct {
	handles          []*executionSeedHandle
	created          bool
	clock            func() time.Time
	start, end, last time.Time
	fault            func(string) error
}

func (s *executionSeedFS) add(fd int, e error) (*os.File, error) {
	var f *os.File
	if fd >= 0 {
		f = os.NewFile(uintptr(fd), "private-seed")
		if f != nil {
			s.handles = append(s.handles, &executionSeedHandle{file: f})
		}
	}
	if e != nil || f == nil {
		return f, executionSessionProtocolError
	}
	return f, nil
}
func (s *executionSeedFS) close(f *os.File) error {
	for _, h := range s.handles {
		if h.file == f {
			if h.attempted {
				return h.result
			}
			h.attempted = true
			h.result = f.Close()
			if s.fault != nil && s.fault("after-close") != nil {
				h.result = executionSessionProtocolError
			}
			return h.result
		}
	}
	return executionSessionProtocolError
}
func (s *executionSeedFS) finish() error {
	var result error
	for i := len(s.handles) - 1; i >= 0; i-- {
		if s.close(s.handles[i].file) != nil {
			result = executionSessionProtocolError
		}
	}
	return result
}
func (s *executionSeedFS) gate() error {
	if s.clock == nil || s.end.IsZero() {
		return nil
	}
	n := s.clock().UTC()
	if n.Before(s.start) || !s.last.IsZero() && n.Before(s.last) || !n.Before(s.end) {
		return executionSessionProtocolError
	}
	s.last = n
	return nil
}
func executionSeedComponent(name string) bool {
	return name != "" && name != "." && name != ".." && filepath.Base(name) == name && !strings.ContainsAny(name, "/\x00")
}
func executionSeedControl(f *os.File, fn func(int) error) error {
	raw, e := f.SyscallConn()
	if e != nil {
		return e
	}
	var inner error
	e = raw.Control(func(fd uintptr) { inner = fn(int(fd)) })
	if e != nil {
		return e
	}
	return inner
}
func (s *executionSeedFS) openAt(parent *os.File, name string, flags int, perm uint32) (*os.File, error) {
	if !executionSeedComponent(name) {
		return nil, executionSessionProtocolError
	}
	fd := -1
	e := executionSeedControl(parent, func(p int) error {
		var e error
		fd, e = syscall.Openat(p, name, flags|syscall.O_NOFOLLOW|syscall.O_CLOEXEC|syscall.O_NONBLOCK, perm)
		return e
	})
	return s.add(fd, e)
}
func executionSeedShape(f *os.File, directory, private bool) (os.FileInfo, error) {
	st, e := f.Stat()
	if e != nil {
		return nil, executionSessionProtocolError
	}
	native, ok := st.Sys().(*syscall.Stat_t)
	if !ok || private && st.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 || directory != st.IsDir() || !directory && (!st.Mode().IsRegular() || native.Nlink != 1) || private && (native.Uid != uint32(os.Geteuid()) || directory && st.Mode().Perm() != 0700 || !directory && st.Mode().Perm() != 0600) {
		return nil, executionSessionProtocolError
	}
	return st, nil
}
func (s *executionSeedFS) parent(path string) (*os.File, error) {
	if !executionSeedPaths(path) {
		return nil, executionSessionProtocolError
	}
	fd, e := syscall.Open("/", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0)
	p, e := s.add(fd, e)
	if e != nil {
		return nil, e
	}
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		next, e := s.openAt(p, part, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
		if e != nil {
			return nil, e
		}
		if _, e = executionSeedShape(next, true, false); e != nil {
			return nil, e
		}
		p = next
	}
	if _, e = executionSeedShape(p, true, true); e != nil {
		return nil, e
	}
	return p, nil
}
func (s *executionSeedFS) directory(parent *os.File, name string, create bool) (*os.File, error) {
	if create {
		if s.gate() != nil || !executionSeedComponent(name) {
			return nil, executionSessionProtocolError
		}
		e := executionSeedControl(parent, func(fd int) error { return syscall.Mkdirat(fd, name, 0700) })
		if e != nil {
			return nil, e
		}
		if name == executionSeedTargetName {
			s.created = true
		}
		if s.gate() != nil {
			return nil, executionSessionProtocolError
		}
	}
	f, e := s.openAt(parent, name, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
	if e != nil {
		return f, e
	}
	_, e = executionSeedShape(f, true, true)
	return f, e
}

type executionSeedStable struct {
	dev, ino                 uint64
	size                     int64
	mode, uid, gid           uint32
	links                    uint64
	msec, mnano, csec, cnano int64
}

func executionSeedStamp(st os.FileInfo) executionSeedStable {
	n := st.Sys().(*syscall.Stat_t)
	return executionSeedStable{uint64(n.Dev), uint64(n.Ino), n.Size, n.Mode, n.Uid, n.Gid, uint64(n.Nlink), n.Mtim.Sec, n.Mtim.Nsec, n.Ctim.Sec, n.Ctim.Nsec}
}
func (s *executionSeedFS) read(parent *os.File, name string, limit int) ([]byte, error) {
	if s.gate() != nil {
		return nil, executionSessionProtocolError
	}
	f, e := s.openAt(parent, name, syscall.O_RDONLY, 0)
	if e != nil {
		return nil, e
	}
	before, e := executionSeedShape(f, false, true)
	if e != nil || before.Size() < 0 || before.Size() > int64(limit) {
		return nil, executionSessionProtocolError
	}
	b, re := io.ReadAll(io.LimitReader(f, int64(limit)+1))
	after, se := executionSeedShape(f, false, true)
	ce := s.close(f)
	if re != nil || se != nil || ce != nil || len(b) > limit || int64(len(b)) != before.Size() || executionSeedStamp(before) != executionSeedStamp(after) || s.gate() != nil {
		clear(b)
		return nil, executionSessionProtocolError
	}
	return b, nil
}
func (s *executionSeedFS) write(parent *os.File, name string, b []byte) (*os.File, error) {
	if s.gate() != nil {
		return nil, executionSessionProtocolError
	}
	f, e := s.openAt(parent, name, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL, 0600)
	if e != nil {
		return f, e
	}
	if _, e = executionSeedShape(f, false, true); e != nil {
		return f, e
	}
	if s.fault != nil && s.fault("before-write") != nil {
		return f, executionSessionProtocolError
	}
	if s.gate() != nil {
		return f, executionSessionProtocolError
	}
	n, e := f.Write(b)
	if e != nil || n != len(b) || s.gate() != nil {
		return f, executionSessionProtocolError
	}
	if e = f.Sync(); e != nil || s.gate() != nil {
		return f, executionSessionProtocolError
	}
	return f, nil
}
func (s *executionSeedFS) sync(f *os.File) error {
	if s.gate() != nil {
		return executionSessionProtocolError
	}
	if f.Sync() != nil || s.gate() != nil {
		return executionSessionProtocolError
	}
	return nil
}
func (s *executionSeedFS) build(parent *os.File, r executionSeedRequest) (executionSeedAssets, error) {
	now := s.clock()
	a, end, e := executionSeedPrepare(r, now)
	if e != nil || !s.last.IsZero() && now.Before(s.last) {
		return a, executionSessionProtocolError
	}
	start, _ := owneddata.Time(r.SampledAt, true)
	s.start, s.end, s.last = start, end, now
	target, e := s.directory(parent, executionSeedTargetName, true)
	if e != nil {
		return a, e
	}
	state, e := s.directory(target, "state", true)
	if e != nil {
		return a, e
	}
	data, e := s.directory(target, "data", true)
	if e != nil {
		return a, e
	}
	marker, e := s.write(state, "initialized", nil)
	if e != nil {
		return a, e
	}
	if s.close(marker) != nil {
		return a, executionSessionProtocolError
	}
	journal, e := s.write(state, "ledger.lock", nil)
	if e != nil {
		return a, e
	}
	execution, e := s.write(state, "execution.lock", nil)
	if e != nil {
		return a, e
	}
	id := func(f *os.File) (x.FilesystemIdentity, error) {
		st, e := f.Stat()
		if e != nil {
			return x.FilesystemIdentity{}, e
		}
		return ledger.FileIdentity(st)
	}
	dirID, e := id(state)
	if e != nil {
		return a, e
	}
	journalID, e := id(journal)
	if e != nil {
		return a, e
	}
	executionID, e := id(execution)
	if e != nil {
		return a, e
	}
	seed := ledger.Data{Version: 7, Owner: strings.Repeat("f", 32), Authority: &ledger.Authority{ID: strings.Repeat("5", 32), StatePath: a.Config.StateRoot, Admission: "active"}, Projects: map[string]ledger.Project{r.ProjectID: {ID: r.ProjectID, Budget: 60}}, Runs: map[string]provider.Record{}, AssessmentExecution: &x.Authority{Schema: x.AuthoritySchema, Identity: x.AuthorityIdentity{WaypointAuthorityID: r.WaypointAuthorityID, Directory: dirID, JournalLease: journalID, ExecutionLease: executionID}, Scopes: map[string]x.ExecutionScope{}, Attempts: map[string]x.AssessmentAttempt{}}}
	b, e := json.Marshal(seed)
	if e != nil {
		return a, e
	}
	file, e := s.write(state, "ledger.json", b)
	if e != nil {
		return a, e
	}
	if s.close(file) != nil || s.close(journal) != nil || s.close(execution) != nil {
		return a, executionSessionProtocolError
	}
	if s.gate() != nil {
		return a, executionSessionProtocolError
	}
	store, e := ledger.Open(a.Config.StateRoot, false)
	if e != nil || s.gate() != nil {
		return a, executionSessionProtocolError
	}
	e = store.View(func(d ledger.Data) error {
		if d.Version != 7 || d.Authority == nil || d.Authority.ID != seed.Authority.ID || d.Owner != seed.Owner || d.AssessmentExecution == nil || d.AssessmentExecution.Identity != seed.AssessmentExecution.Identity || len(d.AssessmentExecution.Scopes) != 0 || len(d.AssessmentExecution.Attempts) != 0 || len(d.Runs) != 0 || len(d.NativeRuns) != 0 || len(d.Projects) != 1 || d.Projects[r.ProjectID].Budget != 60 {
			return executionSessionProtocolError
		}
		return nil
	})
	if e != nil || s.gate() != nil {
		return a, executionSessionProtocolError
	}
	config, e := executionSeedEncode(a.Config, 131072)
	if e != nil {
		return a, e
	}
	file, e = s.write(target, "private-bootstrap.json", config)
	if e != nil {
		return a, e
	}
	if s.close(file) != nil {
		return a, executionSessionProtocolError
	}
	binding, e := executionSeedEncode(a.Binding, 8192)
	if e != nil {
		return a, e
	}
	file, e = s.write(target, "fixed-binding.json", binding)
	if e != nil {
		return a, e
	}
	if s.close(file) != nil {
		return a, executionSessionProtocolError
	}
	if s.sync(state) != nil || s.sync(data) != nil || s.sync(target) != nil || s.sync(parent) != nil {
		return a, executionSessionProtocolError
	}
	if s.gate() != nil {
		return a, executionSessionProtocolError
	}
	return a, nil
}
func executionSeedReadConfig(path string) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || len(path) > 1024 || filepath.Base(path) != "private-bootstrap.json" || filepath.Base(filepath.Dir(path)) != executionSeedTargetName {
		return nil, executionSessionProtocolError
	}
	parentPath := filepath.Dir(filepath.Dir(path))
	s := &executionSeedFS{}
	parent, e := s.parent(parentPath)
	if e != nil {
		_ = s.finish()
		return nil, e
	}
	target, e := s.directory(parent, executionSeedTargetName, false)
	if e != nil {
		_ = s.finish()
		return nil, e
	}
	raw, e := s.read(target, "private-bootstrap.json", 131072)
	ce := s.finish()
	var c executionSessionConfig
	if e != nil || ce != nil || x.DecodeClosed(raw, &c, 131072) != nil || c.StateRoot != filepath.Join(parentPath, executionSeedTargetName, "state") || c.DataRoot != filepath.Join(parentPath, executionSeedTargetName, "data") {
		return nil, executionSessionProtocolError
	}
	return raw, nil
}
func executionSeedPlatformEntry(path string) int {
	s := &executionSeedFS{clock: func() time.Time { return time.Now().UTC() }}
	code := "REFUSED"
	// Stdio wrappers must already be pollable. Mark remapped endpoints private
	// before any effect. The seed inherits no ExtraFiles/session endpoints.
	if executionSeedPrivateInput() != nil || executionSessionPrivatePipe(os.Stdout) != nil || executionSessionPrivatePipe(os.Stderr) != nil {
		_ = executionSessionCloseFiles([9]*os.File{os.Stdin, os.Stdout, os.Stderr})
		return 2
	}
	if filepath.Base(path) != executionSeedRequestName {
		return executionSeedPublish(s, code, time.Time{})
	}
	parent, e := s.parent(filepath.Dir(path))
	if e != nil {
		return executionSeedPublish(s, code, time.Time{})
	}
	raw, e := s.read(parent, executionSeedRequestName, 8192)
	if e != nil {
		return executionSeedPublish(s, code, time.Time{})
	}
	observed := s.clock()
	r, _, end, e := executionSeedDecode(raw, path, observed)
	clear(raw)
	if e != nil {
		return executionSeedPublish(s, code, time.Time{})
	}
	s.last = observed
	_, e = s.build(parent, r)
	if e == nil {
		code = "SEEDED"
	} else if s.created || !errors.Is(e, syscall.EEXIST) {
		code = "HELD"
	}
	return executionSeedPublish(s, code, end)
}
func executionSeedPublish(s *executionSeedFS, code string, end time.Time) int {
	return executionSeedPublishFiles(s, code, end, [3]*os.File{os.Stdin, os.Stdout, os.Stderr})
}
func executionSeedPublishFiles(s *executionSeedFS, code string, end time.Time, files [3]*os.File) int {
	if s.finish() != nil {
		code = "HELD"
	}
	if !end.IsZero() && s.gate() != nil {
		code = "HELD"
	}
	deadline := time.Now().Add(time.Second)
	if !end.IsZero() {
		deadline = end
	}
	if files[1].SetWriteDeadline(deadline) != nil {
		code = "HELD"
	}
	raw, e := executionSeedEncode(executionSeedResult{executionSeedResultSchema, code}, 256)
	if e != nil {
		raw = nil
	}
	writeErr := executionSessionWrite(files[1], raw)
	if !end.IsZero() && s.gate() != nil {
		writeErr = executionSessionProtocolError
	}
	var closeErr error
	for _, f := range files {
		if f != nil && f.Close() != nil {
			closeErr = executionSessionProtocolError
		}
	}
	// A TEST-only negative seam cannot replace any actual close or mint ACK.
	if s.fault != nil && s.fault("after-stdio-close") != nil {
		closeErr = executionSessionProtocolError
	}
	if !end.IsZero() && s.gate() != nil {
		closeErr = executionSessionProtocolError
	}
	if code != "SEEDED" || e != nil || writeErr != nil || closeErr != nil {
		return 2
	}
	return 0
}

// Fixed Go driver dispatch, never a service or HTTP capability. It uses only
// the already-built test executable; the seed child owns only reviewed stdio.
type executionSeedProcessObservation struct {
	Started, Waited, Complete, StderrEmpty, ParentClosed, Accepted, TimedOut bool
	ExitCode                                                                 int
	ReplyCode                                                                string
	Child                                                                    *executionSeedChild
}
type executionSeedProcessOptions struct {
	closedOutput, blockedOutput, finalCloseFault bool
	waitPublication                              <-chan struct{}
	waitReturned                                 chan<- struct{}
}

func executionSeedRunOriginal(r executionSeedRequest) (*executionSeedChild, error) {
	_, end, e := executionSeedPrepare(r, time.Now().UTC())
	if e != nil {
		return nil, e
	}
	d, _ := owneddata.Time(r.SessionDeadline, true)
	o := executionSeedOriginalChild(filepath.Join(r.ScratchParent, executionSeedRequestName), end, d, executionSeedProcessOptions{})
	if !o.Accepted {
		return o.Child, executionSessionProtocolError
	}
	return o.Child, nil
}
func executionSeedOriginalChild(path string, end, d time.Time, option executionSeedProcessOptions) executionSeedProcessObservation {
	o := executionSeedProcessObservation{ExitCode: -1, ReplyCode: "unknown"}
	reapEnd := executionSessionMinDeadline(end.Add(2*time.Second), d)
	ctx, cancel := context.WithDeadline(context.Background(), end)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExecutionPrivateSessionSeedHelper$")
	cmd.Env = []string{"OUTFITTER_PRIVATE_EXECUTION_SESSION_SEED=1", "OUTFITTER_PRIVATE_EXECUTION_SESSION_SEED_REQUEST=" + path, "GOMAXPROCS=2"}
	cmd.WaitDelay = 250 * time.Millisecond
	owned := &executionSeedFS{}
	fd, e := syscall.Open("/dev/null", syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0)
	input, e := owned.add(fd, e)
	if e != nil {
		_ = owned.finish()
		return o
	}
	outR, outW, e := os.Pipe()
	if outR != nil {
		owned.handles = append(owned.handles, &executionSeedHandle{file: outR})
	}
	if outW != nil {
		owned.handles = append(owned.handles, &executionSeedHandle{file: outW})
	}
	if e != nil {
		_ = owned.finish()
		return o
	}
	errR, errW, e := os.Pipe()
	if errR != nil {
		owned.handles = append(owned.handles, &executionSeedHandle{file: errR})
	}
	if errW != nil {
		owned.handles = append(owned.handles, &executionSeedHandle{file: errW})
	}
	if e != nil {
		_ = owned.finish()
		return o
	}
	child := make([]*os.File, 3)
	for i, source := range []*os.File{input, outW, errW} {
		f, e := executionSessionSpawnDuplicate(source)
		if f != nil {
			owned.handles = append(owned.handles, &executionSeedHandle{file: f})
		}
		child[i] = f
		if e != nil {
			_ = owned.finish()
			return o
		}
	}
	if outR.SetReadDeadline(reapEnd) != nil || errR.SetReadDeadline(reapEnd) != nil {
		_ = owned.finish()
		return o
	}
	if option.blockedOutput {
		// Bounded prefill on the original nonblocking write endpoint. No retry
		// after anything except successful accepted bytes; cap the fill work.
		full := false
		e = executionSeedControl(outW, func(fd int) error {
			b := make([]byte, 4096)
			for i := 0; i < 256; i++ {
				n, e := syscall.Write(fd, b)
				if e == syscall.EAGAIN || e == syscall.EWOULDBLOCK {
					full = true
					return nil
				}
				if e != nil || n != len(b) {
					return executionSessionProtocolError
				}
			}
			return executionSessionProtocolError
		})
		if e != nil || !full {
			_ = owned.finish()
			return o
		}
	}
	if option.closedOutput {
		if owned.close(outR) != nil {
			_ = owned.finish()
			return o
		}
	}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = child[0], child[1], child[2]
	if ctx.Err() != nil || !time.Now().Before(end) || cmd.Start() != nil {
		_ = owned.finish()
		return o
	}
	o.Started = true
	closeErr := owned.close(input)
	for _, f := range append(child, outW, errW) {
		if owned.close(f) != nil {
			closeErr = executionSessionProtocolError
		}
	}
	type response struct {
		raw []byte
		err error
	}
	stdout := make(chan response, 1)
	stderr := make(chan response, 1)
	if !option.closedOutput && !option.blockedOutput {
		go func() { b, e := io.ReadAll(io.LimitReader(outR, 257)); stdout <- response{b, e} }()
	} else {
		stdout = nil
	}
	go func() { b, e := io.ReadAll(io.LimitReader(errR, 4097)); stderr <- response{b, e} }()
	o.Child = executionSeedChildWaitPublication(cmd, cancel, owned.finish, reapEnd, option.waitPublication, option.waitReturned)
	wait := o.Child.done
	timer := time.NewTimer(time.Until(reapEnd))
	defer timer.Stop()
	var result, diagnostic response
	var waitErr error
	remaining := 3
	if stdout == nil {
		remaining = 2
	}
	for i := 0; i < remaining; i++ {
		select {
		case result = <-stdout:
			stdout = nil
		case diagnostic = <-stderr:
			stderr = nil
		case <-wait:
			wait = nil
			o.Waited = true
			waitErr = o.Child.waitErr
			o.ExitCode = o.Child.exitCode
		case <-timer.C:
			o.TimedOut = true
			o.Waited = o.Child.Finish()
			if o.Waited {
				o.ExitCode = o.Child.exitCode
			}
			return o
		}
	}
	if option.finalCloseFault {
		owned.fault = func(point string) error {
			if point == "after-close" {
				return executionSessionProtocolError
			}
			return nil
		}
	}
	finishErr := owned.finish()
	o.ParentClosed = finishErr == nil && closeErr == nil
	o.StderrEmpty = diagnostic.err == nil && len(diagnostic.raw) == 0
	o.ReplyCode, o.Complete = executionSeedResultCode(result.raw)
	o.Accepted = o.Started && o.Waited && waitErr == nil && o.ExitCode == 0 && o.Complete && o.ReplyCode == "SEEDED" && result.err == nil && o.StderrEmpty && o.ParentClosed && time.Now().Before(end)
	clear(result.raw)
	clear(diagnostic.raw)
	return o
}

func executionSeedReadAssets(parent *os.File, r executionSeedRequest) (executionSeedAssets, error) {
	return executionSeedReadAssetsUsing(parent, r, &executionSeedFS{clock: func() time.Time { return time.Now().UTC() }})
}
func executionSeedReadAssetsUsing(parent *os.File, r executionSeedRequest, s *executionSeedFS) (executionSeedAssets, error) {
	observed := s.clock()
	want, end, e := executionSeedPrepare(r, observed)
	if e != nil {
		return want, e
	}
	s.end = end
	s.start, _ = owneddata.Time(r.SampledAt, true)
	s.last = observed
	target, e := s.directory(parent, executionSeedTargetName, false)
	if e != nil {
		_ = s.finish()
		return want, e
	}
	raw, e := s.read(target, "private-bootstrap.json", 131072)
	var c executionSessionConfig
	if e != nil || x.DecodeClosed(raw, &c, 131072) != nil || !reflect.DeepEqual(c, want.Config) {
		_ = s.finish()
		return want, executionSessionProtocolError
	}
	raw, e = s.read(target, "fixed-binding.json", 8192)
	expected, ee := executionSeedEncode(want.Binding, 8192)
	ce := s.finish()
	if e != nil || ee != nil || !bytes.Equal(raw, expected) || ce != nil || s.gate() != nil {
		return want, executionSessionProtocolError
	}
	return want, nil
}

func executionSeedPrivateInput() error {
	f, e := os.OpenFile("/dev/null", os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0)
	if e != nil {
		return executionSessionProtocolError
	}
	want, we := f.Stat()
	got, ge := os.Stdin.Stat()
	ce := f.Close()
	if we != nil || ge != nil || ce != nil || !os.SameFile(want, got) || got.Mode()&os.ModeCharDevice == 0 {
		return executionSessionProtocolError
	}
	return executionSeedControl(os.Stdin, func(fd int) error {
		flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFD, 0)
		if errno != 0 {
			return errno
		}
		_, _, errno = syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_SETFD, flags|syscall.FD_CLOEXEC)
		if errno != 0 {
			return errno
		}
		return nil
	})
}
func (s *executionSeedFS) retainParent(parent *os.File) {
	for i, h := range s.handles {
		if h.file == parent {
			s.handles = append(s.handles[:i], s.handles[i+1:]...)
			return
		}
	}
}
func executionSeedDriverSetup(base string, external bool) (executionSeedAssets, *os.File, *executionSeedChild, error) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	expiry := now.Add(time.Hour).Truncate(time.Second)
	r, e := executionSeedRequestFor(base, strings.Repeat("4", 32), strings.Repeat("2", 32), strings.Repeat("3", 32), strings.Repeat("8", 32), now, expiry, expiry, expiry)
	if e != nil {
		return executionSeedAssets{}, nil, nil, e
	}
	a, end, e := executionSeedPrepare(r, now)
	if e != nil {
		return a, nil, nil, e
	}
	s := &executionSeedFS{clock: func() time.Time { return time.Now().UTC() }, start: now, end: end}
	parent, e := s.parent(base)
	if e != nil {
		_ = s.finish()
		return a, nil, nil, e
	}
	request, e := executionSeedEncode(r, 8192)
	if e == nil {
		var f *os.File
		f, e = s.write(parent, executionSeedRequestName, request)
		if e == nil {
			e = s.close(f)
		}
	}
	if e == nil && s.sync(parent) != nil {
		e = executionSessionProtocolError
	}
	var child *executionSeedChild
	if e == nil {
		if external {
			child, e = executionSeedRunOriginal(r)
		} else {
			a, e = s.build(parent, r)
		}
	}
	if e == nil {
		a, e = executionSeedReadAssets(parent, r)
	}
	// This exact opened parent object remains owned by the driver through asset
	// acceptance and owner launch. It is never serialized as authority.
	s.retainParent(parent)
	ce := s.finish()
	if e != nil || ce != nil {
		return a, parent, child, executionSessionProtocolError
	}
	return a, parent, child, nil
}

// These controls construct only private synthetic fixtures. A refused/partial
// target is retained, never repaired or removed to make another seed succeed.
type executionSeedControlFixture struct {
	fs                     *executionSeedFS
	parent                 *os.File
	request                executionSeedRequest
	end                    time.Time
	children               []*executionSeedChild
	expectedUnknown        bool
	expectedCloseUncertain bool
}

func executionSeedControlNew(t *testing.T, capSeconds int) *executionSeedControlFixture {
	t.Helper()
	path, e := os.MkdirTemp("", "outfitter-seed-control-")
	if e != nil {
		t.Fatal("private seed control parent")
	}
	sample := time.Now().UTC().Truncate(time.Microsecond)
	cap := sample.Add(time.Hour)
	if capSeconds > 0 {
		cap = sample.Add(time.Duration(capSeconds) * time.Second)
	}
	r, e := executionSeedRequestFor(path, strings.Repeat("4", 32), strings.Repeat("2", 32), strings.Repeat("3", 32), strings.Repeat("8", 32), sample, cap, cap, cap)
	if e != nil {
		t.Fatal("fixed seed control request")
	}
	_, end, e := executionSeedPrepare(r, sample)
	if e != nil {
		t.Fatal("fixed seed control cutoff")
	}
	f := &executionSeedControlFixture{fs: &executionSeedFS{clock: func() time.Time { return time.Now().UTC() }, start: sample, end: end}, request: r, end: end}
	t.Cleanup(func() {
		for _, child := range f.children {
			if !child.Finish() && !f.expectedUnknown {
				t.Error("original seed Wait unknown; private control root retained")
			}
		}
		if f.fs.finish() != nil && !f.expectedCloseUncertain {
			t.Error("registered control close acknowledgement")
		}
	})
	f.parent, e = f.fs.parent(path)
	if e != nil {
		t.Fatal("opened private control parent")
	}
	raw, e := executionSeedEncode(r, 8192)
	if e != nil {
		t.Fatal("closed control request")
	}
	file, e := f.fs.write(f.parent, executionSeedRequestName, raw)
	if e != nil {
		t.Fatal("exclusive request creation")
	}
	if f.fs.close(file) != nil || f.fs.sync(f.parent) != nil {
		t.Fatal("checked control request publication")
	}
	return f
}
func (f *executionSeedControlFixture) original(t *testing.T, option executionSeedProcessOptions) executionSeedProcessObservation {
	t.Helper()
	d, _ := owneddata.Time(f.request.SessionDeadline, true)
	o := executionSeedOriginalChild(filepath.Join(f.request.ScratchParent, executionSeedRequestName), f.end, d, option)
	if o.Child != nil {
		f.children = append(f.children, o.Child)
	}
	if !o.Started {
		t.Fatal("control did not dispatch original seed child")
	}
	return o
}
func (f *executionSeedControlFixture) build(t *testing.T) executionSeedAssets {
	t.Helper()
	a, e := f.fs.build(f.parent, f.request)
	if e != nil {
		t.Fatal("actual fresh seed factor refused")
	}
	return a
}
func executionSeedControlFinished(t *testing.T, s *executionSeedFS) {
	t.Helper()
	_ = s.finish()
	for _, h := range s.handles {
		if !h.attempted {
			t.Fatal("registered handle close not attempted")
		}
	}
}
func executionSeedControlRefused(t *testing.T, o executionSeedProcessObservation) {
	t.Helper()
	if !o.Started || !o.Waited || o.TimedOut || o.ExitCode != 2 || !o.Complete || o.ReplyCode != "REFUSED" || !o.StderrEmpty || !o.ParentClosed || o.Accepted {
		t.Fatal("actual original child refusal was not closed and definitive")
	}
}
func TestExecutionSessionSeedBoundaryControls(t *testing.T) {
	t.Run("literal-and-clock-boundaries", func(t *testing.T) {
		executionSeedValidateVector(t, executionSeedVectorSource)
		var v executionSeedVector
		if x.DecodeClosed(executionSeedVectorSource, &v, 16384) != nil {
			t.Fatal("closed literal fixture")
		}
		r := v.Request
		s, _ := owneddata.Time(r.SampledAt, true)
		raw, e := executionSeedEncode(r, 8192)
		if e != nil {
			t.Fatal("literal request encode")
		}
		path := filepath.Join(r.ScratchParent, executionSeedRequestName)
		if _, _, _, e = executionSeedDecode(raw, path, s); e != nil {
			t.Fatal("valid exact request frame")
		}
		bad := [][]byte{raw[:len(raw)-1], append(append([]byte{}, raw...), '\n'), append(append([]byte{}, raw...), []byte("{}\n")...), append([]byte(" "), raw...), []byte("{}\n"), bytes.Replace(raw, []byte(`"schema":`), []byte(`"schema":null,"schema":`), 1), bytes.Replace(raw, []byte(`"schema":`), []byte(`"\u0073chema":"duplicate","schema":`), 1), bytes.Replace(raw, []byte(`"owner_epoch":`), []byte(`"unknown":"x","owner_epoch":`), 1), bytes.Replace(raw, []byte(`"owner_epoch":"`+r.OwnerEpoch+`"`), []byte(`"owner_epoch":3`), 1), bytes.Replace(raw, []byte(`"owner_epoch":"`+r.OwnerEpoch+`",`), nil, 1)}
		for i, b := range bad {
			if _, _, _, e = executionSeedDecode(b, path, s); e == nil {
				t.Fatalf("closed request rejection missing at fixed case %d", i)
			}
		}
		for i, key := range []string{"schema", "scratch_parent", "waypoint_authority_id", "project_id", "scope_id", "owner_epoch", "sampled_at", "session_deadline", "authorization_expiry", "project_expiry", "policy_expiry", "manifest_sha256"} {
			var object map[string]json.RawMessage
			if json.Unmarshal(raw, &object) != nil {
				t.Fatal("literal mutation setup")
			}
			delete(object, key)
			b, _ := json.Marshal(object)
			b = append(b, '\n')
			if _, _, _, e = executionSeedDecode(b, path, s); e == nil {
				t.Fatalf("required request key accepted at fixed case %d", i)
			}
			object[key] = json.RawMessage("null")
			b, _ = json.Marshal(object)
			b = append(b, '\n')
			if _, _, _, e = executionSeedDecode(b, path, s); e == nil {
				t.Fatalf("null request key accepted at fixed case %d", i)
			}
		}
		if _, _, _, e = executionSeedDecode(raw, filepath.Join(r.ScratchParent, "other.json"), s); e == nil {
			t.Fatal("request sibling mismatch accepted")
		}
		if _, _, e = executionSeedPrepare(r, s.Add(10*time.Second)); e == nil {
			t.Fatal("expired seed window accepted")
		}
		if _, _, e = executionSeedPrepare(r, s.Add(-time.Microsecond)); e == nil {
			t.Fatal("rollback accepted")
		}
		a, end, e := executionSeedPrepare(r, s)
		d, _ := owneddata.Time(r.SessionDeadline, true)
		policy, _ := owneddata.Time(a.Config.PolicyExpiry, false)
		if e != nil || !end.Equal(s.Add(10*time.Second)) || policy.After(d) || !policy.After(end) || a.Config.SessionDeadline != r.SessionDeadline {
			t.Fatal("seed cutoff changed original session policy")
		}
		for i, b := range [][]byte{[]byte(`{"schema":"outfitter.execution-session-seed-result/v1","code":"SEEDED"}` + "\n"), []byte(`{"code":"REFUSED","schema":"outfitter.execution-session-seed-result/v1"}` + "\n"), []byte(`{"code":"HELD","schema":"outfitter.execution-session-seed-result/v1"}` + "\n")} {
			if _, ok := executionSeedResultCode(b); !ok {
				t.Fatalf("closed result case %d", i)
			}
			for j, poison := range [][]byte{b[:len(b)-1], append(append([]byte{}, b...), '\n'), append(append([]byte{}, b...), []byte("PASS\n")...), bytes.Replace(b, []byte(`"code":`), []byte(`"code":null,"code":`), 1), bytes.Replace(b, []byte(`"code":`), []byte(`"extra":true,"code":`), 1)} {
				if _, ok := executionSeedResultCode(poison); ok {
					t.Fatalf("poison result accepted at fixed case %d/%d", i, j)
				}
			}
		}
		if executionSeedPaths("/"+strings.Repeat("a", 990)) || executionSeedPaths("/tmp/../tmp/x") || executionSeedPaths("relative") || executionSeedComponent("..") || executionSeedComponent("a/b") {
			t.Fatal("path bound refusal missing")
		}
	})
	t.Run("fresh-factor-shape", func(t *testing.T) {
		f := executionSeedControlNew(t, 0)
		a := f.build(t)
		got, e := executionSeedReadAssets(f.parent, f.request)
		if e != nil || !reflect.DeepEqual(a, got) {
			t.Fatal("anchored fresh asset validation")
		}
		store, e := ledger.Open(a.Config.StateRoot, false)
		if e != nil {
			t.Fatal("actual seeded Store open")
		}
		if store.View(func(d ledger.Data) error {
			state, e := os.Stat(a.Config.StateRoot)
			if e != nil {
				return executionSessionProtocolError
			}
			id, e := ledger.FileIdentity(state)
			if e != nil || d.Version != 7 || d.AssessmentExecution == nil || d.AssessmentExecution.Identity.Directory != id || d.AssessmentExecution.Identity.WaypointAuthorityID != f.request.WaypointAuthorityID || len(d.AssessmentExecution.Attempts) != 0 || len(d.AssessmentExecution.Scopes) != 0 || len(d.Runs) != 0 || len(d.NativeRuns) != 0 || len(d.Projects) != 1 || d.Projects[f.request.ProjectID].Budget != 60 {
				return executionSessionProtocolError
			}
			return nil
		}) != nil {
			t.Fatal("fresh actual paired authority shape")
		}
		if a.Config.ScopeID != f.request.ScopeID || a.Config.OwnerEpoch != f.request.OwnerEpoch {
			t.Fatal("fixed original identities changed")
		}
	})
	t.Run("existing-and-partial-refuse", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			f := executionSeedControlNew(t, 0)
			target := filepath.Join(f.request.ScratchParent, executionSeedTargetName)
			if os.Mkdir(target, 0700) != nil {
				t.Fatal("existing target fixture")
			}
			sentinel := []byte("fixed-partial-fixture")
			if i == 1 {
				if os.WriteFile(filepath.Join(target, "partial"), sentinel, 0600) != nil {
					t.Fatal("partial target fixture")
				}
			}
			o := f.original(t, executionSeedProcessOptions{})
			executionSeedControlRefused(t, o)
			entries, e := os.ReadDir(target)
			if e != nil || len(entries) != i {
				t.Fatal("existing target changed")
			}
			if i == 1 {
				b, e := os.ReadFile(filepath.Join(target, "partial"))
				if e != nil || !bytes.Equal(b, sentinel) {
					t.Fatal("partial target overwritten")
				}
			}
		}
	})
	t.Run("nofollow-and-private-shape", func(t *testing.T) {
		// The actual child must reject each special request before any read can
		// block. These are fixed negative fixtures, not caller identity proof.
		for mode := 0; mode < 5; mode++ {
			f := executionSeedControlNew(t, 0)
			path := filepath.Join(f.request.ScratchParent, executionSeedRequestName)
			other := filepath.Join(f.request.ScratchParent, "negative-request")
			if os.Rename(path, other) != nil {
				t.Fatal("negative request move")
			}
			switch mode {
			case 0:
				if os.Symlink("negative-request", path) != nil {
					t.Fatal("symlink fixture")
				}
			case 1:
				if os.Link(other, path) != nil {
					t.Fatal("hardlink fixture")
				}
			case 2:
				if os.Rename(other, path) != nil || os.Chmod(path, 0644) != nil {
					t.Fatal("mode fixture")
				}
			case 3:
				if syscall.Mkfifo(path, 0600) != nil {
					t.Fatal("FIFO fixture")
				}
			case 4:
				if os.Mkdir(path, 0700) != nil {
					t.Fatal("directory request fixture")
				}
			}
			executionSeedControlRefused(t, f.original(t, executionSeedProcessOptions{}))
			if _, e := os.Lstat(filepath.Join(f.request.ScratchParent, executionSeedTargetName)); !os.IsNotExist(e) {
				t.Fatal("negative request caused target effects")
			}
		}
		for _, name := range []string{"private-bootstrap.json", "fixed-binding.json"} {
			f := executionSeedControlNew(t, 0)
			f.build(t)
			target := filepath.Join(f.request.ScratchParent, executionSeedTargetName)
			path := filepath.Join(target, name)
			if os.Rename(path, path+".retained") != nil || os.Symlink(name+".retained", path) != nil {
				t.Fatal("negative asset symlink fixture")
			}
			probe := &executionSeedFS{clock: func() time.Time { return time.Now().UTC() }}
			if _, e := executionSeedReadAssetsUsing(f.parent, f.request, probe); e == nil {
				t.Fatal("symlink asset accepted")
			}
			for _, h := range probe.handles {
				if !h.attempted {
					t.Fatal("failed asset branch omitted registered close")
				}
			}
		}
	})
	t.Run("expired-positive-window", func(t *testing.T) {
		f := executionSeedControlNew(t, 0)
		s, _ := owneddata.Time(f.request.SampledAt, true)
		var e error
		cap := s.Add(time.Hour)
		f.request, e = executionSeedRequestFor(f.request.ScratchParent, f.request.WaypointAuthorityID, f.request.ProjectID, f.request.ScopeID, f.request.OwnerEpoch, s.Add(-11*time.Second), cap, cap, cap)
		if e != nil {
			t.Fatal("fixed expired request construction")
		}
		// Use the exact expired request bytes in a real child; parent admission
		// Prepare is deliberately not used to pre-refuse this negative probe.
		raw, _ := executionSeedEncode(f.request, 8192)
		path := filepath.Join(f.request.ScratchParent, executionSeedRequestName)
		if os.WriteFile(path, raw, 0600) != nil {
			t.Fatal("expired negative request fixture")
		}
		f.end = time.Now().Add(2 * time.Second)
		executionSeedControlRefused(t, f.original(t, executionSeedProcessOptions{}))
		if _, e := os.Lstat(filepath.Join(f.request.ScratchParent, executionSeedTargetName)); !os.IsNotExist(e) {
			t.Fatal("expired request created target")
		}
		g := executionSeedControlNew(t, 0)
		sample, _ := owneddata.Time(g.request.SampledAt, true)
		now := sample
		g.fs.clock = func() time.Time { return now }
		g.fs.fault = func(point string) error {
			if point == "before-write" {
				now = g.end
			}
			return nil
		}
		if _, e := g.fs.build(g.parent, g.request); e == nil || !g.fs.created {
			t.Fatal("post-blocking expired partial accepted")
		}
		g.fs.fault = nil
		executionSeedControlFinished(t, g.fs)
	})
	t.Run("closed-output-held", func(t *testing.T) {
		f := executionSeedControlNew(t, 0)
		o := f.original(t, executionSeedProcessOptions{closedOutput: true})
		if !o.Waited || o.Accepted || o.ExitCode == 0 {
			t.Fatal("closed output accepted or original Wait unknown")
		}
		g := executionSeedControlNew(t, 0)
		o = g.original(t, executionSeedProcessOptions{finalCloseFault: true})
		if !o.Waited || !o.Complete || o.ReplyCode != "SEEDED" || o.ExitCode != 0 || o.ParentClosed || o.Accepted {
			t.Fatal("after-output negative close acknowledgement promoted")
		}
	})
	t.Run("blocked-output-deadline", func(t *testing.T) {
		// A real original grant cap narrows this negative fixture's seed window;
		// no source deadline or default session budget is extended/reset.
		f := executionSeedControlNew(t, 2)
		o := f.original(t, executionSeedProcessOptions{blockedOutput: true})
		if o.Accepted || !o.Waited || !o.Child.WaitKnown() {
			t.Fatal("blocked original output did not remain bounded and held")
		}
	})
	t.Run("partial-write-held", func(t *testing.T) {
		f := executionSeedControlNew(t, 0)
		f.fs.fault = func(point string) error {
			if point == "before-write" {
				return executionSessionProtocolError
			}
			return nil
		}
		if _, e := f.fs.build(f.parent, f.request); e == nil || !f.fs.created {
			t.Fatal("injected partial construction was accepted")
		}
		f.fs.fault = nil
		executionSeedControlFinished(t, f.fs)
		if _, e := os.Lstat(filepath.Join(f.request.ScratchParent, executionSeedTargetName)); e != nil {
			t.Fatal("partial target erased")
		}
	})
	t.Run("checked-close-and-late-wait-held", func(t *testing.T) {
		// A real private descriptor is already closed, so the driver's actual
		// Close returns an error. Its repeat-readable method result may be nil,
		// but the cached uncertainty must retain the same known fixture root.
		retained, e := os.MkdirTemp("", "outfitter-close-retention-")
		if e != nil {
			t.Fatal("known retention fixture parent")
		}
		file, e := os.OpenFile(filepath.Join(retained, "fixed-fixture"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			t.Fatal("private retention descriptor")
		}
		diagnosticDone := make(chan struct{})
		close(diagnosticDone)
		driver := &executionSessionDriver{closed: map[*os.File]bool{}, seedSetupOK: true, stderrDone: diagnosticDone}
		if file.Close() != nil || driver.closeFile(file) == nil {
			t.Fatal("actual private close failure was not exercised")
		}
		if driver.closeFile(file) != nil {
			t.Fatal("shared once-only close replay contract changed")
		}
		driver.mu.Lock()
		uncertain := driver.closeErr != nil
		driver.mu.Unlock()
		if disposed, e := driver.disposeSeedFixture(retained); !uncertain || disposed || e != nil {
			t.Fatal("cached close uncertainty allowed fixture disposal")
		}
		if _, e := os.Lstat(filepath.Join(retained, "fixed-fixture")); e != nil {
			t.Fatal("uncertain close fixture was erased")
		}
		// A pending diagnostic reader independently blocks disposal. Only its
		// actual completion broadcast permits disposal of this separate known
		// empty test fixture; neither observation is a core ownership receipt.
		pending, e := os.MkdirTemp("", "outfitter-diagnostic-retention-")
		if e != nil {
			t.Fatal("known diagnostic fixture parent")
		}
		done := make(chan struct{})
		clean := &executionSessionDriver{closed: map[*os.File]bool{}, seedSetupOK: true, stderrDone: done}
		if disposed, e := clean.disposeSeedFixture(pending); disposed || e != nil {
			t.Fatal("pending diagnostic reader allowed fixture disposal")
		}
		if _, e := os.Lstat(pending); e != nil {
			t.Fatal("pending diagnostic fixture was erased")
		}
		close(done)
		if disposed, e := clean.disposeSeedFixture(pending); !disposed || e != nil {
			t.Fatal("known completed fixture could not be disposed")
		}
		if _, e := os.Lstat(pending); !os.IsNotExist(e) {
			t.Fatal("known fixture disposal was not observed")
		}
		f := executionSeedControlNew(t, 0)
		f.build(t)
		f.expectedCloseUncertain = true
		f.fs.fault = func(point string) error {
			if point == "after-close" {
				return executionSessionProtocolError
			}
			return nil
		}
		if f.fs.finish() == nil || f.fs.finish() == nil {
			t.Fatal("negative close acknowledgement retried as success")
		}
		f.fs.fault = nil
		executionSeedControlFinished(t, f.fs)
		bindingFixture := executionSeedControlNew(t, 0)
		bindingFixture.build(t)
		if os.WriteFile(filepath.Join(bindingFixture.request.ScratchParent, executionSeedTargetName, "fixed-binding.json"), []byte("{}\n"), 0600) != nil {
			t.Fatal("binding mismatch fixture")
		}
		probe := &executionSeedFS{clock: func() time.Time { return time.Now().UTC() }}
		if _, e := executionSeedReadAssetsUsing(bindingFixture.parent, bindingFixture.request, probe); e == nil {
			t.Fatal("binding mismatch accepted")
		}
		for _, h := range probe.handles {
			if !h.attempted {
				t.Fatal("binding mismatch omitted registered close")
			}
		}
		// The actual stdio files close, then a fixed negative seam advances the
		// child clock. Complete-looking output cannot rescue its late exit.
		for mode := 0; mode < 2; mode++ {
			sample := time.Now().UTC()
			now := sample
			s := &executionSeedFS{clock: func() time.Time { return now }, start: sample, end: sample.Add(time.Second)}
			outR, outW, e := os.Pipe()
			if e != nil {
				t.Fatal("private publisher output")
			}
			input, e := os.Open("/dev/null")
			if e != nil {
				_ = outR.Close()
				_ = outW.Close()
				t.Fatal("private publisher input")
			}
			errR, errW, e := os.Pipe()
			if e != nil {
				_ = input.Close()
				_ = outR.Close()
				_ = outW.Close()
				t.Fatal("private publisher error")
			}
			s.fault = func(point string) error {
				if point == "after-stdio-close" {
					if mode == 0 {
						now = s.end
					} else {
						now = sample.Add(-time.Microsecond)
					}
				}
				return nil
			}
			code := executionSeedPublishFiles(s, "SEEDED", s.end, [3]*os.File{input, outW, errW})
			b, re := io.ReadAll(io.LimitReader(outR, 257))
			eb, ee := io.ReadAll(io.LimitReader(errR, 4097))
			c1, c2 := outR.Close(), errR.Close()
			if code == 0 || re != nil || ee != nil || c1 != nil || c2 != nil || len(eb) != 0 || !executionSeedResultRead(b) {
				t.Fatal("post-close expiry/rollback was promoted")
			}
		}
		// Delay only publication of the SAME original child's actual Wait. The
		// captured two-second negative budget expires; no new Wait or PID lookup
		// is permitted. Releasing this gate is a later observation, not admission.
		g := executionSeedControlNew(t, 2)
		g.expectedUnknown = true
		gate := make(chan struct{})
		returned := make(chan struct{})
		released := false
		t.Cleanup(func() {
			if !released {
				close(gate)
			}
		})
		o := g.original(t, executionSeedProcessOptions{waitPublication: gate, waitReturned: returned})
		if !o.TimedOut || o.Accepted || o.Waited || o.Child == nil || o.Child.WaitKnown() {
			t.Fatal("pending original Wait publication was accepted")
		}
		select {
		case <-returned:
		default:
			t.Fatal("late-wait control never reached actual original Wait")
		}
		if o.Child.Finish() {
			t.Fatal("expired negative budget renewed Wait observation")
		}
		close(gate)
		released = true
		// No positive result/asset acceptance is performed after this release.
	})
	t.Run("single-shot-no-reseed", func(t *testing.T) {
		f := executionSeedControlNew(t, 0)
		a := f.build(t)
		journal := filepath.Join(a.Config.StateRoot, "ledger.json")
		before, e := os.ReadFile(journal)
		if e != nil {
			t.Fatal("initial fixture journal")
		}
		if _, e = f.fs.build(f.parent, f.request); !errors.Is(e, syscall.EEXIST) {
			t.Fatal("fresh target was reseeded or repaired")
		}
		after, e := os.ReadFile(journal)
		if e != nil || !bytes.Equal(before, after) {
			t.Fatal("reseed refusal changed journal")
		}
	})
}
