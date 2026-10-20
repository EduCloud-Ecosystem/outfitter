package broker

import (
	"os"
	"syscall"
	"testing"
)

// Record actual hosted syscall observations with the race-test JSON artifact.
// Cross-compilation alone cannot establish these runtime properties.
func TestOwnedPipeLinuxSyscallWitness(t *testing.T) {
	r, w, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	defer w.Close()
	observe := func(name string, f *os.File, wantPipeFS bool) {
		t.Helper()
		raw, e := f.SyscallConn()
		if e != nil {
			t.Fatal(e)
		}
		var st syscall.Stat_t
		var fs syscall.Statfs_t
		var statErr, fsErr error
		var flags uintptr
		var flagErr syscall.Errno
		e = raw.Control(func(fd uintptr) {
			statErr = syscall.Fstat(int(fd), &st)
			fsErr = syscall.Fstatfs(int(fd), &fs)
			flags, _, flagErr = syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFL, 0)
		})
		if e != nil || statErr != nil || fsErr != nil || flagErr != 0 {
			t.Fatalf("%s: control=%v stat=%v statfs=%v flags=%v", name, e, statErr, fsErr, flagErr)
		}
		t.Logf("%s: mode=%#o device=%d inode=%d nlink=%d filesystem=%#x access=%d", name, st.Mode, st.Dev, st.Ino, st.Nlink, fs.Type, flags&syscall.O_ACCMODE)
		if (fs.Type == 0x50495045) != wantPipeFS {
			t.Fatal("unexpected descriptor filesystem", name)
		}
		if wantPipeFS && st.Nlink != 1 {
			t.Fatal("unexpected Linux anonymous pipe link count", st.Nlink)
		}
	}
	observe("anonymous-read", r, true)
	observe("anonymous-write", w, true)
	for _, stage := range []string{"named", "renamed", "unlinked"} {
		fr, fw := ownedTestFIFO(t, stage)
		observe(stage+"-fifo-read", fr, false)
		observe(stage+"-fifo-write", fw, false)
	}
}
