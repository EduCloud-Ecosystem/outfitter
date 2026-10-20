//go:build (darwin || linux) && (amd64 || arm64)

package ledger

import (
	"io"
	"os"
	"syscall"
	"testing"
)

func TestExecutionAtomicWatchDuplicateCloseOnExec(t *testing.T) {
	t.Run("private-independent-duplicate", func(t *testing.T) {
		r, w, e := os.Pipe()
		if e != nil {
			t.Fatal(e)
		}
		defer r.Close()
		duplicate, e := assessmentDupCloseOnExec(w)
		if e != nil {
			_ = w.Close()
			t.Fatal(e)
		}
		defer duplicate.Close()
		raw, e := duplicate.SyscallConn()
		if e != nil {
			t.Fatal(e)
		}
		var flags uintptr
		var nativeErr syscall.Errno
		if e = raw.Control(func(fd uintptr) {
			flags, _, nativeErr = syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFD, 0)
		}); e != nil || nativeErr != 0 || flags&syscall.FD_CLOEXEC == 0 {
			t.Fatal("duplicate was not atomically private", e, nativeErr)
		}
		if e = w.Close(); e != nil {
			t.Fatal(e)
		}
		if _, e = duplicate.Write([]byte{1}); e != nil {
			t.Fatal("parent close released distinct child duplicate", e)
		}
		var b [1]byte
		if _, e = io.ReadFull(r, b[:]); e != nil || b[0] != 1 {
			t.Fatal("duplicate did not retain the original pipe", e)
		}
	})
	t.Run("closed-original-refuses", func(t *testing.T) {
		r, w, e := os.Pipe()
		if e != nil {
			t.Fatal(e)
		}
		defer r.Close()
		if e = w.Close(); e != nil {
			t.Fatal(e)
		}
		duplicate, e := assessmentDupCloseOnExec(w)
		if duplicate != nil || e == nil {
			t.Fatal("closed original accepted a new duplicate")
		}
	})
}
