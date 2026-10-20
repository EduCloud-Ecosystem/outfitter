package broker

import (
	"os"
	"syscall"
	"testing"
	"unsafe"
)

func TestOwnedPipeDarwinSyscallWitness(t *testing.T) {
	r, w, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	defer w.Close()
	observe := func(name string, f *os.File, wantPipe bool) {
		t.Helper()
		raw, e := f.SyscallConn()
		if e != nil {
			t.Fatal(e)
		}
		for _, size := range []int{184, 183} {
			var info [184]byte
			var n uintptr
			var errno syscall.Errno
			var st syscall.Stat_t
			var statErr error
			e = raw.Control(func(fd uintptr) {
				statErr = syscall.Fstat(int(fd), &st)
				// Self-only PROC_INFO_CALL_PIDFDINFO / PROC_PIDFDPIPEINFO.
				n, _, errno = syscall.Syscall6(syscall.SYS_PROC_INFO, 3, uintptr(os.Getpid()), 6, fd, uintptr(unsafe.Pointer(&info[0])), uintptr(size))
			})
			if e != nil || statErr != nil {
				t.Fatal(e, statErr)
			}
			t.Logf("%s: mode=%#o device=%d inode=%d nlink=%d query_size=%d returned_size=%d errno=%d", name, st.Mode, st.Dev, st.Ino, st.Nlink, size, n, errno)
			if wantPipe && size == len(info) {
				if errno != 0 || n != uintptr(len(info)) {
					t.Fatal("anonymous pipe type query failed")
				}
			} else if errno == 0 {
				t.Fatal("FIFO or incomplete buffer type query accepted")
			}
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
