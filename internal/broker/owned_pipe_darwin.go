package broker

import (
	"os"
	"syscall"
	"unsafe"
)

func anonymousPipe(f *os.File, st *syscall.Stat_t) bool {
	// Zero links alone also matches an unlinked filesystem FIFO. Query only
	// this process's opened descriptor: XNU's PROC_PIDFDPIPEINFO requires
	// DTYPE_PIPE, while a filesystem FIFO is a DTYPE_VNODE even after unlink.
	// XNU f6217f891ac0bb64f3d375211650a4c1ff8ca1ea:
	// bsd/sys/proc_info.h and bsd/kern/proc_info.c (proc_pidfdinfo).
	const (
		procInfoCallPIDFDInfo = 3
		procPIDFDPipeInfo     = 6
		// proc_fileinfo (24) + pipe_info (160), the fixed 64-bit Darwin ABI.
		pipeFDInfoSize = 184
	)
	if st.Nlink != 0 {
		return false
	}
	raw, e := f.SyscallConn()
	if e != nil {
		return false
	}
	valid := false
	e = raw.Control(func(fd uintptr) {
		var info [pipeFDInfoSize]byte
		n, _, errno := syscall.Syscall6(syscall.SYS_PROC_INFO,
			procInfoCallPIDFDInfo, uintptr(os.Getpid()), procPIDFDPipeInfo, fd,
			uintptr(unsafe.Pointer(&info[0])), uintptr(len(info)))
		valid = errno == 0 && n == uintptr(len(info))
	})
	return e == nil && valid
}
