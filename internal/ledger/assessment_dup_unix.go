//go:build (darwin || linux) && (amd64 || arm64)

package ledger

import (
	"os"
	"syscall"
)

// assessmentDupCloseOnExec is the v7 watch constructor's private atomic seam.
// Control pins the original file without Fd changing its polling mode. A
// nonnil partial result remains owned even if the outer control reports error.
func assessmentDupCloseOnExec(source *os.File) (*os.File, error) {
	if source == nil {
		return nil, os.ErrInvalid
	}
	raw, e := source.SyscallConn()
	if e != nil {
		return nil, e
	}
	var duplicate *os.File
	var duplicateErr error
	controlErr := raw.Control(func(fd uintptr) {
		n, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, uintptr(syscall.F_DUPFD_CLOEXEC), 0)
		if errno != 0 {
			duplicateErr = errno
			return
		}
		duplicate = os.NewFile(n, "v7-watch-write")
		if duplicate == nil {
			duplicateErr = syscall.EBADF
		}
	})
	if controlErr != nil {
		return duplicate, controlErr
	}
	return duplicate, duplicateErr
}
