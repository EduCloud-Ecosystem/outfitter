package broker

import (
	"os"
	"syscall"
)

func anonymousPipe(f *os.File, st *syscall.Stat_t) bool {
	// Linux anonymous pipes have one link. A named FIFO has the same file
	// kind/link count, so require the opened descriptor's kernel pipefs too.
	// PIPEFS_MAGIC: Linux include/uapi/linux/magic.h; fs/pipe.c creates this
	// internal filesystem, which cannot be mounted by userspace.
	const pipeFSMagic = 0x50495045
	if st.Nlink != 1 {
		return false
	}
	raw, e := f.SyscallConn()
	if e != nil {
		return false
	}
	valid := false
	e = raw.Control(func(fd uintptr) {
		var fs syscall.Statfs_t
		valid = syscall.Fstatfs(int(fd), &fs) == nil && fs.Type == pipeFSMagic
	})
	return e == nil && valid
}
