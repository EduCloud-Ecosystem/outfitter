//go:build !darwin && !linux

package broker

import (
	"os"
	"syscall"
)

// No anonymous-pipe qualification exists for other platforms.
func anonymousPipe(_ *os.File, _ *syscall.Stat_t) bool { return false }
