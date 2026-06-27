//go:build darwin

package hashlog

import (
	"os"
	"syscall"
)

// fFullFsync is macOS F_FULLFSYNC. A plain fsync on macOS (what os.File.Sync calls)
// flushes the OS buffer cache to the drive but not the drive's own volatile cache, so
// a power loss can still lose the data. F_FULLFSYNC forces the drive to flush its
// cache to stable storage, which is the true barrier the frontier's promise rests on.
const fFullFsync = 51

// platformSyncData issues F_FULLFSYNC so the durable frontier advances only after the
// drive has actually persisted the bytes (D14). Without it a macOS Full-dial store
// would believe acknowledged writes are durable when a power loss could still lose
// them, which would make the frontier a lie. Pure Go via the syscall package, no cgo.
func platformSyncData(f *os.File) error {
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), uintptr(fFullFsync), 0)
	if errno != 0 {
		return errno
	}
	return nil
}
