//go:build darwin

package vfs

import (
	"os"
	"syscall"
)

// fcntlBarrierFsync is the macOS F_BARRIERFSYNC command (sys/fcntl.h). It issues
// an I/O barrier to the drive without the full cache flush of F_FULLFSYNC: writes
// before it are ordered ahead of writes after it and survive a process or kernel
// crash, but the bytes are not guaranteed onto stable media before it returns, so
// a power loss can still lose them. That is exactly the SyncBarrier contract.
const fcntlBarrierFsync = 85

// barrierSync issues F_BARRIERFSYNC on f. Some filesystems (network mounts, a few
// older ones) do not support the barrier and report ENOTSUP/EINVAL; there we fall
// back to a full fsync, which is stronger, so the call is never weaker than asked.
func barrierSync(f *os.File) error {
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), fcntlBarrierFsync, 0)
	switch errno {
	case 0:
		return nil
	case syscall.ENOTSUP, syscall.EINVAL:
		return f.Sync()
	default:
		return errno
	}
}

// dataSync is the SyncData primitive on macOS. There is no fdatasync here, and a
// plain fsync does not push the drive's own write cache to stable media, so the only
// call that makes data durable across power loss is F_FULLFSYNC, which os.File.Sync
// issues on darwin. SyncData and SyncFull therefore reach the same primitive on this
// platform: the data-versus-metadata split that lets fdatasync skip the inode write
// exists only on Linux, so there is nothing cheaper to choose here.
func dataSync(f *os.File) error { return f.Sync() }
