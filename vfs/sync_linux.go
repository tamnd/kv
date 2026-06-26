//go:build linux

package vfs

import (
	"os"
	"syscall"
)

// barrierSync issues fdatasync on f, the Linux analogue of the macOS barrier
// level: it flushes the file's data and the metadata needed to read it back, but
// skips unrelated inode metadata, so it is cheaper than a full fsync. It is
// durable across a crash and, on a correctly behaving device, across power loss
// too, so it is never weaker than the SyncBarrier contract promises.
func barrierSync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}

// dataSync issues fdatasync on f, the SyncData primitive on Linux. It flushes the
// file's data and only the metadata a reader needs to get it back (the size, when
// the file grew), and skips unrelated inode metadata such as mtime, so it is cheaper
// than the full fsync that backs SyncFull. This is the WAL's normal flush: an append
// has to be durable but the log file's timestamps never matter, so paying for the
// inode write fsync forces would be waste on the hot path. It is durable across power
// loss on a correctly behaving device.
func dataSync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}
