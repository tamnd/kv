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
