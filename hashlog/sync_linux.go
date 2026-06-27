//go:build linux

package hashlog

import (
	"os"
	"syscall"
)

// platformSyncData flushes the file's data and the metadata needed to read it back
// (the size) to the device with fdatasync (D14). It skips the inode metadata that a
// full fsync also flushes, which the durable frontier does not need, so it is the
// cheaper sufficient barrier. Go's os.File.Sync calls fsync, not fdatasync, so the
// frontier's syncs go through this helper directly via the syscall package, pure Go
// with no cgo and no dependency.
func platformSyncData(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}
