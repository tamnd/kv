//go:build linux

package f2

import (
	"os"
	"syscall"
)

// platformSyncData flushes the file's data and the metadata needed to read it
// back (the size) with fdatasync. It skips the inode metadata a full fsync also
// flushes, which durability does not need, so it is the cheaper sufficient
// barrier. Go's os.File.Sync calls fsync, not fdatasync, so the durable syncs go
// through this helper directly via the syscall package, pure Go with no cgo.
func platformSyncData(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}
