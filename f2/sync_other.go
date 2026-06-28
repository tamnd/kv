//go:build !linux && !darwin

package f2

import "os"

// platformSyncData falls back to os.File.Sync where neither fdatasync nor
// F_FULLFSYNC is the right call. Its barrier is whatever the platform's fsync
// gives, which on some systems is weaker than a true device flush; the durability
// dial carries that caveat the same way hashlog does.
func platformSyncData(f *os.File) error {
	return f.Sync()
}
