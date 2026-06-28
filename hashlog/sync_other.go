//go:build !linux && !darwin

package hashlog

import "os"

// platformSyncData falls back to os.File.Sync on platforms where neither fdatasync
// nor F_FULLFSYNC is the right call. Its barrier is whatever the platform's fsync
// gives, which on some systems is weaker than a true device flush; the durability
// dial's honesty carries that caveat the same way doc 04 section 8 documents it.
func platformSyncData(f *os.File) error {
	return f.Sync()
}
