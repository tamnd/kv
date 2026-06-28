//go:build !unix

package f2

import "os"

// lockFile is a no-op where flock is not available. The single-writer guarantee
// then rests on the operator, the same caveat the platform-fsync fallback
// carries. unlockFile is correspondingly empty.
func lockFile(f *os.File) error { return nil }

func unlockFile(f *os.File) {}
