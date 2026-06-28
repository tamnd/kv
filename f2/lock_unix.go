//go:build unix

package f2

import (
	"os"
	"syscall"
)

// lockFile takes an advisory exclusive lock on the open file so a second process
// cannot open the same store and corrupt it by writing the superblock and
// appending blocks concurrently. The lock is tied to the open file description,
// so closing the file releases it; an explicit unlock on Close is belt and
// braces. LOCK_NB makes a held lock fail fast rather than block forever.
func lockFile(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return errLocked
	}
	return nil
}

func unlockFile(f *os.File) {
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
