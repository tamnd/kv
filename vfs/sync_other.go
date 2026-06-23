//go:build !darwin && !linux

package vfs

import "os"

// barrierSync has no cheaper barrier primitive on this platform, so it falls back
// to a full fsync. That satisfies the SyncBarrier contract because a full flush is
// strictly stronger than an ordering barrier.
func barrierSync(f *os.File) error { return f.Sync() }
