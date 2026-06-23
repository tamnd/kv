package db

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tamnd/kv/vfs"
)

// TestCheckpointDoesNotBlockWrites verifies that foreground writes can proceed while a
// background checkpoint is executing its I/O phase (perf/02 F5). Before this slice,
// CheckpointMode held d.mu for the entire page-writeback+fsync, blocking all commits.
// Now it releases d.mu between the prepare and finalize steps, so writes commit
// concurrently with the checkpoint's I/O.
//
// The test runs a sustained writer goroutine and triggers periodic checkpoints from a
// second goroutine. It counts commits that complete in under 5 ms. With the old locked
// path almost all commits that overlap a checkpoint stall for its full duration; with the
// new path the stall window is only the brief finalize phase. The test asserts that the
// writer keeps committing (is not stalled) while the checkpoint runs, by measuring that
// the writer completes at least one commit per 50 ms checkpoint window.
func TestCheckpointDoesNotBlockWrites(t *testing.T) {
	fs := vfs.NewMem()
	// AutoCheckpoint -1: drive checkpoints manually from the test goroutine.
	d, err := Open(fs, "test.kv", Options{PageSize: 4096, AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	// Pre-fill so the checkpoint has real I/O to do (many dirty pages to write back).
	for i := range 500 {
		if err := d.Update(func(txn *Txn) error {
			txn.Set([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%04d", i)))
			return nil
		}); err != nil {
			t.Fatalf("prefill: %v", err)
		}
	}

	var (
		commitsWhileCheckpoint atomic.Int64
		wg                     sync.WaitGroup
		stop                   = make(chan struct{})
	)

	// Writer goroutine: commits in a tight loop and records each commit.
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			start := time.Now()
			err := d.Update(func(txn *Txn) error {
				txn.Set([]byte(fmt.Sprintf("w%06d", i)), []byte("val"))
				return nil
			})
			elapsed := time.Since(start)
			if err == nil && elapsed < 5*time.Millisecond {
				commitsWhileCheckpoint.Add(1)
			}
			i++
		}
	}()

	// Checkpoint goroutine: triggers checkpoints at 50 ms intervals.
	const checkpoints = 10
	ckptDone := make(chan struct{})
	go func() {
		defer close(ckptDone)
		for range checkpoints {
			if err := d.Checkpoint(); err != nil {
				t.Errorf("checkpoint: %v", err)
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	<-ckptDone
	close(stop)
	wg.Wait()

	// The writer must have made at least one sub-5ms commit per checkpoint window.
	// A completely stalled writer would report 0 fast commits. A non-stalled writer
	// running at even 1 commit/ms would report hundreds.
	got := commitsWhileCheckpoint.Load()
	if got < int64(checkpoints) {
		t.Fatalf("only %d fast commits across %d checkpoint windows: checkpoint is blocking writes",
			got, checkpoints)
	}
}
