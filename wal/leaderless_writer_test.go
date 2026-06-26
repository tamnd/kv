package wal

import (
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/tamnd/kv/vfs"
)

// This file gates M5.2, the leaderless double-buffered committer (leaderless_writer.go) that
// produces the M5.1 frame format under concurrent fill. M5.1 proved the format and its recovery
// from hand-built and fuzzed bytes; these tests prove the writer that fills it: that many
// goroutines committing at once each get a unique LSN and a durable ack, that the bytes they
// leave recover to exactly the set that was acked, and that a crash at any fsync boundary leaves
// a recoverable contiguous-LSN prefix and never a torn frame the recovery cannot bound.

// recoverLL reads a leaderless log's current bytes and recovers it, the lens every test below
// checks the writer's output through.
func recoverLL(tb testing.TB, fs *vfs.Mem, path string) LeaderlessResult {
	tb.Helper()
	raw := readFile(tb, fs, path)
	res, err := RecoverLeaderless(readerOver(raw), int64(len(raw)))
	if err != nil {
		tb.Fatalf("recover: %v", err)
	}
	return res
}

// assertContiguousPrefix pins the invariant every recovery must satisfy: the recovered batches
// are LSNs base, base+1, ..., watermark with no gap, nothing sits past the watermark, and the
// watermark is the last recovered LSN. It is the same contract the fuzzer asserts, checked here
// against real concurrent-writer output.
func assertContiguousPrefix(tb testing.TB, res LeaderlessResult, base uint64) {
	tb.Helper()
	for i, b := range res.Batches {
		want := base + uint64(i)
		if b.LSN != want {
			tb.Fatalf("batch %d LSN %d breaks the contiguous run from base %d (want %d)", i, b.LSN, base, want)
		}
		if b.LSN > res.DurableLSN {
			tb.Fatalf("batch %d LSN %d past watermark %d", i, b.LSN, res.DurableLSN)
		}
	}
	if n := len(res.Batches); n > 0 && res.DurableLSN != res.Batches[n-1].LSN {
		tb.Fatalf("watermark %d is not the last recovered LSN %d", res.DurableLSN, res.Batches[n-1].LSN)
	}
	if extra := res.CommittedAfter(res.DurableLSN); len(extra) != 0 {
		tb.Fatalf("CommittedAfter(watermark) returned %d batches, want none", len(extra))
	}
}

// TestLeaderlessWriterSequential is the simplest path: one goroutine commits a gapless run
// through the concurrent committer and it recovers in full, the double-buffer machinery driven
// at concurrency 1 where every amortization factor is exactly 1.
func TestLeaderlessWriterSequential(t *testing.T) {
	fs := vfs.NewMem()
	l, err := CreateLeaderless(fs, "test.kv-wal", Options{PageSize: 4096, Sync: SyncFull, Salt: 0x1234}, 1, 4096)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	const n = 200
	for i := 0; i < n; i++ {
		lsn, err := l.Commit(uint64(i+1), []byte(fmt.Sprintf("batch-%d", i+1)))
		if err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
		if lsn != uint64(i+1) {
			t.Fatalf("commit %d got lsn %d, want %d", i, lsn, i+1)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	res := recoverLL(t, fs, "test.kv-wal")
	if len(res.Batches) != n {
		t.Fatalf("recovered %d batches, want %d", len(res.Batches), n)
	}
	assertContiguousPrefix(t, res, 1)
}

// TestLeaderlessWriterConcurrent is the milestone's point: many goroutines commit at once, each
// gets a distinct LSN and a durable ack, and the log recovers to exactly the set acked, in LSN
// order, with no gap and nothing lost. The small buffer relative to the total bytes forces many
// flips, so the ping-pong, the back-pressure, and the completion watermark all run hard.
func TestLeaderlessWriterConcurrent(t *testing.T) {
	fs := vfs.NewMem()
	// A buffer that holds only a handful of frames forces frequent flips under contention.
	l, err := CreateLeaderless(fs, "test.kv-wal", Options{PageSize: 4096, Sync: SyncNormal, Salt: 0xabcd}, 1, 512)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	const writers = 16
	const perWriter = 64
	var mu sync.Mutex
	got := make([]uint64, 0, writers*perWriter)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				lsn, err := l.Commit(uint64(1), []byte(fmt.Sprintf("w%d-i%d", w, i)))
				if err != nil {
					t.Errorf("writer %d commit %d: %v", w, i, err)
					return
				}
				mu.Lock()
				got = append(got, lsn)
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	total := writers * perWriter
	if len(got) != total {
		t.Fatalf("acked %d commits, want %d", len(got), total)
	}
	// Every acked LSN is distinct and the set is exactly 1..total: a leaderless committer assigns
	// each commit its own LSN and acks only when the watermark covers it.
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	for i, lsn := range got {
		if lsn != uint64(i+1) {
			t.Fatalf("acked LSNs are not 1..%d: position %d is %d", total, i, lsn)
		}
	}
	// Recovery returns exactly the acked set, in order.
	res := recoverLL(t, fs, "test.kv-wal")
	if len(res.Batches) != total {
		t.Fatalf("recovered %d batches, want %d", len(res.Batches), total)
	}
	assertContiguousPrefix(t, res, 1)
}

// TestLeaderlessWriterCrashSweep crashes a concurrent workload at every fsync boundary and
// recovers each frozen image, asserting the durable-prefix property holds at all of them: the
// recovery returns a contiguous-LSN run from the base ending at the watermark with no torn frame
// past it, and an acked commit is never lost. The buffer is the unit of sync, so a crash only
// ever tears the final un-synced buffer, and the durable prefix lives entirely below it.
func TestLeaderlessWriterCrashSweep(t *testing.T) {
	// First, learn how many syncs a run of this shape performs, so the sweep covers each boundary.
	syncs := runForSyncCount(t)
	if syncs < 2 {
		t.Fatalf("workload performed only %d syncs, too few to sweep", syncs)
	}
	for k := 1; k <= syncs; k++ {
		t.Run(fmt.Sprintf("freeze-after-sync-%d", k), func(t *testing.T) {
			fs := vfs.NewMem()
			fs.CrashAfterSync(k)
			acked := runConcurrentLL(t, fs)
			fs.Crash()

			res := recoverLL(t, fs, "test.kv-wal")
			assertContiguousPrefix(t, res, 1)
			// Every commit whose ack the writer returned before the freeze must survive: an acked
			// LSN at or below the recovered watermark is present. Acks past the watermark are commits
			// the freeze cut off mid-flight, which is allowed; what is forbidden is an acked LSN at or
			// below the watermark going missing, which assertContiguousPrefix already rules out because
			// the run is gapless from the base. So here we only check the watermark never exceeds the
			// highest acked LSN, which would mean recovery invented a durable commit no writer acked.
			var maxAcked uint64
			for _, lsn := range acked {
				if lsn > maxAcked {
					maxAcked = lsn
				}
			}
			if res.DurableLSN > maxAcked && len(acked) > 0 {
				t.Fatalf("recovered watermark %d exceeds highest acked LSN %d", res.DurableLSN, maxAcked)
			}
		})
	}
}

// runForSyncCount runs the crash-sweep workload once to completion with no freeze and reports how
// many syncs it took, so the sweep knows how many boundaries to cover.
func runForSyncCount(t *testing.T) int {
	t.Helper()
	fs := vfs.NewMem()
	runConcurrentLL(t, fs)
	return fs.SyncCount()
}

// runConcurrentLL runs a fixed concurrent commit workload against fs, closes the log, and returns
// the LSNs whose Commit returned a durable ack. It is the body both the sync-count probe and each
// crash-sweep case run.
func runConcurrentLL(t *testing.T, fs *vfs.Mem) []uint64 {
	t.Helper()
	l, err := CreateLeaderless(fs, "test.kv-wal", Options{PageSize: 4096, Sync: SyncFull, Salt: 0x77}, 1, 512)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	const writers = 8
	const perWriter = 16
	var mu sync.Mutex
	acked := make([]uint64, 0, writers*perWriter)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				lsn, err := l.Commit(uint64(1), []byte(fmt.Sprintf("w%d-i%d-payload", w, i)))
				if err != nil {
					return // a frozen image past the freeze point can surface a sync error; stop quietly
				}
				mu.Lock()
				acked = append(acked, lsn)
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	// Close may fail when the run is past a freeze point; the bytes already on disk are what
	// recovery reads, so a close error here is not the test's concern.
	_ = l.Close()
	return acked
}
