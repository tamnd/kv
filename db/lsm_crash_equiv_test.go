package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
	"github.com/tamnd/kv/wal"
)

// This file closes M4 by proving the LSM core is crash-equivalent to the reference
// model. The other crash tests pin a single recovery window; this one sweeps a crash
// across every fsync boundary of one continuous workload and, at each one, checks the
// recovered LSM against the conformance Oracle (engine.NewOracle), the same
// deterministic model the no-crash conformance suite checks every engine against.
//
// The guarantee is the durable-prefix property stated as model equivalence: however the
// crash falls, the recovered database is byte-for-byte what the model produces from some
// committed prefix of the workload's batches, the prefix only grows as the crash moves
// later, and the final boundary recovers the whole workload. A flush half-written, a
// compaction's merged segments durable but its manifest not, a GC that reclaimed a
// version the redo still needs: any of those would surface here as a recovered state no
// prefix of the model can explain.

const (
	ceSteps = 48 // committed batches the workload drives
	ceChurn = 8  // distinct churn keys the overwrites and deletes cycle over
	ceEvery = 8  // run Maintain and Checkpoint every this many steps
)

// ceMarker names step i's marker key. The markers ascend with i and are only ever set,
// never deleted, so the highest marker present in a recovered database names the length
// of the committed prefix it holds. That is what makes the recovered state's prefix
// unambiguous even though the churn keys are overwritten and deleted repeatedly.
func ceMarker(i int) string { return fmt.Sprintf("mk%04d", i) }

// ceChurnKey names the churn key step i mutates. They cycle over a small set, so each
// one collects many superseded versions and tombstones across the workload, which is
// what gives compaction segments to merge and GC versions to reclaim at the watermark.
func ceChurnKey(i int) string { return fmt.Sprintf("ck%02d", i%ceChurn) }

// applyCrashStep writes step i into b: the ascending marker that witnesses the prefix,
// plus one churn-key mutation, mostly an overwrite tagged with i and periodically a
// delete. The two keys are always distinct, so a batch never resolves against itself and
// the model applies it at one version exactly as the database commits it at one version.
func applyCrashStep(b *engine.WriteBatch, i int) {
	b.Set([]byte(ceMarker(i)), []byte("."))
	ck := []byte(ceChurnKey(i))
	if i%5 == 4 {
		b.Delete(ck)
	} else {
		b.Set(ck, []byte(fmt.Sprintf("c%02d#%04d", i%ceChurn, i)))
	}
}

// crashModelStates replays the workload through the conformance Oracle and returns the
// visible state after each prefix length 0..ceSteps, each as a canonical sorted slice of
// "key=value" strings read at a snapshot above every commit. states[j] is what a correct
// engine must hold after committing exactly the first j batches, with old versions and
// reclaimed tombstones invisible at the latest snapshot, which is precisely what GC may
// drop without changing the answer. The Oracle's absolute versions need not match the
// database's: read at the latest snapshot, the visible state depends only on the order of
// versions within each key, and the workload commits the batches in this same order.
func crashModelStates() [][]string {
	oracle := engine.NewOracle(nil)
	snap := engine.Snapshot{Version: 1 << 40}
	states := make([][]string, ceSteps+1)
	states[0] = canonicalKVs(oracle.Scan(nil, nil, snap))
	for i := 0; i < ceSteps; i++ {
		eb := engine.NewWriteBatch(uint64(i + 1))
		applyCrashStep(eb, i)
		oracle.Apply(eb, eb.Version())
		states[i+1] = canonicalKVs(oracle.Scan(nil, nil, snap))
	}
	return states
}

// canonicalKVs renders an Oracle scan as sorted "key=value" strings. The scan already
// returns ascending by key, so the slice is a stable fingerprint of the visible state a
// recovered database's own scan can be compared against directly.
func canonicalKVs(pairs []engine.KV) []string {
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = string(p.Key) + "=" + string(p.Value)
	}
	return out
}

// runCrashEquivWorkload drives the whole workload through d: every step a SyncFull
// commit, and every ceEvery steps a Maintain (compaction and GC at the watermark) and a
// Checkpoint that folds the flushed segments and the MANIFEST into the file and advances
// the durable mark. It returns the total pages compacted so the caller can assert the
// workload actually exercised compaction rather than silently degenerating to a memtable
// dribble. It never closes d: a crash is modelled by reverting the filesystem, not by a
// clean shutdown.
func runCrashEquivWorkload(t *testing.T, d *DB) int {
	t.Helper()
	compacted := 0
	for i := 0; i < ceSteps; i++ {
		step := i
		if _, err := d.Write(func(b *engine.WriteBatch) { applyCrashStep(b, step) }); err != nil {
			t.Fatalf("write step %d: %v", i, err)
		}
		if (i+1)%ceEvery == 0 {
			for {
				rep, err := d.Maintain(1 << 30)
				if err != nil {
					t.Fatalf("maintain after step %d: %v", i, err)
				}
				compacted += rep.PagesCompacted
				if rep.PagesCompacted == 0 {
					break
				}
			}
			if err := d.Checkpoint(); err != nil {
				t.Fatalf("checkpoint after step %d: %v", i, err)
			}
		}
	}
	return compacted
}

// writeAllSteps commits every step of the workload through d as its own SyncFull batch
// and does no maintenance, so the flushed segments pile up in L0 untouched. It is what the
// compaction-window test uses to leave a fresh, un-compacted tree for a single settle to
// merge right before the crash, rather than draining compaction as it goes.
func writeAllSteps(t *testing.T, d *DB) {
	t.Helper()
	for i := 0; i < ceSteps; i++ {
		step := i
		if _, err := d.Write(func(b *engine.WriteBatch) { applyCrashStep(b, step) }); err != nil {
			t.Fatalf("write step %d: %v", i, err)
		}
	}
}

// crashEquivOptions is the workload's open options: the LSM core, a memtable small enough
// that the steps flush many segments, SyncFull so every commit is its own fsync boundary,
// and no auto-checkpoint so the only checkpoints are the deterministic ones the workload
// drives and no background goroutine writes after the freeze.
func crashEquivOptions() Options {
	return Options{
		PageSize:       4096,
		Engine:         format.EngineLSM,
		MemtableSize:   64,
		Sync:           wal.SyncFull,
		AutoCheckpoint: -1,
	}
}

// recoverCrashState reopens fs after a crash and returns the visible state as canonical
// "key=value" strings. Because the LSM core has no structural Verify to lean on, it also
// asserts the recovery's own internal consistency: a full reverse scan must be the exact
// reverse of the forward scan, so a recovered index that disagrees with itself fails here
// rather than masquerading as some model prefix.
func recoverCrashState(t *testing.T, fs *vfs.Mem) []string {
	t.Helper()
	d, err := Open(fs, "test.kv", Options{AutoCheckpoint: -1})
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer d.Close()

	fwd := scanCanonical(t, d, engine.IterOptions{})
	rev := scanCanonical(t, d, engine.IterOptions{Reverse: true})
	if len(fwd) != len(rev) {
		t.Fatalf("forward scan has %d pairs, reverse %d", len(fwd), len(rev))
	}
	for i := range fwd {
		if fwd[i] != rev[len(rev)-1-i] {
			t.Fatalf("reverse scan disagrees with forward at %d: %q vs %q", i, rev[len(rev)-1-i], fwd[i])
		}
	}
	return fwd
}

// scanCanonical runs one full scan under opts and returns its pairs as "key=value"
// strings in iteration order.
func scanCanonical(t *testing.T, d *DB, opts engine.IterOptions) []string {
	t.Helper()
	var out []string
	if err := d.View(func(txn *Txn) error {
		it, err := txn.NewIterator(opts)
		if err != nil {
			return err
		}
		defer it.Close()
		keys, vals := collect(t, it)
		for i := range keys {
			out = append(out, keys[i]+"="+vals[i])
		}
		return nil
	}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return out
}

// matchModelPrefix returns the prefix length j whose model state equals got, or -1 if no
// prefix explains the recovered state. A -1 is the headline failure of this slice: the
// engine recovered to something the model never produces from any prefix of the workload.
func matchModelPrefix(states [][]string, got []string) int {
	for j := range states {
		if eq(got, states[j]...) {
			return j
		}
	}
	return -1
}

// TestLSMCrashEquivalentToModelAtEverySyncBoundary is the slice's centrepiece. It first
// runs the workload clean to learn its fsync boundaries and to confirm it really exercised
// compaction, then sweeps a crash across every boundary. At each one it reverts the
// filesystem to that exact fsync, reopens, and asserts the recovered LSM equals the model
// at some committed prefix, that the prefix never shrinks as the crash moves later, and
// that the final boundary recovers the whole workload. The flush, compaction, and GC
// boundaries all fall inside this sweep, since their durability funnels through the WAL
// fsyncs and the checkpoint main-file fsyncs the workload drives.
func TestLSMCrashEquivalentToModelAtEverySyncBoundary(t *testing.T) {
	states := crashModelStates()

	// A clean probe run, never crashed, measures the sync boundaries and proves the
	// workload reaches compaction. Its final state must itself match the full model prefix,
	// a sanity check on the workload before any crash is introduced.
	probe := vfs.NewMem()
	pd, err := Open(probe, "test.kv", crashEquivOptions())
	if err != nil {
		t.Fatalf("open probe: %v", err)
	}
	compacted := runCrashEquivWorkload(t, pd)
	if compacted == 0 {
		t.Fatalf("workload compacted no pages; it is not exercising the compaction and GC boundaries")
	}
	cleanFinal := scanCanonical(t, pd, engine.IterOptions{})
	if !eq(cleanFinal, states[ceSteps]...) {
		t.Fatalf("clean final state does not match the model:\n got  %v\n want %v", cleanFinal, states[ceSteps])
	}
	totalSyncs := probe.SyncCount()
	if err := pd.Close(); err != nil {
		t.Fatalf("close probe: %v", err)
	}
	if totalSyncs < ceSteps {
		t.Fatalf("workload performed %d syncs, want at least one per commit (%d)", totalSyncs, ceSteps)
	}

	prev := 0
	for n := 1; n <= totalSyncs; n++ {
		fs := vfs.NewMem()
		fs.CrashAfterSync(n)
		d, err := Open(fs, "test.kv", crashEquivOptions())
		if err != nil {
			t.Fatalf("open at boundary %d: %v", n, err)
		}
		runCrashEquivWorkload(t, d)
		fs.Crash()

		got := recoverCrashState(t, fs)
		j := matchModelPrefix(states, got)
		if j < 0 {
			t.Fatalf("crash after sync %d recovered a state no model prefix explains:\n %v", n, got)
		}
		if j < prev {
			t.Fatalf("crash after sync %d recovered prefix %d, shorter than the %d a strictly earlier crash recovered", n, j, prev)
		}
		prev = j
	}

	if prev != ceSteps {
		t.Fatalf("crash at the final sync boundary recovered prefix %d, want the whole workload (%d)", prev, ceSteps)
	}
}

// TestLSMCrashInCompactionCheckpointWindowMatchesModel pins the specific
// compaction-and-GC boundary the sweep covers only incidentally: it drives the workload,
// settles the tree with repeated Maintain so a real compaction and version GC run, then
// crashes inside the window where that maintenance has been folded and the main file
// fsynced but the WAL has not yet been reset. A checkpoint never changes the visible data,
// only where it lives, so whether the crash lands just before or just after the reset the
// recovered database must hold the whole committed workload, identical to the model.
func TestLSMCrashInCompactionCheckpointWindowMatchesModel(t *testing.T) {
	states := crashModelStates()

	// Probe: run the workload, settle compaction and GC to quiescence, and measure the
	// sync that the settling checkpoint's main-file fsync will be.
	probe := vfs.NewMem()
	pd, err := Open(probe, "test.kv", crashEquivOptions())
	if err != nil {
		t.Fatalf("open probe: %v", err)
	}
	writeAllSteps(t, pd)
	settleCompaction(t, pd)
	mainFsync := probe.SyncCount() + 1 // the settling checkpoint's main-file fsync is next
	if err := pd.Close(); err != nil {
		t.Fatalf("close probe: %v", err)
	}

	fs := vfs.NewMem()
	fs.CrashAfterSync(mainFsync)
	d, err := Open(fs, "test.kv", crashEquivOptions())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	writeAllSteps(t, d)
	if c := settleCompaction(t, d); c == 0 {
		t.Fatalf("settle compacted no pages; the crash window is not over a freshly compacted tree")
	}
	// The settling checkpoint folds the merged segments and the MANIFEST and fsyncs the
	// main file (the freeze captures state here), then resets the WAL; the frozen image is
	// the pre-reset one, so the crash lands in the interrupted-checkpoint window over a
	// freshly compacted tree.
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	fs.Crash()

	got := recoverCrashState(t, fs)
	if !eq(got, states[ceSteps]...) {
		t.Fatalf("recovery in the compaction-checkpoint window lost data:\n got  %v\n want %v", got, states[ceSteps])
	}
}

// settleCompaction runs Maintain until no more pages compact, draining the flushed
// segments down through the leveled tree so a real multi-segment compaction and a version
// GC at the watermark have run before the crash window is entered. It returns the pages
// it compacted so a caller can assert the settle was not a no-op.
func settleCompaction(t *testing.T, d *DB) int {
	t.Helper()
	compacted := 0
	for {
		rep, err := d.Maintain(1 << 30)
		if err != nil {
			t.Fatalf("maintain: %v", err)
		}
		compacted += rep.PagesCompacted
		if rep.PagesCompacted == 0 {
			return compacted
		}
	}
}
