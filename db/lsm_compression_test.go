package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
)

// loadCompressible writes a compressible workload into d in many batches, then overwrites
// and deletes a fraction of it at higher versions. The batches matter: a flush only happens
// on the Apply after a memtable crosses its cap, so a single giant transaction would never
// flush and the data would sit in the memtable untouched by any segment. Writing in batches
// produces real flushed segments and, once they are large enough to span pages, the dense
// packing that block compression rests on. The values share a long common prefix so a page
// of them compresses well.
func loadCompressible(t *testing.T, d *DB, n int) {
	t.Helper()
	const batch = 100
	for lo := 0; lo < n; lo += batch {
		hi := lo + batch
		if hi > n {
			hi = n
		}
		if err := d.Update(func(txn *Txn) error {
			for i := lo; i < hi; i++ {
				txn.Set([]byte(fmt.Sprintf("key%06d", i)), []byte(fmt.Sprintf("payload-field-value-shared-prefix-%06d", i)))
			}
			return nil
		}); err != nil {
			t.Fatalf("seed batch [%d,%d): %v", lo, hi, err)
		}
	}
	if err := d.Update(func(txn *Txn) error {
		for i := 0; i < n; i += 3 {
			txn.Set([]byte(fmt.Sprintf("key%06d", i)), []byte(fmt.Sprintf("payload-field-rewrite-%06d", i)))
		}
		for i := 0; i < n; i += 5 {
			txn.Delete([]byte(fmt.Sprintf("key%06d", i)))
		}
		return nil
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
}

// TestLSMCompressionTransparent drives the Compression option through the public database:
// the same writes are loaded into one database with compression off and one with it on,
// both settled into segments, and every read must agree. Heat-tiered block compression is a
// pure space optimization, so turning it on may never change what a point read or a scan
// returns, present key or absent. The memtable is sized so a flush spans pages, so the
// settled tree carries genuinely compressed segments behind the read path rather than only
// single-page flushes the codec leaves raw.
func TestLSMCompressionTransparent(t *testing.T) {
	const n = 4000
	// A memtable large enough that a flush writes a multi-page run, so the cold compaction
	// output actually packs and compresses rather than staying one raw page per flush.
	base := Options{PageSize: 4096, Engine: format.EngineLSM, MemtableSize: 64 * 1024}
	plain := openMem(t, base)
	loadCompressible(t, plain, n)
	settleLSM(t, plain)

	compOpts := base
	compOpts.Compression = true
	comp := openMem(t, compOpts)
	loadCompressible(t, comp, n)
	settleLSM(t, comp)

	// Point reads of every key, present and absent, must give identical answers with and
	// without compression. The deleted keys (i%5==0) are the absent population.
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%06d", i)
		pv, pok := txnGet(t, plain, k)
		cv, cok := txnGet(t, comp, k)
		if pok != cok || pv != cv {
			t.Fatalf("point read of %s disagrees: plain (%q,%v), compressed (%q,%v)", k, pv, pok, cv, cok)
		}
	}
	for i := n; i < n+200; i++ {
		k := fmt.Sprintf("key%06d", i)
		_, pok := txnGet(t, plain, k)
		_, cok := txnGet(t, comp, k)
		if pok || cok {
			t.Fatalf("never-written key %s reported present: plain %v, compressed %v", k, pok, cok)
		}
	}

	// Spot-check the compressed result against the truth with point reads so a bug shared by
	// both paths cannot pass: a deleted key gone, an overwritten key carrying its new value,
	// an untouched key its original.
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%06d", i)
		got, ok := txnGet(t, comp, k)
		switch {
		case i%5 == 0:
			if ok {
				t.Fatalf("deleted key %s still present under compression", k)
			}
		case i%3 == 0:
			if !ok || got != fmt.Sprintf("payload-field-rewrite-%06d", i) {
				t.Fatalf("overwritten key %s = %q,%v, want rewrite", k, got, ok)
			}
		default:
			if !ok || got != fmt.Sprintf("payload-field-value-shared-prefix-%06d", i) {
				t.Fatalf("key %s = %q,%v, want original", k, got, ok)
			}
		}
	}
}

// TestLSMColdCompression drives the cold-only compression policy through the public
// CompressionMode option. Cold-only leaves the hot shallow levels raw and compresses only
// the cold deep levels (perf/05 F4d), so it must stay invisible to reads exactly like the
// heat-tiered mode, and still shrink the settled file below the uncompressed baseline because
// the bulk of the data settles in the cold levels it does compress. The same workload is
// loaded three ways, settled, and checkpointed; cold-only reads must match the plain database
// key for key, and the cold-only file must land between the plain file (larger) and the
// fully heat-tiered file (no larger than cold-only, since it also compresses the hot levels).
func TestLSMColdCompression(t *testing.T) {
	const n = 4000
	orig := func(i int) string { return fmt.Sprintf("payload-field-value-shared-prefix-%06d", i) }

	write := func(fs *vfs.Mem, mode engine.CompressionMode) {
		opts := Options{PageSize: 4096, Engine: format.EngineLSM, MemtableSize: 64 * 1024, CompressionMode: mode}
		d, err := Open(fs, "test.kv", opts)
		if err != nil {
			t.Fatalf("open (mode=%d): %v", mode, err)
		}
		const batch = 100
		for lo := 0; lo < n; lo += batch {
			if err := d.Update(func(txn *Txn) error {
				for i := lo; i < lo+batch; i++ {
					txn.Set([]byte(fmt.Sprintf("key%06d", i)), []byte(orig(i)))
				}
				return nil
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}
		}
		settleLSM(t, d)
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
		if err := d.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}

	plainFS := vfs.NewMem()
	write(plainFS, engine.CompressOff)
	coldFS := vfs.NewMem()
	write(coldFS, engine.CompressColdOnly)
	tieredFS := vfs.NewMem()
	write(tieredFS, engine.CompressHeatTiered)

	// Reopen the cold-only database with bare options and check every key reads back, proving
	// the deep compressed levels decode from their self-describing frames with no policy hint.
	d2, err := Open(coldFS, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen cold-only db: %v", err)
	}
	defer d2.Close()
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%06d", i)
		v, ok := txnGet(t, d2, k)
		if !ok || v != orig(i) {
			t.Fatalf("after reopen key %s = %q,%v, want %q", k, v, ok, orig(i))
		}
	}

	// Size ordering is a stable invariant of the policy: off compresses nothing, cold-only a
	// subset of the levels, heat-tiered all of them, on the same compressible data. So
	// tiered <= cold-only <= plain always holds (with equality when the data is shallow enough
	// that no level cold-only touches exists). Heat-tiered always reaches the hot levels this
	// workload does populate, so it strictly shrinks the file, proving compression engages.
	plainSize := fileSize(t, plainFS, "test.kv")
	coldSize := fileSize(t, coldFS, "test.kv")
	tieredSize := fileSize(t, tieredFS, "test.kv")
	if tieredSize >= plainSize {
		t.Fatalf("heat-tiered did not shrink the file: plain %d, tiered %d", plainSize, tieredSize)
	}
	if coldSize > plainSize {
		t.Fatalf("cold-only file larger than plain: plain %d, cold-only %d", plainSize, coldSize)
	}
	if tieredSize > coldSize {
		t.Fatalf("heat-tiered file larger than cold-only: tiered %d, cold-only %d", tieredSize, coldSize)
	}
}

// TestLSMCompressionPersistsAndDecodesWithoutOption is the load-bearing persistence test:
// a database written with compression on is settled, checkpointed, closed, and reopened with
// a plain Options{} that does not set Compression at all. Every key must still read back.
// Because the reader never learns the segments were compressed from its options, this proves
// the decode path is driven entirely by the self-describing per-page frame, not by any
// runtime policy. The compressed main file must also be smaller than the uncompressed one,
// which only holds if real compressed pages reached durable storage through the checkpoint.
func TestLSMCompressionPersistsAndDecodesWithoutOption(t *testing.T) {
	const n = 4000
	orig := func(i int) string { return fmt.Sprintf("payload-field-value-shared-prefix-%06d", i) }

	write := func(fs *vfs.Mem, compression bool) {
		opts := Options{PageSize: 4096, Engine: format.EngineLSM, MemtableSize: 64 * 1024, Compression: compression}
		d, err := Open(fs, "test.kv", opts)
		if err != nil {
			t.Fatalf("open (compression=%v): %v", compression, err)
		}
		const batch = 100
		for lo := 0; lo < n; lo += batch {
			if err := d.Update(func(txn *Txn) error {
				for i := lo; i < lo+batch; i++ {
					txn.Set([]byte(fmt.Sprintf("key%06d", i)), []byte(orig(i)))
				}
				return nil
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}
		}
		settleLSM(t, d)
		// Fold the flushed and compacted segments into the main file so the reopen reads them
		// from pages and the file size reflects the compressed footprint.
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
		if err := d.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}

	plainFS := vfs.NewMem()
	write(plainFS, false)
	compFS := vfs.NewMem()
	write(compFS, true)

	// Reopen the compressed database with a bare Options{}: no engine, no compression flag.
	// The reader rediscovers the LSM engine from the file header and must decode the
	// compressed data pages purely from their on-page codec id.
	d2, err := Open(compFS, "test.kv", Options{})
	if err != nil {
		t.Fatalf("reopen compressed db with bare options: %v", err)
	}
	defer d2.Close()
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%06d", i)
		v, ok := txnGet(t, d2, k)
		if !ok || v != orig(i) {
			t.Fatalf("after reopen key %s = %q,%v, want %q", k, v, ok, orig(i))
		}
	}

	// The compressed main file must be smaller, confirming dense packing reached durable
	// storage and was not undone by the checkpoint.
	plainSize := fileSize(t, plainFS, "test.kv")
	compSize := fileSize(t, compFS, "test.kv")
	if compSize >= plainSize {
		t.Fatalf("compressed file not smaller: plain %d bytes, compressed %d bytes", plainSize, compSize)
	}
}
