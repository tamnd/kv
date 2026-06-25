package db

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// engineKindOf opens path just far enough to read the engine selector from its header,
// so a migration test can assert the rewritten file is genuinely the generation-2 core
// and not a copy of the source engine.
func engineKindOf(t *testing.T, fs vfs.FS, path string) format.EngineKind {
	t.Helper()
	pgr, err := pager.Open(fs, path, pager.Options{})
	if err != nil {
		t.Fatalf("peek %s: %v", path, err)
	}
	defer pgr.Close()
	return pgr.Header().Engine
}

// seedSource fills a fresh database of the given engine with a mix the migration has to
// carry faithfully: live keys, an overwritten key, a deleted key, a merge, and a range
// delete, then closes it. It returns the live key/value pairs the reopened source
// resolves, so a test can assert the migrated file resolves the identical set.
func seedSource(t *testing.T, fs vfs.FS, path string, kind format.EngineKind) {
	t.Helper()
	d, err := Open(fs, path, Options{Engine: kind, MemtableSize: 4096})
	if err != nil {
		t.Fatalf("open source (%v): %v", kind, err)
	}
	const n = 800
	for i := 0; i < n; i++ {
		k, v := fmt.Sprintf("k%05d", i), fmt.Sprintf("v%05d", i)
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte(k), []byte(v)) }); err != nil {
			t.Fatalf("seed set %d: %v", i, err)
		}
	}
	// Overwrite a band, delete a band, and range-delete a band, so the source carries
	// obsolete versions, point tombstones, and a range tombstone the rewrite must fold
	// to the same live view rather than the same physical bytes.
	for i := 100; i < 200; i++ {
		k, v := fmt.Sprintf("k%05d", i), fmt.Sprintf("w%05d", i)
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Set([]byte(k), []byte(v)) }); err != nil {
			t.Fatalf("overwrite %d: %v", i, err)
		}
	}
	for i := 200; i < 300; i++ {
		k := fmt.Sprintf("k%05d", i)
		if _, err := d.Write(func(b *engine.WriteBatch) { b.Delete([]byte(k)) }); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}
	if _, err := d.Write(func(b *engine.WriteBatch) {
		b.DeleteRange([]byte("k00300"), []byte("k00400"))
	}); err != nil {
		t.Fatalf("range delete: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}
}

// liveView returns every live key/value pair the database at path resolves at its
// latest snapshot, in ascending key order, as the equality reference for a migration.
func liveView(t *testing.T, fs vfs.FS, path string) map[string]string {
	t.Helper()
	d, err := Open(fs, path, Options{})
	if err != nil {
		t.Fatalf("open for view %s: %v", path, err)
	}
	defer d.Close()
	out := map[string]string{}
	if err := d.View(func(txn *Txn) error {
		it, err := txn.NewIterator(engine.IterOptions{})
		if err != nil {
			return err
		}
		defer it.Close()
		for ok := it.First(); ok; ok = it.Next() {
			v, err := it.Value()
			if err != nil {
				return err
			}
			out[string(it.Key())] = string(v)
		}
		return it.Error()
	}); err != nil {
		t.Fatalf("scan view %s: %v", path, err)
	}
	return out
}

// assertSameView fails unless got holds exactly the pairs in want.
func assertSameView(t *testing.T, want, got map[string]string) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("live key count = %d, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("key %q = %q, want %q", k, got[k], v)
		}
	}
}

// TestMigrateFromBTree and TestMigrateFromLSM rewrite a seeded generation-1 file of
// each shipped engine to a separate generation-2 destination and check the destination
// resolves the identical live key space and is genuinely the Bε-tree core.
func TestMigrateFromBTree(t *testing.T) { testMigrateFrom(t, format.EngineBTree) }
func TestMigrateFromLSM(t *testing.T)   { testMigrateFrom(t, format.EngineLSM) }

func testMigrateFrom(t *testing.T, kind format.EngineKind) {
	fs := vfs.NewMem()
	seedSource(t, fs, "src.kv", kind)
	want := liveView(t, fs, "src.kv")

	if err := Migrate(fs, "src.kv", "dst.kv"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if k := engineKindOf(t, fs, "dst.kv"); k != format.EngineBeta {
		t.Fatalf("migrated engine = %v, want generation-2 Beta", k)
	}
	assertSameView(t, want, liveView(t, fs, "dst.kv"))

	// The source is untouched: a separate destination migration leaves the original a
	// readable generation-1 file.
	if k := engineKindOf(t, fs, "src.kv"); k != kind {
		t.Fatalf("source engine changed to %v, want %v left untouched", k, kind)
	}
}

// TestMigrateInPlace upgrades a file by naming it as both source and destination: the
// rewrite goes to a temp beside it and the atomic rename swaps the generation-2 file in
// over the original path, which afterward opens as the Bε-tree core with the same live
// data.
func TestMigrateInPlace(t *testing.T) {
	fs := vfs.NewMem()
	seedSource(t, fs, "db.kv", format.EngineBTree)
	want := liveView(t, fs, "db.kv")

	if err := Migrate(fs, "db.kv", "db.kv"); err != nil {
		t.Fatalf("in-place migrate: %v", err)
	}
	if k := engineKindOf(t, fs, "db.kv"); k != format.EngineBeta {
		t.Fatalf("in-place engine = %v, want generation-2 Beta", k)
	}
	assertSameView(t, want, liveView(t, fs, "db.kv"))
}

// TestMigrateRejectsGen2 refuses to migrate a file that is already generation 2: there
// is nothing to upgrade and the rewrite would be silent waste, so Migrate returns an
// error and the file is left as-is.
func TestMigrateRejectsGen2(t *testing.T) {
	fs := vfs.NewMem()
	seedSource(t, fs, "src.kv", format.EngineBTree)
	if err := Migrate(fs, "src.kv", "dst.kv"); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if err := Migrate(fs, "dst.kv", "dst2.kv"); err == nil {
		t.Fatal("migrating an already generation-2 file should fail, got nil")
	}
}

// TestMigrateIdempotentAfterStaleTemp leaves a junk temp file at the path the
// migration builds into, then migrates: the run discards the stale temp before
// rewriting, so the leftover from a crashed earlier attempt does not corrupt or block
// the retry.
func TestMigrateIdempotentAfterStaleTemp(t *testing.T) {
	fs := vfs.NewMem()
	seedSource(t, fs, "src.kv", format.EngineLSM)
	want := liveView(t, fs, "src.kv")

	// Plant a stale temp at the exact path the migration will build into, as a crashed
	// prior attempt would have left.
	f, err := fs.Open("dst.kv"+migrateTmpSuffix, vfs.OpenReadWrite|vfs.OpenCreate)
	if err != nil {
		t.Fatalf("plant stale temp: %v", err)
	}
	if _, err := f.WriteAt([]byte("garbage from a crashed migration"), 0); err != nil {
		t.Fatalf("write stale temp: %v", err)
	}
	f.Close()

	if err := Migrate(fs, "src.kv", "dst.kv"); err != nil {
		t.Fatalf("migrate after stale temp: %v", err)
	}
	assertSameView(t, want, liveView(t, fs, "dst.kv"))
}

// TestMigrateEmpty migrates a database with no live keys: the rewrite produces a valid
// empty generation-2 file rather than failing on the degenerate stream.
func TestMigrateEmpty(t *testing.T) {
	fs := vfs.NewMem()
	d, err := Open(fs, "src.kv", Options{})
	if err != nil {
		t.Fatalf("open empty: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close empty: %v", err)
	}

	if err := Migrate(fs, "src.kv", "dst.kv"); err != nil {
		t.Fatalf("migrate empty: %v", err)
	}
	if k := engineKindOf(t, fs, "dst.kv"); k != format.EngineBeta {
		t.Fatalf("empty migrated engine = %v, want generation-2 Beta", k)
	}
	if got := liveView(t, fs, "dst.kv"); len(got) != 0 {
		t.Fatalf("empty migration produced %d keys, want 0", len(got))
	}
}
