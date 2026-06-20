package kv_test

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/tamnd/kv"
)

// open creates a fresh database in a temp dir, registering its cleanup.
func open(t *testing.T, opts ...kv.Option) *kv.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data.kv")
	d, err := kv.Open(path, opts...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

// TestOpenSetGet round-trips a value through the public Update/View surface.
func TestOpenSetGet(t *testing.T) {
	d := open(t)
	if err := d.Update(func(txn *kv.Txn) error {
		return txn.Set([]byte("hello"), []byte("world"))
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := d.View(func(txn *kv.Txn) error {
		v, err := txn.Get([]byte("hello"))
		if err != nil {
			return err
		}
		if string(v) != "world" {
			t.Fatalf("get = %q, want world", v)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

// TestNotFound checks the public ErrNotFound sentinel is matchable.
func TestNotFound(t *testing.T) {
	d := open(t)
	err := d.View(func(txn *kv.Txn) error {
		_, err := txn.Get([]byte("absent"))
		return err
	})
	if !errors.Is(err, kv.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// TestReadOnlyTxnRejectsWrite checks a write on a View transaction surfaces ErrReadOnly.
func TestReadOnlyTxnRejectsWrite(t *testing.T) {
	d := open(t)
	err := d.View(func(txn *kv.Txn) error {
		return txn.Set([]byte("k"), []byte("v"))
	})
	if !errors.Is(err, kv.ErrReadOnly) {
		t.Fatalf("err = %v, want ErrReadOnly", err)
	}
}

// TestExplicitConflict checks the explicit Begin/Commit surface and ErrConflict.
func TestExplicitConflict(t *testing.T) {
	d := open(t)
	if err := d.Update(func(txn *kv.Txn) error { return txn.Set([]byte("k"), []byte("0")) }); err != nil {
		t.Fatalf("seed: %v", err)
	}

	t1 := d.Begin(true)
	defer t1.Discard()
	t2 := d.Begin(true)
	defer t2.Discard()

	if _, err := t1.Get([]byte("k")); err != nil {
		t.Fatalf("t1 get: %v", err)
	}
	if _, err := t2.Get([]byte("k")); err != nil {
		t.Fatalf("t2 get: %v", err)
	}
	t1.Set([]byte("k"), []byte("1"))
	t2.Set([]byte("k"), []byte("2"))

	if err := t1.Commit(); err != nil {
		t.Fatalf("t1 commit: %v", err)
	}
	if err := t2.Commit(); !errors.Is(err, kv.ErrConflict) {
		t.Fatalf("t2 commit = %v, want ErrConflict", err)
	}
}

// TestIterator walks a prefix scan through the public iterator.
func TestIterator(t *testing.T) {
	d := open(t)
	if err := d.Update(func(txn *kv.Txn) error {
		for i := 0; i < 5; i++ {
			txn.Set([]byte(fmt.Sprintf("user:%d", i)), []byte(fmt.Sprintf("v%d", i)))
		}
		txn.Set([]byte("other"), []byte("x"))
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var got []string
	if err := d.View(func(txn *kv.Txn) error {
		it, err := txn.NewIterator(kv.IterOptions{Prefix: []byte("user:")})
		if err != nil {
			return err
		}
		defer it.Close()
		for it.First(); it.Valid(); it.Next() {
			got = append(got, string(it.Key()))
		}
		return it.Error()
	}); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("scanned %d keys, want 5: %v", len(got), got)
	}
}

// TestMergeOperator checks a registered associative operator folds blind operands.
func TestMergeOperator(t *testing.T) {
	add := func(existing, operand []byte) []byte {
		var sum int
		if len(existing) > 0 {
			fmt.Sscanf(string(existing), "%d", &sum)
		}
		var inc int
		fmt.Sscanf(string(operand), "%d", &inc)
		return []byte(fmt.Sprintf("%d", sum+inc))
	}
	d := open(t, kv.WithMergeOperator("add", add))

	for i := 0; i < 3; i++ {
		if err := d.Update(func(txn *kv.Txn) error {
			return txn.Merge([]byte("hits"), []byte("1"))
		}); err != nil {
			t.Fatalf("merge %d: %v", i, err)
		}
	}
	if err := d.View(func(txn *kv.Txn) error {
		v, err := txn.Get([]byte("hits"))
		if err != nil {
			return err
		}
		if string(v) != "3" {
			t.Fatalf("hits = %q, want 3", v)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

// TestSerializableOption checks WithIsolation(Serializable) aborts write skew through
// the public surface.
func TestSerializableOption(t *testing.T) {
	d := open(t, kv.WithIsolation(kv.Serializable))
	if err := d.Update(func(txn *kv.Txn) error {
		txn.Set([]byte("x"), []byte("1"))
		txn.Set([]byte("y"), []byte("1"))
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	t1 := d.Begin(true)
	defer t1.Discard()
	t2 := d.Begin(true)
	defer t2.Discard()
	for _, tx := range []*kv.Txn{t1, t2} {
		if _, err := tx.Get([]byte("x")); err != nil {
			t.Fatalf("read x: %v", err)
		}
		if _, err := tx.Get([]byte("y")); err != nil {
			t.Fatalf("read y: %v", err)
		}
	}
	t1.Set([]byte("x"), []byte("0"))
	t2.Set([]byte("y"), []byte("0"))
	if err := t1.Commit(); err != nil {
		t.Fatalf("t1 commit: %v", err)
	}
	if err := t2.Commit(); !errors.Is(err, kv.ErrConflict) {
		t.Fatalf("t2 commit = %v, want ErrConflict under Serializable", err)
	}
}

// TestStatsReportsSpaceAndVersion checks the public Stats surface reflects writes, the
// checkpoint backlog, and that a checkpoint drains the backlog and persists the version.
func TestStatsReportsSpaceAndVersion(t *testing.T) {
	d := open(t)
	for i := 0; i < 20; i++ {
		if err := d.Update(func(txn *kv.Txn) error {
			return txn.Set([]byte(fmt.Sprintf("k%02d", i)), []byte("v"))
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	s := d.Stats()
	if s.Engine != kv.BTree {
		t.Fatalf("engine = %v, want btree", s.Engine)
	}
	if s.Version != 20 {
		t.Fatalf("version = %d, want 20", s.Version)
	}
	if s.PageSize <= 0 || s.PageCount == 0 {
		t.Fatalf("page accounting = %d size / %d count, want positive", s.PageSize, s.PageCount)
	}
	if s.WALBacklog == 0 {
		t.Fatalf("wal backlog = 0 before checkpoint, want pending frames")
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	after := d.Stats()
	if after.WALBacklog != 0 {
		t.Fatalf("wal backlog = %d after checkpoint, want 0", after.WALBacklog)
	}
	if after.Version != 20 {
		t.Fatalf("version = %d after checkpoint, want 20", after.Version)
	}
}

// TestAutoCheckpointBoundsWAL checks the public WithAutoCheckpoint option wires the
// background checkpointer so a long run of commits keeps the WAL backlog bounded
// (spec 09 §1.3) without the caller ever calling Checkpoint.
func TestAutoCheckpointBoundsWAL(t *testing.T) {
	const threshold = 8
	d := open(t, kv.WithAutoCheckpoint(threshold))
	for i := 0; i < 300; i++ {
		if err := d.Update(func(txn *kv.Txn) error {
			return txn.Set([]byte(fmt.Sprintf("k%04d", i)), []byte("v"))
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	var last uint64
	for time.Now().Before(deadline) {
		last = d.Stats().WALBacklog
		if last < threshold {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if last >= threshold {
		t.Fatalf("WAL backlog %d never fell below threshold %d", last, threshold)
	}
}

// TestCheckReportsSound writes a spread of keys and confirms the public Check returns a
// sound report: no problems, a positive key count, and balanced page accounting.
func TestCheckReportsSound(t *testing.T) {
	d := open(t)
	for i := 0; i < 200; i++ {
		if err := d.Update(func(txn *kv.Txn) error {
			return txn.Set([]byte(fmt.Sprintf("k%05d", i)), []byte("v"))
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	rep, err := d.Check()
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !rep.OK() {
		t.Fatalf("sound database reported %d problems: %+v", len(rep.Problems), rep.Problems)
	}
	if rep.Keys != 200 {
		t.Fatalf("check keys = %d, want 200", rep.Keys)
	}
	if got := 1 + rep.PagesVisited + rep.FreePages; uint32(got) != rep.PageCount {
		t.Fatalf("accounting 1+%d+%d = %d != page count %d", rep.PagesVisited, rep.FreePages, got, rep.PageCount)
	}
}

// TestVacuumKeepsDatabaseSound fills the database, deletes a swath of it, and runs an
// incremental vacuum, confirming the call is safe through the public surface: it never
// errors, the page count never grows, the structure stays sound, and surviving keys read
// back unchanged (spec 09 §3.1). The B-tree core does not yet return emptied pages to the
// freelist (lazy node merge is a later milestone), so today the vacuum is a sound no-op on
// a tree-backed file; the page-level reclamation it performs once the freelist carries
// trailing pages is proven in the pager and db tests. This test guards that wiring vacuum
// into the public API neither corrupts nor loses data, and stays valid once node merge
// begins feeding the freelist.
func TestVacuumKeepsDatabaseSound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.kv")
	d, err := kv.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	const n = 1000
	val := make([]byte, 128)
	for i := 0; i < n; i++ {
		if err := d.Update(func(txn *kv.Txn) error {
			return txn.Set([]byte(fmt.Sprintf("k%06d", i)), val)
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	before := d.Stats().PageCount

	for i := n / 2; i < n; i++ {
		if err := d.Update(func(txn *kv.Txn) error {
			return txn.Delete([]byte(fmt.Sprintf("k%06d", i)))
		}); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}

	freed, err := d.Vacuum(0)
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if freed < 0 {
		t.Fatalf("vacuum reported %d freed pages", freed)
	}
	if after := d.Stats().PageCount; after > before {
		t.Fatalf("page count grew from %d to %d across a vacuum", before, after)
	}

	rep, err := d.Check()
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !rep.OK() {
		t.Fatalf("database unsound after vacuum: %+v", rep.Problems)
	}
	if err := d.View(func(txn *kv.Txn) error {
		v, err := txn.Get([]byte("k000000"))
		if err != nil {
			return err
		}
		if len(v) != len(val) {
			t.Fatalf("surviving value len = %d, want %d", len(v), len(val))
		}
		return nil
	}); err != nil {
		t.Fatalf("view after vacuum: %v", err)
	}
}

// TestReopenPersists checks data survives Close and reopen of the same path.
func TestReopenPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.kv")
	d, err := kv.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := d.Update(func(txn *kv.Txn) error { return txn.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := kv.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	if err := d2.View(func(txn *kv.Txn) error {
		v, err := txn.Get([]byte("k"))
		if err != nil {
			return err
		}
		if string(v) != "v" {
			t.Fatalf("after reopen k = %q, want v", v)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

// TestHeaderTagsPersist confirms the application_id and user_version header tags (spec 22 §2)
// are durable: a value set on one handle is readable after a full close and reopen, and the
// surrounding key data is unharmed.
func TestHeaderTagsPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.kv")
	d, err := kv.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := d.Update(func(txn *kv.Txn) error { return txn.Set([]byte("k"), []byte("v")) }); err != nil {
		t.Fatalf("update: %v", err)
	}
	if got := d.ApplicationID(); got != 0 {
		t.Fatalf("fresh application_id = %d, want 0", got)
	}
	if err := d.SetApplicationID(0xCAFEF00D); err != nil {
		t.Fatalf("set application_id: %v", err)
	}
	if err := d.SetUserVersion(7); err != nil {
		t.Fatalf("set user_version: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	d2, err := kv.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	if got := d2.ApplicationID(); got != 0xCAFEF00D {
		t.Fatalf("application_id after reopen = %#x, want 0xcafef00d", got)
	}
	if got := d2.UserVersion(); got != 7 {
		t.Fatalf("user_version after reopen = %d, want 7", got)
	}
	// The key data set before stamping the tags survived the header writes.
	if err := d2.View(func(txn *kv.Txn) error {
		v, err := txn.Get([]byte("k"))
		if err != nil {
			return err
		}
		if string(v) != "v" {
			t.Fatalf("k = %q after tag writes, want v", v)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
	// The file is still structurally sound after the header rewrites.
	rep, err := d2.Check()
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !rep.OK() {
		t.Fatalf("check found %d problem(s) after tag writes", len(rep.Problems))
	}
}
