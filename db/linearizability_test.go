package db

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/tamnd/kv/engine"
)

// This file is the model-based linearizability check the M2 exit criteria call for
// (spec 23 §2, §5): many goroutines drive randomized transactions against a real
// database, every committed transaction records what it read and wrote, and three
// verifiers prove the concurrent history is explainable by snapshot isolation under
// the single serial commit order the oracle imposes.
//
// The single writer makes the commit-version order a total order, so the serial order
// is known rather than searched for (no Porcupine-style exponential search needed):
// the obligations reduce to (1) every read returned the value its snapshot resolves,
// (2) the final state equals replaying committed writes in commit order, and (3) no
// committed read-modify-write lost an update to a writer that committed inside its
// window.

// absent is the sentinel a record uses for a key that read as missing, distinct from
// any real value the harness writes (which are always "wN" decimal strings).
const absent = "\x00absent\x00"

// txnRecord is one committed transaction's observable history.
type txnRecord struct {
	readVersion   uint64
	commitVersion uint64
	reads         map[string]string // key -> observed value, or absent
	writes        map[string]string // key -> net value written, or absent for a delete
	rmw           map[string]bool   // keys this txn both read and wrote (its conflict set)
}

// TestLinearizableSI runs a randomized concurrent workload and checks the resulting
// history against snapshot isolation.
func TestLinearizableSI(t *testing.T) {
	const (
		workers   = 8
		perWorker = 200
		keyspace  = 12
		seed      = 0x5eed
	)
	d := openMem(t, Options{MaxRetries: 0}) // explicit txns; the harness handles conflict

	// Seed the keyspace so reads have something to see.
	if err := d.Update(func(txn *Txn) error {
		for k := 0; k < keyspace; k++ {
			txn.Set(key(k), []byte("w0"))
		}
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var mu sync.Mutex
	var history []txnRecord
	var counter int64 // unique per-write value source

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed) + int64(w)))
			for i := 0; i < perWorker; i++ {
				rec, ok := runRandomTxn(d, rng, keyspace, &mu, &counter)
				if ok {
					mu.Lock()
					history = append(history, rec)
					mu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()

	if len(history) < workers { // sanity: the workload actually committed work
		t.Fatalf("only %d transactions committed, workload too quiet to be a test", len(history))
	}

	verifySnapshotReads(t, d, history)
	verifySerialReplay(t, d, history, keyspace)
	verifyNoLostUpdate(t, history)
}

// runRandomTxn runs one randomized explicit transaction. It reads a random subset of
// keys, optionally writes some (a write to a key it read is a read-modify-write), and
// commits. It returns the record and whether it committed (a conflict or empty
// read-only txn returns ok=false). The shared mutex+counter mint globally unique write
// values so a verifier can attribute a stored value to exactly one transaction.
func runRandomTxn(d *DB, rng *rand.Rand, keyspace int, mu *sync.Mutex, counter *int64) (txnRecord, bool) {
	writable := rng.Intn(100) < 70
	txn := d.Begin(writable)

	rec := txnRecord{
		readVersion: txn.readVersion,
		reads:       map[string]string{},
		writes:      map[string]string{},
		rmw:         map[string]bool{},
	}

	nReads := 1 + rng.Intn(4)
	readKeys := map[int]bool{}
	for r := 0; r < nReads; r++ {
		k := rng.Intn(keyspace)
		readKeys[k] = true
		v, err := txn.Get(key(k))
		switch {
		case errors.Is(err, engine.ErrNotFound):
			rec.reads[ks(k)] = absent
		case err != nil:
			txn.Discard()
			return txnRecord{}, false
		default:
			rec.reads[ks(k)] = string(v)
		}
	}

	if writable {
		nWrites := rng.Intn(3)
		for wn := 0; wn < nWrites; wn++ {
			k := rng.Intn(keyspace)
			if rng.Intn(10) == 0 {
				txn.Delete(key(k))
				rec.writes[ks(k)] = absent
			} else {
				mu.Lock()
				*counter++
				val := fmt.Sprintf("w%d", *counter)
				mu.Unlock()
				txn.Set(key(k), []byte(val))
				rec.writes[ks(k)] = val
			}
			if readKeys[k] {
				rec.rmw[ks(k)] = true
			}
		}
	}

	if !writable || len(rec.writes) == 0 {
		// A read-only transaction still produced a snapshot read history worth checking.
		txn.Discard()
		if len(rec.reads) == 0 {
			return txnRecord{}, false
		}
		rec.commitVersion = rec.readVersion // read-only: it observed the snapshot at rv
		return rec, true
	}

	if err := txn.Commit(); err != nil {
		txn.Discard()
		return txnRecord{}, false // a write-write conflict aborted it; nothing committed
	}
	rec.commitVersion = txn.commitTs
	return rec, true
}

// verifySnapshotReads checks every recorded read returned exactly the value the
// database resolves at that transaction's read version, after the fact. A live read
// during the concurrent chaos must equal the deterministic snapshot at the same
// version: this is snapshot isolation (stable, repeatable, never torn or dirty). The
// test runs no GC, so every historical version is still resolvable.
func verifySnapshotReads(t *testing.T, d *DB, history []txnRecord) {
	t.Helper()
	for _, rec := range history {
		for k, observed := range rec.reads {
			want := absent
			if v, ok, err := d.snapshotGet(rec.readVersion, []byte(k)); err != nil {
				t.Fatalf("snapshotGet(%d,%q): %v", rec.readVersion, k, err)
			} else if ok {
				want = string(v)
			}
			if observed != want {
				t.Fatalf("snapshot read %q at v%d observed %q, snapshot resolves %q (dirty/unstable read)",
					k, rec.readVersion, show(observed), show(want))
			}
		}
	}
}

// verifySerialReplay checks the final database state equals replaying every committed
// transaction's writes in commit-version order. The single writer makes that order a
// total order, so this is the unique serial schedule SI must be equivalent to.
func verifySerialReplay(t *testing.T, d *DB, history []txnRecord, keyspace int) {
	t.Helper()
	writers := make([]txnRecord, 0, len(history))
	for _, rec := range history {
		if len(rec.writes) > 0 {
			writers = append(writers, rec)
		}
	}
	sort.Slice(writers, func(i, j int) bool { return writers[i].commitVersion < writers[j].commitVersion })

	// No two writing transactions may share a commit version: the serial order is total.
	for i := 1; i < len(writers); i++ {
		if writers[i].commitVersion == writers[i-1].commitVersion {
			t.Fatalf("two committed writers share commit version %d", writers[i].commitVersion)
		}
	}

	model := map[string]string{}
	for k := 0; k < keyspace; k++ {
		model[ks(k)] = "w0"
	}
	for _, rec := range writers {
		for k, v := range rec.writes {
			if v == absent {
				delete(model, k)
			} else {
				model[k] = v
			}
		}
	}

	for k := 0; k < keyspace; k++ {
		want, present := model[ks(k)]
		v, ok := txnGet(t, d, ks(k))
		if present != ok || (present && v != want) {
			t.Fatalf("final %q = %q,%v, serial replay says %q,%v", ks(k), v, ok, want, present)
		}
	}
}

// verifyNoLostUpdate checks the first-committer-wins rule held for every read-modify-
// write: no transaction committed an RMW on a key that another transaction wrote in
// the window between this one's read snapshot and its commit. A violation would be a
// lost update -- the SI write-write anomaly the oracle must prevent.
func verifyNoLostUpdate(t *testing.T, history []txnRecord) {
	t.Helper()
	for _, rec := range history {
		if len(rec.rmw) == 0 {
			continue
		}
		for _, other := range history {
			if other.commitVersion <= rec.readVersion || other.commitVersion >= rec.commitVersion {
				continue // not strictly inside (readVersion, commitVersion)
			}
			if len(other.writes) == 0 {
				continue
			}
			for k := range rec.rmw {
				if _, wrote := other.writes[k]; wrote {
					t.Fatalf("lost update: txn (r%d,c%d) RMW %q but txn c%d wrote it in between",
						rec.readVersion, rec.commitVersion, k, other.commitVersion)
				}
			}
		}
	}
}

// TestWriteSkewAllowedUnderSI documents the one anomaly SI permits (spec 10 §3): two
// transactions read an overlapping set, write disjoint keys, and both commit, jointly
// breaking an invariant no serial order would. It is the boundary SSI (§4) closes; under
// SI it must be allowed, so this test asserts both commits succeed.
func TestWriteSkewAllowedUnderSI(t *testing.T) {
	d := openMem(t, Options{})
	d.Update(func(txn *Txn) error {
		txn.Set([]byte("x"), []byte("1"))
		txn.Set([]byte("y"), []byte("1"))
		return nil
	})

	// Invariant the application "wants": x and y not both set to 0. Each txn reads both,
	// sees the invariant holds, and zeroes the one the other did not -- classic write skew.
	t1 := d.Begin(true)
	t2 := d.Begin(true)

	if _, err := t1.Get([]byte("x")); err != nil {
		t.Fatalf("t1 read x: %v", err)
	}
	if _, err := t1.Get([]byte("y")); err != nil {
		t.Fatalf("t1 read y: %v", err)
	}
	if _, err := t2.Get([]byte("x")); err != nil {
		t.Fatalf("t2 read x: %v", err)
	}
	if _, err := t2.Get([]byte("y")); err != nil {
		t.Fatalf("t2 read y: %v", err)
	}
	t1.Set([]byte("x"), []byte("0"))
	t2.Set([]byte("y"), []byte("0"))

	if err := t1.Commit(); err != nil {
		t.Fatalf("t1 commit: %v", err)
	}
	// Disjoint write sets, so first-committer-wins does not abort t2: SI lets the skew
	// through. Both x and y end up 0, the jointly-invalid state.
	if err := t2.Commit(); err != nil {
		t.Fatalf("t2 commit = %v, want success (SI permits write skew)", err)
	}
	if vx, _ := txnGet(t, d, "x"); vx != "0" {
		t.Fatalf("x = %q, want 0", vx)
	}
	if vy, _ := txnGet(t, d, "y"); vy != "0" {
		t.Fatalf("y = %q, want 0", vy)
	}
}

func key(k int) []byte { return []byte(ks(k)) }
func ks(k int) string  { return fmt.Sprintf("k%02d", k) }

// show renders a value for an error message, naming the absent sentinel.
func show(v string) string {
	if v == absent {
		return "<absent>"
	}
	return v
}
