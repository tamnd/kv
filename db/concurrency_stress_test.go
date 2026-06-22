package db

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// This is the concurrency stress harness (spec 23 §6). The earlier concurrency tests run readers and
// writers against each other; what they do not run is the background work, the compaction and the
// checkpoint, that mutates the same structures a reader is traversing and a writer is appending to. That
// is the race surface this slice targets: a maintenance pass rewriting segments while a snapshot reads
// across them, a checkpoint folding the WAL while a commit appends to it. The test drives all four kinds
// of actor at once under the race detector and a soak loop, and holds them to two facts that survive any
// interleaving: a snapshot is stable, and a committed write is never lost or rolled back.
//
// The keyspace is partitioned so each writer owns a disjoint set of keys and no two writers touch the
// same key. Execution serializes writes anyway, so this is not about write conflicts; it is so the test
// has a precise model of the committed state without reconstructing an interleaving. Writers only ever
// set larger values, so the committed value of a key climbs monotonically, which gives readers a strong
// invariant to check and gives the end a single expected value per key. Deletes are left to the
// operation and compaction fuzzers, which already cover them; here the clean monotonic model is worth
// more than the extra coverage, because it turns a subtle consistency bug into an inequality that fails.

func TestConcurrentMaintenanceStress(t *testing.T) {
	for _, eng := range []struct {
		name string
		kind format.EngineKind
	}{
		{"btree", format.EngineBTree},
		{"lsm", format.EngineLSM},
	} {
		t.Run(eng.name, func(t *testing.T) {
			// A tiny memtable on the LSM side forces frequent flushes and so frequent compactions, putting
			// real background work in flight against the foreground. On the B-tree side the same loop drives
			// checkpoints folding the WAL against live commits.
			d := openMem(t, Options{Engine: eng.kind, MemtableSize: 4096})
			runMaintenanceStress(t, d)
		})
	}
}

func runMaintenanceStress(t *testing.T, d *DB) {
	t.Helper()
	const (
		writers   = 6
		perWriter = 500
		readers   = 4
		perKey    = 8 // keys each writer owns
	)

	// next is the single source of written values: a global counter, so every value is unique and, for
	// any one key, strictly increasing in commit order. A reader can therefore treat the value it sees
	// for a key as a watermark that must never go backwards across its successive snapshots.
	var next int64
	stop := make(chan struct{}) // closed when every writer has finished, to wind down the background actors

	skey := func(w, k int) string { return fmt.Sprintf("w%02d-k%03d", w, k) }

	// committed[w] is writer w's private record of the last value it wrote to each of its keys. The
	// partitions are disjoint, so the union of these maps is the exact committed state once the writers
	// stop, with no locking needed because no other writer touches writer w's keys.
	committed := make([]map[string]string, writers)

	var wg sync.WaitGroup        // readers + maintenance + checkpoint
	var writersWG sync.WaitGroup // writers only, so the closer can wind the background down when they finish

	// Writers: each owns a key partition and keeps setting larger values, several keys per transaction so
	// a single commit spans the partition and batches grow enough to spill the LSM memtable.
	for w := 0; w < writers; w++ {
		committed[w] = map[string]string{}
		writersWG.Add(1)
		go func(w int) {
			defer writersWG.Done()
			for i := 0; i < perWriter; i++ {
				err := d.Update(func(txn *Txn) error {
					for k := 0; k < perKey; k++ {
						v := strconv.FormatInt(atomic.AddInt64(&next, 1), 10)
						if err := txn.Set([]byte(skey(w, k)), []byte(v)); err != nil {
							return err
						}
						committed[w][skey(w, k)] = v
					}
					return nil
				})
				if err != nil {
					t.Errorf("writer %d update: %v", w, err)
					return
				}
			}
		}(w)
	}

	// Readers: open snapshots and check the two consistency facts. Within one snapshot a key read twice
	// must return the same value, since a snapshot is a fixed point in version order; and across this
	// reader's successive snapshots, taken in time order, a key's value must never decrease, since each
	// new snapshot reads a version at least as recent as the last and writers only ever raise a value.
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			seen := map[string]int64{} // highest value this reader has observed per key
			saw := 0
			for {
				select {
				case <-stop:
					if saw == 0 {
						t.Errorf("reader %d observed no keys, workload too quiet to be a test", r)
					}
					return
				default:
				}
				err := d.View(func(txn *Txn) error {
					for w := 0; w < writers; w++ {
						for k := 0; k < perKey; k++ {
							key := skey(w, k)
							v1, ok1, err := stressGet(txn, key)
							if err != nil {
								return err
							}
							if !ok1 {
								continue
							}
							saw++
							// Snapshot stability: the same key read again in this snapshot is unchanged.
							v2, ok2, err := stressGet(txn, key)
							if err != nil {
								return err
							}
							if !ok2 || v1 != v2 {
								t.Errorf("snapshot not stable for %s: %q then %q (present=%v)", key, v1, v2, ok2)
								return nil
							}
							n, err := strconv.ParseInt(v1, 10, 64)
							if err != nil {
								t.Errorf("key %s held non-numeric value %q", key, v1)
								return nil
							}
							// Monotonic reads: a later snapshot never shows an older value than this reader
							// already saw for the key.
							if n < seen[key] {
								t.Errorf("read regressed for %s: saw %d, now %d", key, seen[key], n)
								return nil
							}
							seen[key] = n
						}
					}
					return nil
				})
				if err != nil {
					t.Errorf("reader %d view: %v", r, err)
					return
				}
			}
		}(r)
	}

	// Maintenance: drive compaction (LSM) or vLog GC continuously against the foreground. The watermark
	// is whatever the oracle reports, so this is the real background pass, not a quiesced one.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := d.Maintain(64); err != nil {
				t.Errorf("maintain: %v", err)
				return
			}
			runtime.Gosched()
		}
	}()

	// Checkpoint: fold the WAL into the main file repeatedly while commits keep appending to it.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := d.Checkpoint(); err != nil {
				t.Errorf("checkpoint: %v", err)
				return
			}
			runtime.Gosched()
		}
	}()

	// Wind the background and reader actors down once the writers have all finished.
	go func() {
		writersWG.Wait()
		close(stop)
	}()

	wg.Wait()

	// The committed state is now fixed. Fold once more so the assertion reads through the main file, then
	// check every key equals the last value its writer wrote: nothing lost under compaction or checkpoint,
	// nothing rolled back, nothing resurrected.
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("final checkpoint: %v", err)
	}
	want := map[string]string{}
	for w := 0; w < writers; w++ {
		for k, v := range committed[w] {
			want[k] = v
		}
	}
	err := d.View(func(txn *Txn) error {
		for key, wantVal := range want {
			got, ok, err := stressGet(txn, key)
			if err != nil {
				return err
			}
			if !ok {
				t.Errorf("final: key %s absent, want %q", key, wantVal)
				continue
			}
			if got != wantVal {
				t.Errorf("final: key %s = %q, want %q", key, got, wantVal)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("final view: %v", err)
	}
}

// stressGet reads a key inside a transaction, mapping not-found to (",", false) so a reader can tell an
// absent key from an empty value without treating the miss as an error.
func stressGet(txn *Txn, key string) (string, bool, error) {
	v, err := txn.Get([]byte(key))
	if err != nil {
		if err.Error() == engine.ErrNotFound.Error() {
			return "", false, nil
		}
		return "", false, err
	}
	return string(v), true, nil
}
