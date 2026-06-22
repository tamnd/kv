package lsm

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// newAutoLSM opens an LSM with the background compactor left on, the production default, so a
// test can drive a sustained write stream and watch the flusher both seal segments and
// compact them down with no host Maintain in the loop. It is the deliberate opposite of
// newLSM, which turns the compactor off to pin a known segment shape.
func newAutoLSM(t *testing.T) *LSM {
	t.Helper()
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "auto.kv", pager.Options{
		PageSize:    4096,
		CacheFrames: 64,
		Engine:      format.EngineLSM,
	})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	l := New(p)
	if err := l.Open(&engine.Env{Pager: p, Options: engine.EngineOptions{PageSize: p.PageSize()}}); err != nil {
		t.Fatalf("open lsm: %v", err)
	}
	l.SetMergeFunc(concatMerge)
	t.Cleanup(func() { l.Close() })
	return l
}

// settleAuto blocks until the background flusher has drained the sealed queue and the
// compactor has no due action left, the quiescent point at which the tree shape is stable
// enough to assert on. Because compactionDueLocked stays true while a compaction's inputs are
// still in the live set, this never returns mid-merge.
func (l *LSM) settleAuto(t *testing.T) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	for (len(l.imm) > 0 || l.compactionDueLocked()) && l.flushErr == nil {
		l.flushCond.Wait()
	}
	if l.flushErr != nil {
		t.Fatalf("background flush/compaction: %v", l.flushErr)
	}
}

// TestAutoCompactionBoundsL0 streams enough small batches to flush far more than a trigger's
// worth of L0 segments and confirms the background compactor, with no host Maintain, keeps L0
// under its fan-in trigger and folds the data down into deeper levels on its own.
func TestAutoCompactionBoundsL0(t *testing.T) {
	l := newAutoLSM(t)
	l.memtableCap = 1 // every applied batch seals the prior one, so each Apply yields an L0 segment

	const batches = 40
	version := uint64(1)
	for s := 0; s < batches; s++ {
		b := engine.NewWriteBatch(version)
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("key%02d%05d", s, i)
			b.Set([]byte(key), []byte(fmt.Sprintf("v%d", version)))
		}
		if err := l.Apply(b, version); err != nil {
			t.Fatalf("apply batch %d: %v", s, err)
		}
		version++
	}
	l.settleAuto(t)

	l.mu.Lock()
	l0 := 0
	if len(l.levelsLocked()) > 0 {
		l0 = len(l.levelsLocked()[0])
	}
	deeper := len(l.levelsLocked()) > 1
	l.mu.Unlock()

	if l0 >= l.l0Trigger {
		t.Fatalf("L0 holds %d segments after settle, want below the trigger %d: the compactor did not bound it", l0, l.l0Trigger)
	}
	if !deeper {
		t.Fatal("no level below L0 after 40 flushed batches: the compactor never pushed anything down")
	}
}

// TestAutoCompactionKeepsDataVisible drives the same sustained stream with overwrites and a
// delete, then reads every key back, proving the background compactor merges versions without
// dropping a live value or resurrecting a deleted one while it runs off the foreground path.
func TestAutoCompactionKeepsDataVisible(t *testing.T) {
	l := newAutoLSM(t)
	l.memtableCap = 1

	const keys = 300
	// First wave: set every key.
	b1 := engine.NewWriteBatch(10)
	for i := 0; i < keys; i++ {
		b1.Set([]byte(fmt.Sprintf("k%05d", i)), []byte("first"))
	}
	if err := l.Apply(b1, 10); err != nil {
		t.Fatalf("apply first wave: %v", err)
	}
	// Second wave, split into many small batches so the flusher seals a long run of L0
	// segments the compactor has to fold: overwrite the even keys, delete every fifth.
	version := uint64(11)
	for i := 0; i < keys; i++ {
		b := engine.NewWriteBatch(version)
		k := []byte(fmt.Sprintf("k%05d", i))
		switch {
		case i%5 == 0:
			b.Delete(k)
		case i%2 == 0:
			b.Set(k, []byte("second"))
		}
		if err := l.Apply(b, version); err != nil {
			t.Fatalf("apply key %d: %v", i, err)
		}
		version++
	}
	l.settleAuto(t)

	rd, err := l.NewReader(engine.Snapshot{Version: version})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer rd.Close()
	for i := 0; i < keys; i++ {
		k := []byte(fmt.Sprintf("k%05d", i))
		v, err := rd.Get(k)
		switch {
		case i%5 == 0:
			if err != engine.ErrNotFound {
				t.Fatalf("Get(%s) = (%q,%v), want ErrNotFound after delete", k, v, err)
			}
		case i%2 == 0:
			if err != nil || string(v) != "second" {
				t.Fatalf("Get(%s) = (%q,%v), want second", k, v, err)
			}
		default:
			if err != nil || string(v) != "first" {
				t.Fatalf("Get(%s) = (%q,%v), want first", k, v, err)
			}
		}
	}
}
