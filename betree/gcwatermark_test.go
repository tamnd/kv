package betree

import (
	"sync"
	"sync/atomic"
	"testing"
)

// This file gates the M5.3 split OLTP/OLAP garbage-collection watermark (gcwatermark.go), the piece
// that decides when a dead MVCC version may be reclaimed without one global low-watermark stalling
// behind the oldest reader. The contract has three parts. The OLTP gate: a dead version superseded at
// succTs is reclaimable on the OLTP side only when succTs is at or below the oldest short reader's
// read timestamp, and when no short reader is open nothing on that side constrains GC. Per-range OLAP
// pinning, the central win: a long analytical scan over a narrow range pins old versions only inside
// that range and only for as long as it runs, so it never freezes versions outside its range the way
// a single global watermark would. Composition: a version is reclaimable only when it clears the OLTP
// gate and no OLAP reader pins it, and that joint answer is read off one consistent registry state.

func TestKeyRangeContains(t *testing.T) {
	// Half-open [b, f): b is in, f is out, nil bounds are unbounded on that side.
	r := keyRange{lo: []byte("b"), hi: []byte("f")}
	for _, tc := range []struct {
		key  string
		want bool
	}{
		{"a", false}, {"b", true}, {"c", true}, {"e", true}, {"f", false}, {"g", false},
	} {
		if got := r.contains([]byte(tc.key)); got != tc.want {
			t.Errorf("[b,f) contains %q = %v, want %v", tc.key, got, tc.want)
		}
	}
	// Unbounded below: everything under f is in.
	loOpen := keyRange{hi: []byte("f")}
	if !loOpen.contains([]byte("a")) || loOpen.contains([]byte("f")) {
		t.Fatalf("unbounded-below range misjudged a/f")
	}
	// Unbounded above: everything at or over b is in.
	hiOpen := keyRange{lo: []byte("b")}
	if hiOpen.contains([]byte("a")) || !hiOpen.contains([]byte("z")) {
		t.Fatalf("unbounded-above range misjudged a/z")
	}
	// Zero value is the whole keyspace.
	var whole keyRange
	if !whole.contains([]byte("")) || !whole.contains([]byte("anything")) {
		t.Fatalf("zero-value range should cover the whole keyspace")
	}
}

func TestGCWatermarkEmptyReclaimsEverything(t *testing.T) {
	g := newGCWatermark()
	// With no reader open the OLTP watermark is the maximum and nothing is pinned, so any dead version
	// at any timestamp is reclaimable. This is the idle state that must never stall GC.
	if g.oltpWatermark() != maxHLCTime {
		t.Fatalf("idle OLTP watermark is %d, want max", g.oltpWatermark())
	}
	if !g.reclaimable([]byte("k"), 1) || !g.reclaimable([]byte("k"), maxHLCTime-1) {
		t.Fatalf("idle registry should reclaim every dead version")
	}
}

func TestGCWatermarkOLTPGate(t *testing.T) {
	g := newGCWatermark()
	// Two short readers; the watermark is the older one's read timestamp.
	older := g.registerOLTP(100)
	g.registerOLTP(140)
	if w := g.oltpWatermark(); w != 100 {
		t.Fatalf("OLTP watermark is %d, want the oldest reader 100", w)
	}
	// A version superseded at or before 100 is past both readers and reclaimable; one superseded after
	// 100 is still needed by the older reader and held.
	if !g.reclaimable([]byte("k"), 100) {
		t.Fatalf("version superseded at the watermark should reclaim")
	}
	if g.reclaimable([]byte("k"), 101) {
		t.Fatalf("version superseded after the oldest OLTP reader must be held")
	}
	// Release the older reader; the watermark jumps to 140 and the held version clears.
	g.release(older)
	if w := g.oltpWatermark(); w != 140 {
		t.Fatalf("after releasing the oldest, watermark is %d, want 140", w)
	}
	if !g.reclaimable([]byte("k"), 140) {
		t.Fatalf("version at the advanced watermark should reclaim")
	}
}

// TestGCWatermarkOLAPPinsOnlyItsRange is the central property: a long scan over a narrow range freezes
// old versions inside that range and nowhere else, so GC of the rest of the keyspace runs unimpeded.
// A single global watermark would pin all of it behind the one long reader; the split watermark does
// not.
func TestGCWatermarkOLAPPinsOnlyItsRange(t *testing.T) {
	g := newGCWatermark()
	// A long analytical scan reading at timestamp 50 over [m, t). It does not touch the OLTP watermark,
	// which stays at the maximum because no short reader is open.
	g.registerOLAP(50, []byte("m"), []byte("t"))
	if g.oltpWatermark() != maxHLCTime {
		t.Fatalf("an OLAP reader must not pull the OLTP watermark down")
	}

	// A version inside the range, superseded after the scan's read timestamp, is pinned: the scan has
	// not advanced to the superseding write and still needs this version. It is not reclaimable even
	// though it clears the OLTP gate.
	if g.reclaimable([]byte("p"), 60) {
		t.Fatalf("a version inside the OLAP range superseded after its read ts must be pinned")
	}
	if !g.pinnedByOLAP([]byte("p"), 60) {
		t.Fatalf("pinnedByOLAP should report the in-range version pinned")
	}

	// A version outside the range is untouched by the scan and reclaimable, which is the whole point:
	// the long reader does not stall GC across the keyspace.
	if !g.reclaimable([]byte("a"), 60) {
		t.Fatalf("a version below the range must not be pinned by the scan")
	}
	if !g.reclaimable([]byte("z"), 60) {
		t.Fatalf("a version above the range must not be pinned by the scan")
	}

	// A version inside the range but superseded at or before the scan's read timestamp is one the scan
	// has already advanced past, so it is not pinned.
	if !g.reclaimable([]byte("p"), 50) {
		t.Fatalf("an in-range version superseded at or before the scan's read ts is not pinned")
	}
}

func TestGCWatermarkOLAPReleaseUnpins(t *testing.T) {
	g := newGCWatermark()
	h := g.registerOLAP(50, []byte("m"), []byte("t"))
	if g.reclaimable([]byte("p"), 60) {
		t.Fatalf("in-range version should be pinned while the scan runs")
	}
	// The scan ends. Its range pin lifts and the version reclaims, so the freeze lasts only as long as
	// the scan.
	g.release(h)
	if !g.reclaimable([]byte("p"), 60) {
		t.Fatalf("releasing the OLAP reader must unpin its range")
	}
}

func TestGCWatermarkCompositionOLTPAndOLAP(t *testing.T) {
	g := newGCWatermark()
	// A short reader at 200 and a long scan at 50 over [m, t). Reclaimability needs both gates.
	g.registerOLTP(200)
	g.registerOLAP(50, []byte("m"), []byte("t"))

	// Inside the OLAP range, superseded at 60: clears the OLTP gate (60 <= 200) but the scan pins it.
	if g.reclaimable([]byte("p"), 60) {
		t.Fatalf("in-range version must be held by the OLAP pin despite clearing the OLTP gate")
	}
	// Outside the OLAP range, superseded at 60: clears both gates.
	if !g.reclaimable([]byte("a"), 60) {
		t.Fatalf("out-of-range version clearing the OLTP gate should reclaim")
	}
	// Outside the OLAP range, superseded at 250: fails the OLTP gate, so held regardless of the scan.
	if g.reclaimable([]byte("a"), 250) {
		t.Fatalf("version superseded after the OLTP watermark must be held")
	}
}

// TestGCWatermarkConcurrent stresses register/release/query under the race detector. Readers churn
// while a sampler reads the watermark and reclaimability; the registry must stay internally
// consistent (no torn reads, no leaked entries) and end empty after every reader releases.
func TestGCWatermarkConcurrent(t *testing.T) {
	g := newGCWatermark()
	const workers = 8
	const perWorker = 500
	var wg sync.WaitGroup
	var released atomic.Int64

	stop := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Add(1)
	go func() {
		defer sampler.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			// The watermark is always a valid timestamp; reclaimable never panics on a concurrent
			// registry. We are checking for races and torn state, not exact values here.
			_ = g.oltpWatermark()
			_ = g.reclaimable([]byte("p"), 60)
		}
	}()

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				var h uint64
				if (w+i)%2 == 0 {
					h = g.registerOLTP(hlcTime(100 + i))
				} else {
					h = g.registerOLAP(hlcTime(50), []byte("m"), []byte("t"))
				}
				g.release(h)
				released.Add(1)
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	sampler.Wait()

	if got := released.Load(); got != workers*perWorker {
		t.Fatalf("released %d, want %d", got, workers*perWorker)
	}
	// Every reader released, so the registry is empty and back to reclaiming everything.
	if g.oltpWatermark() != maxHLCTime {
		t.Fatalf("after all readers released, OLTP watermark is %d, want max", g.oltpWatermark())
	}
	if !g.reclaimable([]byte("p"), 60) {
		t.Fatalf("empty registry should reclaim everything")
	}
}
