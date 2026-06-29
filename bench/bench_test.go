package bench

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/tamnd/kv"
)

// smokeConfig is a tiny but realistic configuration the invariant gate and the unit tests
// use: small enough to run all workloads on both engines in a fraction of a second, large
// enough that the LSM flushes real segments and the amplification numbers are meaningful.
func smokeConfig(engine kv.EngineKind, dir string) Config {
	c := DefaultConfig(engine, dir)
	c.KeyCount = 2000
	c.Ops = 2000
	return c
}

// TestRunInvariants is the per-PR perf gate. It does not assert an absolute throughput,
// because that depends on the machine and would be flaky in CI; the absolute SLO gate runs
// on fixed hardware in kvbench (spec 21 §5). What it asserts here is that the harness itself
// is honest on every workload and both engines: it measures every operation (none silently
// dropped, spec 21 §3), its latency percentiles are well ordered, it computes the
// amplification it can and flags the one it cannot, and the result round-trips through the
// JSON the regression tracker reads.
func TestRunInvariants(t *testing.T) {
	for _, engine := range []kv.EngineKind{kv.BTree, kv.LSM} {
		for _, w := range Standard() {
			w := w
			name := engineName(engine) + "/" + w.Name
			t.Run(name, func(t *testing.T) {
				res, err := Run(smokeConfig(engine, t.TempDir()), w)
				if err != nil {
					t.Fatalf("run: %v", err)
				}
				if res.Ops <= 0 {
					t.Fatalf("no operations measured")
				}
				if res.Dropped != 0 {
					t.Fatalf("operations dropped: %d (a throughput number with hidden drops is dishonest)", res.Dropped)
				}
				if res.Duration <= 0 {
					t.Fatalf("non-positive measured duration %v", res.Duration)
				}
				if res.Throughput <= 0 {
					t.Fatalf("non-positive throughput %v", res.Throughput)
				}
				checkLatency(t, "reads", res.Reads)
				checkLatency(t, "writes", res.Writes)

				// At least one of the two op kinds must have been exercised.
				if res.Reads.Count == 0 && res.Writes.Count == 0 {
					t.Fatalf("neither reads nor writes recorded")
				}
				// A read-only workload must record reads; a write-only one must record writes.
				if w.ReadFraction == 1.0 && !w.RMW && res.Reads.Count == 0 {
					t.Fatalf("read-only workload recorded no reads")
				}
				if w.ReadFraction == 0.0 && res.Writes.Count == 0 {
					t.Fatalf("write-only workload recorded no writes")
				}

				amp := res.Amplification
				if amp.Space < 0 {
					t.Fatalf("negative space amplification %v", amp.Space)
				}
				if amp.Write <= 0 {
					t.Fatalf("non-positive write factor %v (the file should hold the data)", amp.Write)
				}
				// A workload with reads reports a real, non-negative read amplification from
				// the pager counter; a write-only workload leaves the not-measured sentinel.
				hasReads := w.ReadFraction > 0 || w.RMW || w.ReadLatest
				if hasReads {
					if amp.Read < 0 {
						t.Fatalf("read workload should report read amplification, got sentinel %v", amp.Read)
					}
				} else if amp.Read != readNotMeasured {
					t.Fatalf("write-only workload should leave the not-measured sentinel %v, got %v", readNotMeasured, amp.Read)
				}

				// The result must round-trip through JSON unchanged, since that JSON is the
				// durable record the regression gates diff over time.
				raw, err := res.JSON()
				if err != nil {
					t.Fatalf("marshal: %v", err)
				}
				var back Result
				if err := json.Unmarshal(raw, &back); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}
				if back.Workload != res.Workload || back.Engine != res.Engine || back.Ops != res.Ops {
					t.Fatalf("json round trip lost fields: %s", raw)
				}
			})
		}
	}
}

// checkLatency asserts a latency summary is internally consistent: the percentiles are
// non-decreasing and bracketed by min and max. An empty summary (the op kind did not run)
// is vacuously fine.
func checkLatency(t *testing.T, label string, l Latency) {
	t.Helper()
	if l.Count == 0 {
		return
	}
	order := []time.Duration{l.Min, l.P50, l.P90, l.P99, l.P999, l.Max}
	for i := 1; i < len(order); i++ {
		if order[i] < order[i-1] {
			t.Fatalf("%s latency percentiles out of order at %d: %v", label, i, order)
		}
	}
}

// TestGeneratorDeterministic checks that the seed alone fixes the draw sequence and the
// key/value bytes, the property that makes a run reproducible across machines and time
// (spec 21 §5).
func TestGeneratorDeterministic(t *testing.T) {
	cfg := GenConfig{KeyCount: 1000, KeyLen: 24, ValLen: 48, Dist: Zipfian, Seed: 42}
	a := NewGenerator(cfg)
	b := NewGenerator(cfg)
	var ka, kb []byte
	for i := 0; i < 5000; i++ {
		ia := a.nextIndex()
		ib := b.nextIndex()
		if ia != ib {
			t.Fatalf("draw %d differs: %d vs %d", i, ia, ib)
		}
		ka = a.Key(ka, ia)
		kb = b.Key(kb, ib)
		if !bytes.Equal(ka, kb) {
			t.Fatalf("key %d differs", i)
		}
		if !bytes.Equal(a.Value(ia), b.Value(ib)) {
			t.Fatalf("value %d differs", i)
		}
	}
}

// TestGeneratorWidthsAndRange checks keys and values come out at the configured widths and
// every drawn index stays inside the keyspace, for all three distributions.
func TestGeneratorWidthsAndRange(t *testing.T) {
	for _, d := range []Distribution{Sequential, Uniform, Zipfian} {
		g := NewGenerator(GenConfig{KeyCount: 500, KeyLen: 20, ValLen: 40, Dist: d, Seed: 7})
		var kbuf []byte
		for i := 0; i < 2000; i++ {
			idx := g.nextIndex()
			if idx >= uint64(g.KeyCount()) {
				t.Fatalf("dist %d drew out-of-range index %d (keyspace %d)", d, idx, g.KeyCount())
			}
			kbuf = g.Key(kbuf, idx)
			if len(kbuf) != 20 {
				t.Fatalf("dist %d key width %d, want 20", d, len(kbuf))
			}
			if len(g.Value(idx)) != 40 {
				t.Fatalf("dist %d value width %d, want 40", d, len(g.Value(idx)))
			}
		}
	}
}

// TestGeneratorClampsWidths checks sub-minimum widths are clamped, not honored, so a
// degenerate config still produces a usable key big enough to hold its index.
func TestGeneratorClampsWidths(t *testing.T) {
	g := NewGenerator(GenConfig{KeyCount: 10, KeyLen: 1, ValLen: 1, Dist: Sequential, Seed: 1})
	k := g.Key(nil, 3)
	if len(k) < minKeyLen {
		t.Fatalf("key width %d below clamp %d", len(k), minKeyLen)
	}
	if len(g.Value(3)) < minValLen {
		t.Fatalf("value width %d below clamp %d", len(g.Value(3)), minValLen)
	}
}

// TestHistogramPercentiles checks the nearest-rank percentiles against a known sample set:
// 1..100ns, where the p-quantile is the ceil(p*100)th value.
func TestHistogramPercentiles(t *testing.T) {
	h := NewHistogram(100)
	for i := 1; i <= 100; i++ {
		h.Record(time.Duration(i))
	}
	s := h.Summary()
	cases := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"min", s.Min, 1},
		{"p50", s.P50, 50},
		{"p90", s.P90, 90},
		{"p99", s.P99, 99},
		{"p999", s.P999, 100},
		{"max", s.Max, 100},
		{"mean", s.Mean, 50}, // (1+..+100)/100 = 50.5 truncated to 50
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Fatalf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if s.Count != 100 {
		t.Fatalf("count %d, want 100", s.Count)
	}
}

// TestHistogramEmpty checks an empty histogram summarizes to a zero Latency rather than
// panicking on the percentile math.
func TestHistogramEmpty(t *testing.T) {
	if got := NewHistogram(0).Summary(); got.Count != 0 || got.Max != 0 {
		t.Fatalf("empty summary not zero: %+v", got)
	}
}

// runMicro drives one workload on one engine through the harness, scaling the run phase to
// b.N, and reports the harness's own throughput and read/write p99 as custom metrics. Go's
// default ns/op includes the fixed load and settle and is not the figure to read; the
// reported kvops/s and p99 are. The competitive, fixed-hardware numbers live in kvbench.
func runMicro(b *testing.B, engine kv.EngineKind, w Workload) {
	b.Helper()
	cfg := smokeConfig(engine, b.TempDir())
	cfg.Ops = b.N
	res, err := Run(cfg, w)
	if err != nil {
		b.Fatalf("run: %v", err)
	}
	b.ReportMetric(res.Throughput, "kvops/s")
	if res.Reads.Count > 0 {
		b.ReportMetric(float64(res.Reads.P99.Nanoseconds()), "read-p99-ns")
	}
	if res.Writes.Count > 0 {
		b.ReportMetric(float64(res.Writes.P99.Nanoseconds()), "write-p99-ns")
	}
}

func BenchmarkPointReadBTree(b *testing.B) {
	runMicro(b, kv.BTree, Workload{Name: "ycsb-c", Dist: Zipfian, ReadFraction: 1})
}
func BenchmarkPointReadLSM(b *testing.B) {
	runMicro(b, kv.LSM, Workload{Name: "ycsb-c", Dist: Zipfian, ReadFraction: 1})
}

func BenchmarkWriteBTree(b *testing.B) {
	runMicro(b, kv.BTree, Workload{Name: "write", Dist: Uniform, ReadFraction: 0})
}
func BenchmarkWriteLSM(b *testing.B) {
	runMicro(b, kv.LSM, Workload{Name: "write", Dist: Uniform, ReadFraction: 0})
}

func BenchmarkMixedBTree(b *testing.B) {
	runMicro(b, kv.BTree, Workload{Name: "ycsb-a", Dist: Zipfian, ReadFraction: 0.5})
}
func BenchmarkMixedLSM(b *testing.B) {
	runMicro(b, kv.LSM, Workload{Name: "ycsb-a", Dist: Zipfian, ReadFraction: 0.5})
}
