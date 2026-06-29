package hashlog

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// These benchmarks travel with the durable code so a regression in the spill path
// shows up locally during the build. Their numbers are not published: per spec 2070
// doc 08 the only durable performance number that ships is the M10 real-hardware
// gate. The resident benchmark here is a guard that the memory-only ceiling did not
// move, not a headline figure.

func benchKey(i int) []byte {
	return []byte("k:" + strconv.Itoa(i))
}

func benchValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return b
}

// BenchmarkDurableSpillGet reads from a store whose resident budget is a fraction of
// its working set, so most reads fault a page in from the one file. This is the
// larger-than-memory read path M1 introduces.
func BenchmarkDurableSpillGet(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.hlog")
	t := Tunables{
		Shards:                8,
		PageSize:              4096,
		ExtentSize:            4096,
		ResidentPagesPerShard: 4,
		Path:                  path,
	}
	s, err := New(t)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	const keys = 50000
	v := benchValue(64)
	for i := 0; i < keys; i++ {
		if err := s.Set(benchKey(i), v); err != nil {
			b.Fatal(err)
		}
	}
	if s.Spilled() == 0 {
		b.Fatal("benchmark did not spill; not exercising the durable read path")
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := benchKey(i % keys)
			if _, _, err := s.Get(k); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// BenchmarkDurableDialSet measures the SET cost at each durability dial so the price
// of the barrier is visible: None pays nothing, Normal pays a barrier per seal, Full
// pays a true device flush before every SET returns. The gap between them is the dial
// doing its job, not a regression. Numbers stay local until the M10 hardware gate.
func BenchmarkDurableDialSet(b *testing.B) {
	dials := []struct {
		name string
		d    Durability
	}{
		{"None", DurabilityNone},
		{"Normal", DurabilityNormal},
		{"Full", DurabilityFull},
	}
	v := benchValue(64)
	for _, dl := range dials {
		b.Run(dl.name, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "dial.hlog")
			t := Tunables{
				Shards:                8,
				PageSize:              4096,
				ExtentSize:            4096,
				ResidentPagesPerShard: 8,
				Path:                  path,
				Durability:            dl.d,
			}
			s, err := New(t)
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.Set(benchKey(i), v); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFullSetFlushSuffix isolates L3: a Full SET flushes its tail and barriers,
// and flushDurable used to scan every page from page 0 to find the dirty suffix, so the
// per-SET CPU cost grew with the log's page count. The sub-benchmarks prefill the single
// shard to a fixed page count, then time fresh Full SETs: before the suffix fix the
// ns/op climbs with the prefill (more pages to scan), after it is flat. Eviction is off
// (a huge resident budget) so no page faults intrude, and the device barrier is stubbed
// to a no-op so the number is the page-scan CPU the fix targets, not F_FULLFSYNC latency
// (which would swamp it and make the prefill take minutes). Correctness of the real
// barrier is covered by the durability tests, not here.
func BenchmarkFullSetFlushSuffix(b *testing.B) {
	const pageSize = 256
	for _, pages := range []int{16, 512, 8192} {
		b.Run("pages="+strconv.Itoa(pages), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "flush.hlog")
			t := Tunables{
				Shards:                1,
				PageSize:              pageSize,
				ExtentSize:            pageSize,
				ResidentPagesPerShard: 1 << 24,
				Path:                  path,
				Durability:            DurabilityFull,
			}
			s, err := New(t)
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			s.df.syncHook = func(*os.File) error { return nil } // measure scan, not the barrier
			v := benchValue(8)
			sh := s.shards[0]
			i := 0
			for sh.tailPage < int64(pages) {
				if err := s.Set(benchKey(i), v); err != nil {
					b.Fatal(err)
				}
				i++
			}
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if err := s.Set(benchKey(i), v); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	}
}

// BenchmarkOverwrite isolates L2: an overwrite of an existing key repoints its index slot.
// Before the packed slot that allocated a fresh *entry per Set; now it is one atomic store
// with no allocation. A memory-only full-resident store never takes the same-size in-place
// tail path (that is the durable eviction profile), so every Set here is the slot-repoint
// path the audit flagged. The keys are pre-inserted and cycled, the value is small and the
// page large so page rolls are rare next to the per-op work, and ReportAllocs surfaces the
// allocation the fix removes. Numbers stay local until the M10 hardware gate.
func BenchmarkOverwrite(b *testing.B) {
	s, err := New(Tunables{Shards: 1, PageSize: 1 << 20, ResidentPagesPerShard: 0})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	const keys = 1024
	v := benchValue(16)
	for i := 0; i < keys; i++ {
		if err := s.Set(benchKey(i), v); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Set(benchKey(i%keys), v); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCheckpoint measures one checkpoint over a populated store: capture every
// shard's cut, encode the snapshot, write and barrier its extents, and flip the
// superblock. It is the cost of the periodic durability artifact, amortized over the
// CheckpointBytes of appends between checkpoints, so it is paid rarely. Reported per op
// where one op is one full checkpoint. Numbers stay local until the M10 hardware gate.
func BenchmarkCheckpoint(b *testing.B) {
	path := filepath.Join(b.TempDir(), "ckpt.hlog")
	t := Tunables{
		Shards:                8,
		PageSize:              4096,
		ExtentSize:            4096,
		ResidentPagesPerShard: 8,
		Path:                  path,
		Durability:            DurabilityNormal,
	}
	s, err := New(t)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	const keys = 50000
	v := benchValue(64)
	for i := 0; i < keys; i++ {
		if err := s.Set(benchKey(i), v); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Checkpoint(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCompact measures one compaction pass over a store that a full overwrite has
// left with wholly-dead sealed pages: select the dead extents, copy the live records each
// still holds to the tail, repoint the index, and retire them. The overwrite that
// manufactures the dead pages is excluded from the timer (StopTimer around it), so the
// number is the reclaim work itself, not the writes that created the garbage. It is the
// cost of the background space-reclamation pass M8 adds, paid off the write path. Numbers
// stay local until the M10 hardware gate.
func BenchmarkCompact(b *testing.B) {
	path := filepath.Join(b.TempDir(), "compact.hlog")
	t := Tunables{
		Shards:                8,
		PageSize:              4096,
		ExtentSize:            4096,
		ResidentPagesPerShard: 8,
		Path:                  path,
		Durability:            DurabilityNormal,
	}
	s, err := New(t)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	const keys = 50000
	for i := 0; i < keys; i++ {
		if err := s.Set(benchKey(i), benchValue(64)); err != nil {
			b.Fatal(err)
		}
	}
	if err := s.Checkpoint(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Overwrite every key with a different-size value so the prior versions go dead and
		// their pages cross the threshold. This is setup, not the work under test, so it runs
		// with the timer stopped.
		b.StopTimer()
		size := 48 + (i%8)*16 // varies per round so each overwrite appends and kills the old record
		v := benchValue(size)
		for k := 0; k < keys; k++ {
			if err := s.Set(benchKey(k), v); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()

		if err := s.Compact(); err != nil {
			b.Fatal(err)
		}

		// Fold in a checkpoint off the timer so the retired extents are freed and reused,
		// keeping the file from growing across rounds.
		b.StopTimer()
		if err := s.Checkpoint(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}

// BenchmarkOversize measures the SET and GET cost of a spanning value (M9, doc 03 section
// 7): a SET writes the cont chain then the home record, a GET reads the descriptor and
// reassembles the value from its cont extents with a CRC check. The value is large enough to
// span several cont extents, so the number is dominated by the byte movement the chain costs,
// not the index work. Like the rest of the durable suite the figure stays local until the M10
// hardware gate; it travels with the code as a regression guard on the spanning path.
func BenchmarkOversize(b *testing.B) {
	const valueBytes = 16384
	for _, op := range []string{"Set", "Get"} {
		b.Run(op, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "oversize.hlog")
			t := Tunables{
				Shards:                8,
				PageSize:              4096,
				ExtentSize:            4096,
				ResidentPagesPerShard: 8,
				Path:                  path,
				Durability:            DurabilityNormal,
			}
			s, err := New(t)
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			v := benchValue(valueBytes)

			if op == "Get" {
				const keys = 2000
				for i := 0; i < keys; i++ {
					if err := s.Set(benchKey(i), v); err != nil {
						b.Fatal(err)
					}
				}
				if s.OversizeValues() == 0 {
					b.Fatal("benchmark stored no oversize values")
				}
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						if _, _, err := s.Get(benchKey(i % keys)); err != nil {
							b.Fatal(err)
						}
						i++
					}
				})
				return
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.Set(benchKey(i), v); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkResidentCeilingGet is the memory-only path the durable work must never
// slow. It mirrors the resident config the head-to-head bench uses, so a drift here
// flags that the durable branch leaked cost into the full-resident GET.
func BenchmarkResidentCeilingGet(b *testing.B) {
	t := Tunables{Shards: 8, PageSize: 1 << 20, ResidentPagesPerShard: 0}
	s, err := New(t)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	const keys = 50000
	v := benchValue(64)
	for i := 0; i < keys; i++ {
		if err := s.Set(benchKey(i), v); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, _, err := s.Get(benchKey(i % keys)); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// BenchmarkStats measures the operability snapshot so its cost stays low enough to poll on
// a metrics cadence (audit A6, A7). The store holds many resident pages across 8 shards, so
// the walk touches every shard's read lock and sums its per-page dead and fill arrays, the
// work that scales with the store. It is a per-shard linear pass, not a per-key one, so it
// should stay microsecond-class regardless of key count.
func BenchmarkStats(b *testing.B) {
	t := Tunables{Shards: 8, PageSize: 1 << 16, ResidentPagesPerShard: 0}
	s, err := New(t)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	const keys = 100000
	v := benchValue(64)
	for i := 0; i < keys; i++ {
		if err := s.Set(benchKey(i), v); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st := s.Stats()
		if st.LiveKeys != keys {
			b.Fatalf("LiveKeys = %d, want %d", st.LiveKeys, keys)
		}
	}
}
