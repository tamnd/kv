package bench

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/tamnd/kv"
)

// Config parameterizes a benchmark run: which engine, how big the keyspace and the
// key/value widths, how many measured operations, the durability level, and the seed that
// makes it reproducible. The zero value is not useful; use DefaultConfig and adjust.
type Config struct {
	// Engine is the storage core under test (kv.BTree or kv.LSM). Every workload runs on
	// both so the read/write tradeoff is quantified, not asserted (spec 21 §2).
	Engine kv.EngineKind
	// Dir is a writable directory the harness creates the database file in. The caller owns
	// it (a test passes b.TempDir); the harness measures the whole directory's size for the
	// space footprint, so nothing else should live in it.
	Dir string
	// PageSize is the database page size; zero uses the kv default.
	PageSize int
	// CacheBytes caps the buffer pool. Zero uses the engine default. Setting it well below the
	// working set forces the out-of-cache regime where reads miss to disk and read
	// amplification becomes visible (spec 21 §3), the regime that distinguishes a B-tree's page
	// discipline from an LSM's read cost.
	CacheBytes int
	// Sync is the WAL durability level the run discloses and pays for (spec 21 §3, §6).
	Sync kv.Sync

	// KeyCount is the number of distinct keys the load phase writes and the run phase draws
	// over. KeyLen and ValLen are the fixed encoded widths.
	KeyCount int
	KeyLen   int
	ValLen   int

	// Ops is the number of measured operations in the run phase. Ignored for a bulk-load
	// workload, whose measured window is the load itself.
	Ops int
	// Concurrency is how many goroutines drive the run phase in parallel, each with its own
	// seeded draw stream. Zero or one runs the phase serially, the default the regression gate
	// uses for a stable single-threaded number; a higher value measures throughput and tail
	// latency under contention (spec 21 §3). It does not affect the bulk-load workload.
	Concurrency int
	// BatchSize is how many writes share one transaction in the load phase and in
	// write-heavy run phases; batching is what lets the LSM flush real multi-page segments.
	BatchSize int
	// Seed fixes the PRNG so the draw sequence is identical across runs (spec 21 §5).
	Seed int64

	// Profile, when its Dir is set, captures pprof profiles around the measured window so a
	// regression is diagnosable (spec 21 §5). It is off by default, which is what the
	// regression gate and the microbenchmarks use, because profiling perturbs the timings they
	// measure.
	Profile ProfileSet
}

// DefaultConfig returns a modest cache-resident configuration good for a CI smoke run: a
// keyspace and op count small enough to finish in well under a second yet large enough that
// the LSM flushes real segments. Callers scale KeyCount/Ops up for a real measurement.
func DefaultConfig(engine kv.EngineKind, dir string) Config {
	return Config{
		Engine:      engine,
		Dir:         dir,
		PageSize:    4096,
		Sync:        kv.SyncFull,
		KeyCount:    20000,
		KeyLen:      24,
		ValLen:      64,
		Ops:         20000,
		Concurrency: 1,
		BatchSize:   100,
		Seed:        1,
	}
}

// syncName labels a durability level for the disclosed setup.
func syncName(s kv.Sync) string {
	switch s {
	case kv.SyncOff:
		return "off"
	case kv.SyncNormal:
		return "normal"
	case kv.SyncExtra:
		return "extra"
	default:
		return "full"
	}
}

func engineName(e kv.EngineKind) string {
	switch e {
	case kv.LSM:
		return "lsm"
	case kv.Beta:
		return "betree"
	default:
		return "btree"
	}
}

// openOptions builds the kv.Open options a config asks for: engine, durability, and the
// optional page-size and cache-size overrides. It is the single place the harness translates a
// Config into open options, so the load and any reopen see exactly the same database shape.
func openOptions(cfg Config) []kv.Option {
	opts := []kv.Option{kv.WithEngine(cfg.Engine), kv.WithSynchronous(cfg.Sync)}
	if cfg.PageSize > 0 {
		opts = append(opts, kv.WithPageSize(cfg.PageSize))
	}
	if cfg.CacheBytes > 0 {
		opts = append(opts, kv.WithCacheSize(cfg.CacheBytes))
	}
	return opts
}

// Run executes one workload under cfg and returns its measured Result. It opens a fresh
// database in cfg.Dir, runs the load phase, settles the engine to a steady file shape, then
// measures the run phase (or the load itself for a bulk-load workload). It is the single
// entry point both the microbenchmarks and the regression gate call.
func Run(cfg Config, w Workload) (Result, error) {
	if cfg.BatchSize < 1 {
		cfg.BatchSize = 1
	}
	path := filepath.Join(cfg.Dir, "bench.kv")

	db, err := kv.Open(path, openOptions(cfg)...)
	if err != nil {
		return Result{}, fmt.Errorf("open: %w", err)
	}
	defer db.Close()

	setup := Setup{
		GoVersion:    runtime.Version(),
		GOOS:         runtime.GOOS,
		GOARCH:       runtime.GOARCH,
		NumCPU:       runtime.NumCPU(),
		KeyCount:     cfg.KeyCount,
		KeyLen:       cfg.KeyLen,
		ValLen:       cfg.ValLen,
		Distribution: distName(w.Dist),
		Seed:         cfg.Seed,
		Synchronous:  syncName(cfg.Sync),
		BatchSize:    cfg.BatchSize,
		Concurrency:  concurrencyOf(cfg),
		CacheBytes:   cfg.CacheBytes,
	}
	res := Result{Workload: w.Name, Engine: engineName(cfg.Engine), Setup: setup}

	// Logical bytes ingested by the load, the denominator of the write factor.
	loadGen := NewGenerator(GenConfig{KeyCount: cfg.KeyCount, KeyLen: cfg.KeyLen, ValLen: cfg.ValLen, Dist: Sequential, Seed: cfg.Seed})
	ingested := int64(cfg.KeyCount) * int64(loadGen.BytesPerOp())

	// Read-amplification inputs, filled by the run phase (a bulk-load workload leaves them
	// zero, so its read amplification stays the not-measured sentinel).
	var readOps int64
	var pageReadDelta uint64

	profLabel := engineName(cfg.Engine) + "-" + w.Name

	if w.MeasureLoad {
		// The bulk-load workload measures the load itself: time and GC are captured around it,
		// and the profiler wraps exactly that window.
		prof, err := startProfiling(cfg.Profile, profLabel)
		if err != nil {
			return Result{}, err
		}
		gcStart := readGC()
		start := time.Now()
		writes, err := loadPhase(db, loadGen, cfg.KeyCount, cfg.BatchSize, true)
		dur := time.Since(start)
		gcEnd := readGC()
		if perr := prof.stop(); perr != nil {
			return Result{}, perr
		}
		if err != nil {
			return Result{}, err
		}
		res.Ops = int64(cfg.KeyCount)
		res.Duration = dur
		res.Writes = writes.Summary()
		res.GC = gcEnd.diff(gcStart)
		res.Throughput = opsPerSec(res.Ops, dur)
	} else {
		// Every other workload loads first (unmeasured), settles, then measures the run phase.
		if _, err := loadPhase(db, loadGen, cfg.KeyCount, cfg.BatchSize, false); err != nil {
			return Result{}, err
		}
		if err := settle(db); err != nil {
			return Result{}, err
		}
		prof, err := startProfiling(cfg.Profile, profLabel)
		if err != nil {
			return Result{}, err
		}
		rr, err := runPhase(db, cfg, w)
		if perr := prof.stop(); perr != nil {
			return Result{}, perr
		}
		if err != nil {
			return Result{}, err
		}
		res.Ops = rr.ops
		res.Dropped = rr.dropped
		res.Duration = rr.dur
		res.Reads = rr.reads.Summary()
		res.Writes = rr.writes.Summary()
		res.GC = rr.gc
		res.Throughput = opsPerSec(rr.ops, rr.dur)
		readOps = rr.readOps
		pageReadDelta = rr.pageReadDelta
	}

	// Settle once more so the file footprint reflects folded, compacted state, then read the
	// amplification triple. Read amplification comes from the run phase's own page-read delta
	// over its logical reads; the space and write factors come from the settled file.
	if err := settle(db); err != nil {
		return Result{}, err
	}
	res.Amplification = amplification(db, cfg.Dir, ingested)
	if readOps > 0 {
		res.Amplification.Read = float64(pageReadDelta) / float64(readOps)
	}
	return res, nil
}

// loadPhase writes count keys in sorted order, batched, and (when measure) records each
// commit's latency. Batching matters: a flush only fires on the Apply after the memtable
// crosses its cap, so single-op transactions would never build a real segment.
func loadPhase(db *kv.DB, gen *Generator, count, batch int, measure bool) (*Histogram, error) {
	h := NewHistogram(0)
	if measure {
		h = NewHistogram(count)
	}
	var kbuf []byte
	for lo := 0; lo < count; lo += batch {
		hi := lo + batch
		if hi > count {
			hi = count
		}
		start := time.Now()
		err := db.Update(func(txn *kv.Txn) error {
			for i := lo; i < hi; i++ {
				kbuf = gen.Key(kbuf, uint64(i))
				if err := txn.Set(kbuf, gen.Value(uint64(i))); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("load batch [%d,%d): %w", lo, hi, err)
		}
		if measure {
			// Charge the batch's commit latency evenly across its operations, so a per-op
			// latency reflects the amortized cost of a batched write.
			per := time.Since(start) / time.Duration(hi-lo)
			for i := lo; i < hi; i++ {
				h.Record(per)
			}
		}
	}
	return h, nil
}

// settle folds the WAL into the main file and lets the engine reach a stable on-disk shape,
// so a later size read reflects the compacted footprint rather than a transient pile of WAL
// frames. A full steady-state equilibrium (compaction quiesced) is a later refinement; a
// checkpoint is the honest, available approximation today.
func settle(db *kv.DB) error {
	if err := db.Checkpoint(); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	return nil
}

// runResult is the run phase's measured output: the read and write latency histograms, the
// op and dropped counts, the wall-clock window, the window's GC cost, and the read-side I/O
// the read amplification ratio needs (logical read ops and the physical page reads the pager
// issued to serve them).
type runResult struct {
	reads, writes *Histogram
	ops, dropped  int64
	readOps       int64
	pageReadDelta uint64
	dur           time.Duration
	gc            GCStats
}

// concurrencyOf is the effective worker count: at least one.
func concurrencyOf(cfg Config) int {
	if cfg.Concurrency < 1 {
		return 1
	}
	return cfg.Concurrency
}

// splitOps divides total ops as evenly as possible across workers, handing the remainder to
// the first few so the counts sum back to total exactly.
func splitOps(total, workers int) []int {
	out := make([]int, workers)
	base, rem := total/workers, total%workers
	for i := range out {
		out[i] = base
		if i < rem {
			out[i]++
		}
	}
	return out
}

// runPhase performs cfg.Ops operations of workload w against a loaded database, optionally
// spread across cfg.Concurrency goroutines, timing each op and bucketing its latency into the
// read or write histogram. It samples the pager's physical-read counter, the GC counters, and
// the wall clock across the whole parallel window, so the read amplification, the GC cost, and
// the throughput all describe the same span the latencies were drawn from, not the load phase.
func runPhase(db *kv.DB, cfg Config, w Workload) (runResult, error) {
	rr := runResult{reads: NewHistogram(cfg.Ops), writes: NewHistogram(cfg.Ops)}
	pageReads0 := db.Stats().PageReads
	gcStart := readGC()
	start := time.Now()

	// One window, two drivers. A read-latest workload runs on a single goroutine because its
	// head is shared mutable state; everything else fans out across cfg.Concurrency workers.
	var err error
	if w.ReadLatest {
		err = runReadLatestInto(&rr, db, cfg, w)
	} else {
		err = runConcurrentInto(&rr, db, cfg, w)
	}
	if err != nil {
		return rr, err
	}

	rr.dur = time.Since(start)
	rr.gc = readGC().diff(gcStart)
	rr.pageReadDelta = db.Stats().PageReads - pageReads0
	return rr, nil
}

// runConcurrentInto drives the standard read/write/RMW workloads, optionally spread across
// cfg.Concurrency goroutines, and folds every worker's histograms and counts into rr. With one
// worker it runs inline; with more it fans out and merges under a barrier so the run's
// percentiles are true global percentiles.
func runConcurrentInto(rr *runResult, db *kv.DB, cfg Config, w Workload) error {
	workers := concurrencyOf(cfg)
	per := splitOps(cfg.Ops, workers)

	if workers == 1 {
		wr, err := runWorker(db, cfg, w, 0, per[0])
		if err != nil {
			return err
		}
		mergeWorker(rr, wr)
		return nil
	}

	results := make([]workerResult, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = runWorker(db, cfg, w, i, per[i])
		}(i)
	}
	wg.Wait()
	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			return errs[i]
		}
		mergeWorker(rr, results[i])
	}
	return nil
}

// runReadLatestInto drives the YCSB-D read-latest pattern against a growing keyspace (spec 21
// §2). It runs on a single goroutine because the head, the index the next insert takes, is
// shared mutable state that a concurrent fan-out would race on. An op inserts a fresh key at
// the head with probability w.InsertFraction, extending the keyspace; otherwise it reads a
// recent key, drawn by a Zipfian offset back from the head so the most recently inserted keys
// are the hottest. The offset draws over [0, KeyCount-1] and head starts at KeyCount, so the
// computed index is always in range and the read window is the KeyCount most recent keys.
func runReadLatestInto(rr *runResult, db *kv.DB, cfg Config, w Workload) error {
	wr := workerResult{reads: NewHistogram(cfg.Ops), writes: NewHistogram(cfg.Ops)}
	// One Zipfian stream serves both the offset draw and the key/value encoding (encoding does
	// not advance the PRNG), and a separate uniform stream is the insert/read coin.
	gen := NewGenerator(GenConfig{KeyCount: cfg.KeyCount, KeyLen: cfg.KeyLen, ValLen: cfg.ValLen, Dist: Zipfian, Seed: cfg.Seed})
	coin := NewGenerator(GenConfig{KeyCount: 1 << 20, KeyLen: minKeyLen, ValLen: minValLen, Dist: Uniform, Seed: cfg.Seed + 1})

	head := uint64(cfg.KeyCount) // the index the next insert will take
	var kbuf []byte
	for n := 0; n < cfg.Ops; n++ {
		coinVal := float64(coin.nextIndex()) / float64(1<<20)
		if coinVal < w.InsertFraction {
			// Insert a brand-new key at the head, extending the keyspace by one.
			kbuf = gen.Key(kbuf, head)
			if e := writeOp(db, kbuf, gen.Value(head), wr.writes); e != nil {
				if e == kv.ErrConflict {
					wr.dropped++
					continue
				}
				return e
			}
			head++
		} else {
			// Read a recent key: offset 0 is the latest insert, which the Zipfian head favors.
			idx := head - 1 - gen.nextIndex()
			kbuf = gen.Key(kbuf, idx)
			if e := readOp(db, kbuf, wr.reads); e != nil {
				return e
			}
			wr.logicalRead++
		}
		wr.ops++
	}
	mergeWorker(rr, wr)
	return nil
}

// workerResult is one goroutine's slice of the run phase: its own latency histograms and its
// own op, dropped, and logical-read counts, all merged into the run's totals afterward.
type workerResult struct {
	reads, writes             *Histogram
	ops, dropped, logicalRead int64
}

// mergeWorker folds a worker's result into the run total. Histograms merge sample-for-sample
// so the run's percentiles are the true percentiles across all workers, not an average of
// per-worker percentiles.
func mergeWorker(rr *runResult, wr workerResult) {
	rr.reads.Merge(wr.reads)
	rr.writes.Merge(wr.writes)
	rr.ops += wr.ops
	rr.dropped += wr.dropped
	rr.readOps += wr.logicalRead
}

// runWorker drives ops operations of workload w on its own seeded draw streams. Each worker
// gets a distinct seed so the workers do not all replay the same key sequence in lockstep,
// while staying reproducible run to run. A write that loses its retry race under contention
// surfaces as kv.ErrConflict and is counted as a dropped op, not a fatal error.
func runWorker(db *kv.DB, cfg Config, w Workload, worker, ops int) (workerResult, error) {
	wr := workerResult{reads: NewHistogram(ops), writes: NewHistogram(ops)}
	// A separate draw stream for keys and for the read/write coin, both seeded, so the op mix
	// is reproducible. The per-worker offset keeps each goroutine's stream distinct, and the
	// coin's offset keeps it independent of the key draw.
	seed := cfg.Seed + int64(worker)*0x9e3779b1
	keyGen := NewGenerator(GenConfig{KeyCount: cfg.KeyCount, KeyLen: cfg.KeyLen, ValLen: cfg.ValLen, Dist: w.Dist, Seed: seed})
	coin := NewGenerator(GenConfig{KeyCount: 1 << 20, KeyLen: minKeyLen, ValLen: minValLen, Dist: Uniform, Seed: seed + 1})

	var kbuf []byte
	for n := 0; n < ops; n++ {
		idx := keyGen.nextIndex()
		kbuf = keyGen.Key(kbuf, idx)
		coinVal := float64(coin.nextIndex()) / float64(1<<20)

		switch {
		case w.RMW:
			if e := rmwOp(db, kbuf, keyGen, idx, wr.reads, wr.writes); e != nil {
				if e == kv.ErrConflict {
					wr.dropped++
					continue
				}
				return wr, e
			}
			wr.logicalRead++ // an RMW reads before it writes
		case w.isWrite(coinVal):
			if e := writeOp(db, kbuf, keyGen.Value(idx), wr.writes); e != nil {
				if e == kv.ErrConflict {
					wr.dropped++
					continue
				}
				return wr, e
			}
		default:
			if e := readOp(db, kbuf, wr.reads); e != nil {
				return wr, e
			}
			wr.logicalRead++
		}
		wr.ops++
	}
	return wr, nil
}

// readOp times a single point lookup. A miss is not an error: under a Zipfian draw over a
// fully loaded keyspace every key is present, but the timing is what matters, present or not.
func readOp(db *kv.DB, key []byte, h *Histogram) error {
	start := time.Now()
	err := db.View(func(txn *kv.Txn) error {
		_, e := txn.Get(key)
		if e != nil && e != kv.ErrNotFound {
			return e
		}
		return nil
	})
	h.Record(time.Since(start))
	return err
}

// writeOp times a single committed update of one key. Under concurrency Update may still lose
// a write-write race after exhausting its retry bound; that surfaces as kv.ErrConflict, which
// the caller counts as a dropped op rather than a fatal error. A dropped write records no
// latency sample, so the histogram holds only ops that actually committed.
func writeOp(db *kv.DB, key, val []byte, h *Histogram) error {
	start := time.Now()
	err := db.Update(func(txn *kv.Txn) error { return txn.Set(key, val) })
	if err == kv.ErrConflict {
		return err
	}
	h.Record(time.Since(start))
	return err
}

// rmwOp times a read-modify-write: one transaction reads the key then writes a new value
// derived from it, the YCSB-F pattern that exercises read-own-write inside a writable txn.
func rmwOp(db *kv.DB, key []byte, gen *Generator, idx uint64, reads, writes *Histogram) error {
	start := time.Now()
	err := db.Update(func(txn *kv.Txn) error {
		if _, e := txn.Get(key); e != nil && e != kv.ErrNotFound {
			return e
		}
		return txn.Set(key, gen.Value(idx))
	})
	if err == kv.ErrConflict {
		return err
	}
	d := time.Since(start)
	// An RMW touches both paths; charge it to both histograms so neither tail is hidden.
	reads.Record(d)
	writes.Record(d)
	return err
}

// amplification reads the engine's space accounting and computes the storage write factor
// from the directory's total footprint. Read amplification is left unmeasured (see the
// Amplification.Read doc).
func amplification(db *kv.DB, dir string, ingested int64) Amplification {
	st := db.Stats()
	space := st.Amplification
	if space == 0 && st.LiveBytes > 0 {
		space = float64(st.PhysicalBytes) / float64(st.LiveBytes)
	}
	var write float64
	if ingested > 0 {
		write = float64(dirSize(dir)) / float64(ingested)
	}
	return Amplification{Space: space, Write: write, Read: readNotMeasured}
}

// dirSize sums the bytes of every regular file in dir, the database's full on-disk
// footprint including any WAL sidecar. The harness owns the directory, so this is the
// database's footprint and nothing else.
func dirSize(dir string) int64 {
	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total
}

// opsPerSec is throughput, guarding the zero-duration edge a trivially small run can hit.
func opsPerSec(ops int64, dur time.Duration) float64 {
	if dur <= 0 {
		return 0
	}
	return float64(ops) / dur.Seconds()
}
