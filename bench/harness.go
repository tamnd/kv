package bench

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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
		Engine:    engine,
		Dir:       dir,
		PageSize:  4096,
		Sync:      kv.SyncFull,
		KeyCount:  20000,
		KeyLen:    24,
		ValLen:    64,
		Ops:       20000,
		BatchSize: 100,
		Seed:      1,
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
	if e == kv.LSM {
		return "lsm"
	}
	return "btree"
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

	opts := []kv.Option{kv.WithEngine(cfg.Engine), kv.WithSynchronous(cfg.Sync)}
	if cfg.PageSize > 0 {
		opts = append(opts, kv.WithPageSize(cfg.PageSize))
	}
	db, err := kv.Open(path, opts...)
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

// runPhase performs cfg.Ops operations of workload w against a loaded database, timing each
// and bucketing its latency into the read or write histogram. It samples the pager's
// physical-read counter across the window so the read amplification denominator (logical
// reads) and numerator (page reads) come from the same window, not from the load phase.
func runPhase(db *kv.DB, cfg Config, w Workload) (runResult, error) {
	rr := runResult{
		reads:  NewHistogram(cfg.Ops),
		writes: NewHistogram(cfg.Ops),
	}
	// A separate draw stream for keys and for the read/write coin, both seeded, so the op
	// mix is reproducible. Offsetting the coin's seed keeps it independent of the key draw.
	keyGen := NewGenerator(GenConfig{KeyCount: cfg.KeyCount, KeyLen: cfg.KeyLen, ValLen: cfg.ValLen, Dist: w.Dist, Seed: cfg.Seed})
	coin := NewGenerator(GenConfig{KeyCount: 1 << 20, KeyLen: minKeyLen, ValLen: minValLen, Dist: Uniform, Seed: cfg.Seed + 1})

	var kbuf []byte
	pageReads0 := db.Stats().PageReads
	gcStart := readGC()
	start := time.Now()
	for n := 0; n < cfg.Ops; n++ {
		idx := keyGen.nextIndex()
		kbuf = keyGen.Key(kbuf, idx)
		coinVal := float64(coin.nextIndex()) / float64(1<<20)

		switch {
		case w.RMW:
			if e := rmwOp(db, kbuf, keyGen, idx, rr.reads, rr.writes); e != nil {
				return rr, e
			}
			rr.readOps++ // an RMW reads before it writes
		case w.ScanLength > 0:
			if e := scanOp(db, kbuf, w.ScanLength, rr.reads); e != nil {
				return rr, e
			}
			rr.readOps++ // a scan is one logical read that touches many keys
		case w.isWrite(coinVal):
			if e := writeOp(db, kbuf, keyGen.Value(idx), rr.writes); e != nil {
				return rr, e
			}
		default:
			if e := readOp(db, kbuf, rr.reads); e != nil {
				return rr, e
			}
			rr.readOps++
		}
		rr.ops++
	}
	rr.dur = time.Since(start)
	rr.gc = readGC().diff(gcStart)
	rr.pageReadDelta = db.Stats().PageReads - pageReads0
	return rr, nil
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

// writeOp times a single committed update of one key.
func writeOp(db *kv.DB, key, val []byte, h *Histogram) error {
	start := time.Now()
	err := db.Update(func(txn *kv.Txn) error { return txn.Set(key, val) })
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
	d := time.Since(start)
	// An RMW touches both paths; charge it to both histograms so neither tail is hidden.
	reads.Record(d)
	writes.Record(d)
	return err
}

// scanOp times a forward range scan of up to length keys starting at key, the YCSB-E
// iteration pattern.
func scanOp(db *kv.DB, key []byte, length int, h *Histogram) error {
	start := time.Now()
	err := db.View(func(txn *kv.Txn) error {
		it, e := txn.NewIterator(kv.IterOptions{})
		if e != nil {
			return e
		}
		defer it.Close()
		seen := 0
		for ok := it.SeekGE(key); ok && seen < length; ok = it.Next() {
			if _, e := it.Value(); e != nil {
				return e
			}
			seen++
		}
		return it.Error()
	})
	h.Record(time.Since(start))
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
