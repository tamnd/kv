package bench

import (
	"encoding/json"
	"runtime"
	"time"
)

// Amplification is the RUM triple, the heart of why this suite exists. The RUM conjecture
// says you cannot minimize read, update, and memory (space) overhead at once: an engine
// that is cheap on one pays on another. A benchmark that reports only throughput hides
// which corner the tax landed in, so every result carries all three amplifications next to
// the throughput and latency, for whichever engine ran (spec 21 §1).
type Amplification struct {
	// Space is the on-disk footprint over the live logical bytes (physical / live). It is
	// read straight from the engine's own space accounting (spec 09), the same number the
	// vacuum driver uses, so it reflects dead versions and freelist holes honestly.
	Space float64 `json:"space"`
	// Write is the storage write factor: the database's final on-disk size over the logical
	// bytes ingested. It is a footprint ratio, not the cumulative bytes-written-to-device
	// that a true write-amplification number reports; the cumulative counter needs an engine
	// I/O meter a later slice adds. This footprint factor is what is honestly measurable from
	// the public surface today, and it is labeled as such rather than dressed up as the real
	// write amp.
	Write float64 `json:"write_factor"`
	// Read is read amplification: physical page reads the pager issued to serve the run
	// phase over the logical read operations the workload performed (spec 21 §1). It is
	// measured from the pager's page-read counter sampled across the run window. A workload
	// with no read operations (a bulk load, a write-only ingest) leaves it at the -1
	// not-measured sentinel rather than dividing by zero.
	Read float64 `json:"read_ios_per_op"`
}

// readNotMeasured is the sentinel for an amplification dimension the harness cannot yet
// measure honestly. It marshals as -1 and is distinguishable from any real ratio (which is
// non-negative).
const readNotMeasured = -1.0

// GCStats captures the Go-runtime tax over the measured window: how many collections ran,
// the total and worst stop-the-world pause, and the heap size at the end. A flat, low GC
// cost as the database grows is one of the spec's targets (spec 20, spec 21 §6), so the
// suite measures it rather than trusting it.
type GCStats struct {
	NumGC      uint32        `json:"num_gc"`
	TotalPause time.Duration `json:"total_pause_ns"`
	MaxPause   time.Duration `json:"max_pause_ns"`
	HeapInUse  uint64        `json:"heap_inuse_bytes"`
	TotalAlloc uint64        `json:"total_alloc_bytes"`
}

// gcSnapshot is a point-in-time read of the runtime's GC counters, taken at the start and
// end of the measured window so the difference is the window's own GC cost.
type gcSnapshot struct {
	numGC      uint32
	pauseTotal uint64
	maxPause   uint64
	heapInUse  uint64
	totalAlloc uint64
}

// readGC reads the current GC counters. PauseTotalNs and NumGC are monotonic since process
// start, so the window cost is end-minus-start; MaxPause is taken from the most recent pause
// buckets, which is a conservative read of the window's worst pause.
func readGC() gcSnapshot {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	var maxPause uint64
	// PauseNs is a ring of the 256 most recent pauses; scan it for the window's worst. For a
	// bounded microbenchmark the window's pauses fit inside the ring, so this is exact.
	for _, p := range m.PauseNs {
		if p > maxPause {
			maxPause = p
		}
	}
	return gcSnapshot{
		numGC:      m.NumGC,
		pauseTotal: m.PauseTotalNs,
		maxPause:   maxPause,
		heapInUse:  m.HeapInuse,
		totalAlloc: m.TotalAlloc,
	}
}

// diff turns a start/end pair of snapshots into the window's GCStats.
func (end gcSnapshot) diff(start gcSnapshot) GCStats {
	return GCStats{
		NumGC:      end.numGC - start.numGC,
		TotalPause: time.Duration(end.pauseTotal - start.pauseTotal),
		MaxPause:   time.Duration(end.maxPause),
		HeapInUse:  end.heapInUse,
		TotalAlloc: end.totalAlloc - start.totalAlloc,
	}
}

// Result is one workload's measured outcome on one engine: enough to reproduce the run
// (the setup block) and enough to judge it (throughput, latency, the amplification triple,
// GC). It marshals to the machine-readable JSON the suite tracks over time and the
// regression gates read (spec 21 §5).
type Result struct {
	// Workload and Engine name what ran.
	Workload string `json:"workload"`
	Engine   string `json:"engine"`

	// Setup is the disclosure block: without it a number is meaningless (spec 21 §3).
	Setup Setup `json:"setup"`

	// Ops is the number of measured operations and Duration the measured wall-clock window.
	Ops      int64         `json:"ops"`
	Duration time.Duration `json:"duration_ns"`
	// Dropped counts operations that did not complete (a stall that gave up, a retry budget
	// exhausted). A throughput number with hidden dropped work is a lie, so it is reported,
	// never swallowed (spec 21 §3, no silent caps).
	Dropped int64 `json:"dropped"`

	// Throughput is Ops over Duration in operations per second, the headline number.
	Throughput float64 `json:"throughput_ops_per_sec"`

	// Reads and Writes break latency out by operation kind, because a mixed workload's read
	// tail and write tail are different stories (group commit hits writes, the cache hits
	// reads). A pure workload leaves the other side's Count at zero.
	Reads  Latency `json:"read_latency"`
	Writes Latency `json:"write_latency"`

	Amplification Amplification `json:"amplification"`
	GC            GCStats       `json:"gc"`
}

// Setup is the run's disclosed environment and configuration. A result is only as
// trustworthy as the setup printed beside it (spec 21 §3).
type Setup struct {
	GoVersion    string `json:"go_version"`
	GOOS         string `json:"goos"`
	GOARCH       string `json:"goarch"`
	NumCPU       int    `json:"num_cpu"`
	KeyCount     int    `json:"key_count"`
	KeyLen       int    `json:"key_len"`
	ValLen       int    `json:"val_len"`
	Distribution string `json:"distribution"`
	Seed         int64  `json:"seed"`
	Synchronous  string `json:"synchronous"`
	BatchSize    int    `json:"batch_size"`
	Concurrency  int    `json:"concurrency"`
	CacheBytes   int    `json:"cache_bytes"`
}

// JSON renders the result as indented, machine-readable JSON for storage and diffing.
func (r Result) JSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

func distName(d Distribution) string {
	switch d {
	case Sequential:
		return "sequential"
	case Zipfian:
		return "zipfian"
	default:
		return "uniform"
	}
}
