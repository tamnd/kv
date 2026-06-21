package bench

// Workload is one access pattern the harness can drive against a database. The standard set
// below is the YCSB core (the lingua franca that makes kv numbers comparable to the
// published RocksDB/LMDB/LevelDB literature) plus the two targeted micro workloads the spec
// calls out: a cold bulk load and a write-saturated ingest (spec 21 §2).
//
// A workload run has two parts. A load phase fills the keyspace with KeyCount keys in
// sorted order, batched so the LSM actually flushes; then a run phase performs Ops
// operations drawn from the workload's distribution and mix. The harness measures the run
// phase, except for a bulk-load workload where the load itself is the thing being measured.
type Workload struct {
	// Name labels the workload in the result JSON.
	Name string
	// Dist is the key access distribution for the run phase.
	Dist Distribution
	// ReadFraction is the probability an operation is a read; the rest are blind writes
	// (updates). 1.0 is read-only, 0.0 is write-only.
	ReadFraction float64
	// ScanLength, when positive, makes each read a forward range scan of this many keys
	// instead of a point lookup (YCSB-E exercises iteration and the LSM merge cost).
	ScanLength int
	// RMW makes each operation a read-modify-write of one key: a Get then a Set in the same
	// transaction (YCSB-F exercises the conflict and read-own-write paths).
	RMW bool
	// MeasureLoad measures the bulk load instead of a run phase, for the cold-population
	// workload. When true the run phase is skipped.
	MeasureLoad bool
	// ReadLatest drives the YCSB-D pattern: a growing keyspace where inserts append new keys
	// at the head and reads skew toward the most recently inserted ones. It runs on a single
	// goroutine (the head is shared mutable state) and uses InsertFraction for its op mix
	// instead of ReadFraction.
	ReadLatest bool
	// InsertFraction is the probability a ReadLatest op inserts a new key; the rest read a
	// recent one. Ignored unless ReadLatest is set.
	InsertFraction float64
}

// scanLenE is the short-scan length YCSB-E uses; 50 keys is the conventional value.
const scanLenE = 50

// insertFracD is the insert share YCSB-D uses: 5% inserts, 95% read-latest, the conventional
// mix.
const insertFracD = 0.05

// Standard is the canonical workload matrix the suite reports for both engines (spec 21 §2):
// the YCSB core A through F plus the two targeted micro workloads, a cold bulk load and a
// write-saturated ingest.
func Standard() []Workload {
	return []Workload{
		{Name: "ycsb-a", Dist: Zipfian, ReadFraction: 0.50},
		{Name: "ycsb-b", Dist: Zipfian, ReadFraction: 0.95},
		{Name: "ycsb-c", Dist: Zipfian, ReadFraction: 1.00},
		{Name: "ycsb-d", Dist: Zipfian, ReadLatest: true, InsertFraction: insertFracD},
		{Name: "ycsb-e", Dist: Zipfian, ReadFraction: 1.00, ScanLength: scanLenE},
		{Name: "ycsb-f", Dist: Zipfian, ReadFraction: 1.00, RMW: true},
		{Name: "bulk-load", Dist: Sequential, ReadFraction: 0.0, MeasureLoad: true},
		{Name: "write-saturated", Dist: Uniform, ReadFraction: 0.0},
	}
}

// isWrite reports whether the op drawn with probability r against ReadFraction is a write.
// A read-modify-write counts as both, handled by the harness; this is the blind read/write
// split.
func (w Workload) isWrite(r float64) bool {
	if w.RMW {
		return false // RMW handled wholesale; each op reads then writes
	}
	return r >= w.ReadFraction
}
