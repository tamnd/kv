package engine

import (
	"errors"

	"github.com/tamnd/kv/format"
)

// Snapshot is a consistent read version. A reader created at a snapshot sees the
// newest committed version of each key that is <= Version (spec 10). It is a
// struct rather than a bare uint64 so a future SSI read-tracking handle can be
// added without changing the seam.
type Snapshot struct {
	Version uint64
	// Now is the wall-clock time in nanoseconds used to evaluate TTL expiry during a
	// read (spec 15 §6). A read treats a TTL set whose expiry is <= Now as absent. Zero
	// disables expiry, so a read with no clock, and GC, never expire a value the
	// background sweep has not yet removed.
	Now uint64
	// Clock, when set, is a deferred wall clock the read path consults only after a fold
	// first meets a TTL cell, instead of reading Now eagerly for every read (perf/01 F6).
	// Nearly all keys carry no TTL, so a fold that never touches a TTL cell never calls
	// Clock and pays no clock read at all. A fold that does meet one reads Clock once and
	// caches the result for the rest of that fold through TTLClock. Clock takes precedence
	// over Now when both are set; a fixed-time caller (GC, compaction, the model) leaves
	// Clock nil and the fold uses the pinned Now.
	Clock func() uint64
}

// TTLClock returns a per-fold resolver for the wall clock TTL expiry is tested against.
// It defers reading Snapshot.Clock until For first sees a KindSetWithTTL cell and caches
// it from then on, so a fold over keys that carry no TTL, which is almost every read,
// never reads the clock (perf/01 F6). With Clock nil it always returns the pinned Now,
// the behavior GC, compaction, and the model rely on. A TTLClock is single-fold state and
// must not be shared across goroutines.
func (s Snapshot) TTLClock() TTLClock {
	return TTLClock{clock: s.Clock, fixed: s.Now}
}

// TTLClock resolves the TTL wall clock lazily within one fold. See Snapshot.TTLClock.
type TTLClock struct {
	clock    func() uint64
	fixed    uint64
	resolved bool
	now      uint64
}

// For returns the wall clock to evaluate a cell of the given kind against. A non-TTL cell
// needs no clock and returns zero without consulting it; a TTL cell reads the deferred
// Clock once and reuses it for the rest of the fold. With no deferred Clock it returns the
// pinned Now for every kind, matching the eager behavior fixed-time callers expect.
func (c *TTLClock) For(kind format.Kind) uint64 {
	if c.clock == nil {
		return c.fixed
	}
	if kind != format.KindSetWithTTL {
		return 0
	}
	if !c.resolved {
		c.now = c.clock()
		c.resolved = true
	}
	return c.now
}

// MaintBudget bounds a single Maintain call so background work never starves the
// foreground (spec 09). A zero budget means "do nothing this call".
type MaintBudget struct {
	// MaxPages caps how many pages maintenance may read+write.
	MaxPages int
	// MaxBytes caps the bytes of I/O maintenance may perform.
	MaxBytes int64
	// Watermark is the version-GC horizon: the oldest version any live or future
	// reader can still observe (the oracle's read-mark, spec 10 §6). Every version
	// at or below it is reclaimable, since no snapshot below the watermark will ever
	// be taken again, so the whole history at or below it collapses to the single
	// value a snapshot at the watermark resolves. Zero disables version GC for the
	// call.
	Watermark uint64
	// Now is the wall-clock time in nanoseconds the TTL sweeper compares expiries
	// against (spec 15 §6). A TTL set whose expiry is at or before Now is swept into a
	// tombstone so its value bytes are reclaimed and version GC can collapse it. Reads
	// already resolve such a key absent before the sweep, so the sweep is purely a
	// space optimization. Zero disables sweeping for the call, the same way GC and
	// recovery disable read-time expiry.
	Now uint64
}

// MaintReport summarizes what a Maintain call did.
type MaintReport struct {
	PagesCompacted int
	BytesWritten   int64
	BytesReclaimed int64
	// ExpiredSwept is the number of expired TTL sets the sweeper turned into tombstones
	// this call (spec 15 §6), for observability; it does not bound the call.
	ExpiredSwept int64
	// More is true if the engine has more maintenance pending and would like to
	// be called again.
	More bool
}

// EngineStats is the space accounting the checkpoint/vacuum driver reads
// (spec 09). Counts are engine-defined but the named fields are common to both
// cores.
type EngineStats struct {
	// LiveKeys is the number of user keys visible at the newest snapshot.
	LiveKeys int64
	// LiveBytes is the logical size of live keys+values.
	LiveBytes int64
	// PhysicalBytes is the on-disk footprint, including dead versions not yet
	// reclaimed.
	PhysicalBytes int64
	// FreePages is the number of pages on the engine's freelist.
	FreePages int64
	// Amplification is the engine's current space amplification estimate
	// (physical / live), for the RUM tradeoff observability (spec 19).
	Amplification float64

	// CompactionScore is the urgency of the most-pending compaction, normalized so 1.0
	// means a level is exactly at its trigger and a larger value means further past it
	// (spec 19 §1.5). It is 0 when nothing is due. A value that climbs and stays high is
	// compaction losing to the write rate, the read-amplification and write-stall
	// precursor an operator watches.
	CompactionScore float64
}

// ErrNotFound is returned by Reader.Get when no version of the key is visible at
// the snapshot. The public kv package re-exports it (spec 15).
var ErrNotFound = errors.New("kv: key not found")

// ErrBatchCorrupt is returned by DecodeBatch when a serialized batch is truncated
// or internally inconsistent, so a torn WAL frame is rejected outright rather than
// half-applied.
var ErrBatchCorrupt = errors.New("kv: corrupt write batch")
