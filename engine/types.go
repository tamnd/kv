package engine

import "errors"

// Snapshot is a consistent read version. A reader created at a snapshot sees the
// newest committed version of each key that is <= Version (spec 10). It is a
// struct rather than a bare uint64 so a future SSI read-tracking handle can be
// added without changing the seam.
type Snapshot struct {
	Version uint64
}

// IterOptions controls a range scan (spec 11). All keys are user keys.
type IterOptions struct {
	// Lower is the inclusive lower bound; nil means unbounded below.
	Lower []byte
	// Upper is the exclusive upper bound; nil means unbounded above.
	Upper []byte
	// Prefix restricts iteration to keys with this prefix; it is a convenience
	// that the iterator layer translates into Lower/Upper bounds.
	Prefix []byte
	// Reverse iterates from high to low keys.
	Reverse bool
	// KeysOnly skips value materialization, so separated values are never
	// fetched.
	KeysOnly bool
}

// LazyValue defers fetching a separated value (vLog/overflow) until the caller
// actually reads it. For inline values the bytes are already present; for
// separated values fetch resolves the pointer on demand.
type LazyValue struct {
	inline []byte
	length int
	fetch  func() ([]byte, error)
}

// InlineValue wraps already-materialized bytes.
func InlineValue(b []byte) LazyValue {
	return LazyValue{inline: b, length: len(b)}
}

// SeparatedValue wraps a deferred fetch of a value of known length.
func SeparatedValue(length int, fetch func() ([]byte, error)) LazyValue {
	return LazyValue{length: length, fetch: fetch}
}

// Len reports the value length without materializing a separated value.
func (lv LazyValue) Len() int { return lv.length }

// Value materializes the value, resolving a separated pointer if needed.
func (lv LazyValue) Value() ([]byte, error) {
	if lv.fetch != nil {
		return lv.fetch()
	}
	return lv.inline, nil
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
}

// MaintReport summarizes what a Maintain call did.
type MaintReport struct {
	PagesCompacted int
	BytesWritten   int64
	BytesReclaimed int64
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
}

// ErrNotFound is returned by Reader.Get when no version of the key is visible at
// the snapshot. The public kv package re-exports it (spec 15).
var ErrNotFound = errors.New("kv: key not found")

// ErrBatchCorrupt is returned by DecodeBatch when a serialized batch is truncated
// or internally inconsistent, so a torn WAL frame is rejected outright rather than
// half-applied.
var ErrBatchCorrupt = errors.New("kv: corrupt write batch")
