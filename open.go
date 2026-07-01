package kv

// Open is the friendly in-process constructor: it opens a tiered store at path with one option
// struct and sane defaults, so a caller does not have to size five separate knobs to get a
// correct store. It is the front door for using kv as an embedded key-value engine, the
// SQLite-feel single-file store the design set out to be. The returned *DB is the engine
// itself; its Set, Get, Delete, Sync, and Close are the whole surface.
//
// The store is one file at path (plus a sibling commit watermark file). Values larger than
// memory live on disk in the cold tier; the working set is served from the in-memory hot tier
// and a read cache. There is no separate WAL and no transaction manager: a write is durable once
// it has been group-committed to the file, and Sync forces that barrier on demand.
func Open(path string, opts Options) (*DB, error) {
	o := opts.withDefaults()
	t, err := openTiered(path, o.HotBytes, o.hotKeys(), o.ResidentBytes, o.KeyCapacity, o.ReadCacheCells)
	if err != nil {
		return nil, err
	}
	t.syncWrites = o.SyncWrites
	return t, nil
}

// Options sizes a store opened by Open. The zero value is valid: every field falls back to a
// default tuned for a general embedded workload, so Open(path, Options{}) just works. Set the
// fields that matter for a particular deployment and leave the rest zero.
type Options struct {
	// KeyCapacity is the expected number of distinct keys. It sizes the cold index, which is
	// resident in memory and does not grow, so set it at or above the working key count. This is
	// the one knob worth setting for a large store: the F2-style design keeps a full key index in
	// memory while the values spill to disk, so the index, not the value bytes, is the memory
	// floor. Default 1<<20 (about a million keys).
	KeyCapacity int

	// HotBytes is the size in bytes of one in-memory hot segment. Writes land here and are
	// migrated to the cold tier a segment at a time, so this bounds the resident write buffer
	// (at most two segments live at once) and sets how much a crash between syncs can lose.
	// Default 8 MiB.
	HotBytes int64

	// HotKeys is the number of records one hot segment's index is sized for. Set it when the
	// value size is known so the index fits the segment's real record count: the default
	// heuristic assumes a tiny average record and over-allocates the index by orders of
	// magnitude for large values, which the profiler showed dominated per-seal allocation and
	// drove fill-throughput variance (note 182). A too-small value only causes an earlier seal,
	// never a lost write, so this is a tuning knob, not a correctness one. Zero falls back to the
	// HotBytes/hotKeyBytes heuristic.
	HotKeys int

	// ResidentBytes is the cold log's resident tail window: how many bytes of the most recently
	// migrated cold records stay in memory for fast reads before falling back to the file.
	// Default 64 MiB.
	ResidentBytes int64

	// ReadCacheCells is the number of cells in the read cache over cold reads. More cells catch
	// more repeated cold keys at a cost of memory. Rounded up to a power of two. Default 1<<16.
	ReadCacheCells int

	// SyncWrites picks the durability contract. Both settings are durable; the knob only moves when
	// the fsync happens relative to the returned Set, so it is a granularity choice, not durable
	// versus not.
	//
	// The default, SyncWrites false, is background group commit: a write lands in the in-memory hot
	// tier and returns, and the flusher fsyncs it a moment later. This is durable-on-a-short-delay,
	// not durable-on-return: a crash in the window between the ack and the next flush loses the
	// un-flushed hot records, bounded to at most two segments, the same bounded-loss-window a Redis
	// appendfsync-everysec or an OS-buffered write gives. It is the right default for a cache, a read
	// model, or any store that can replay a sub-second tail, and it is where the engine's throughput
	// lead lives, because the ack does not wait on the disk.
	//
	// SyncWrites true is the honest per-commit contract: a Set does not return until its record is
	// fsynced to the file, so an acked write survives a crash with zero loss, the guarantee bbolt,
	// pebble, and a per-commit sqlite give. It is not a per-write fsync in disguise: writes route
	// straight to the cold log's group-commit flusher, so many concurrent writers coalesce onto one
	// shared fsync, the batching trick that lets a group-committing store stay fast under
	// concurrency instead of hitting the disk's per-flush ceiling on every write. Reach for it for a
	// ledger, a queue, anything where the write you acked must not vanish. Durability uses the file's
	// Sync, which is fsync on Linux, a full-file flush on Windows, and the platform's file sync on
	// macOS, the same primitive every embedded Go engine here flushes with, so the comparison stays
	// on one footing across all three.
	SyncWrites bool
}

const (
	defaultKeyCapacity    = 1 << 20
	defaultHotBytes       = 8 << 20
	defaultResidentBytes  = 64 << 20
	defaultReadCacheCells = 1 << 16

	// hotKeyBytes is the average record size Open assumes when sizing a hot segment's index from
	// HotBytes. It is deliberately small so the index rarely fills before the buffer does; a
	// too-small estimate only triggers an early seal, never a lost write, because a full hot index
	// reports full exactly as a full buffer does.
	hotKeyBytes = 32
)

func (o Options) withDefaults() Options {
	if o.KeyCapacity <= 0 {
		o.KeyCapacity = defaultKeyCapacity
	}
	if o.HotBytes <= 0 {
		o.HotBytes = defaultHotBytes
	}
	if o.ResidentBytes <= 0 {
		o.ResidentBytes = defaultResidentBytes
	}
	if o.ReadCacheCells <= 0 {
		o.ReadCacheCells = defaultReadCacheCells
	}
	return o
}

// hotKeys sizes one hot segment's index. An explicit HotKeys wins, since a caller that knows its
// value size can size the index to the segment's real record count and avoid the heuristic's
// large-value over-allocation. Otherwise it falls back to the byte budget over a small assumed
// record so the index outlasts the buffer, never below a small floor so a tiny HotBytes still
// yields a usable table.
func (o Options) hotKeys() int {
	if o.HotKeys > 0 {
		return max(o.HotKeys, 1024)
	}
	return max(int(o.HotBytes/hotKeyBytes), 1024)
}
