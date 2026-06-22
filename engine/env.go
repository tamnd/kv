package engine

// Env is what the host injects into a core at Open: the core's window onto the
// shared substrate (spec 04 §2.5). A core never opens files or allocates pages
// directly; it asks the Env. Injecting the pager, cache, codec, and crypto here
// is what guarantees both cores share caching, compression, and encryption
// rather than each reinventing them.
//
// The dependency fields are interfaces (Pager, WAL, ...) so the cores compile and
// test against the model engine before the concrete subsystems exist. Each
// interface is fleshed out in the milestone that builds its subsystem; for now
// they carry the methods the seam already needs. A nil field is legal for a core
// (such as the model engine) that does not use that subsystem.
type Env struct {
	// Pager turns page numbers into pinned in-memory frames (spec 03).
	Pager Pager
	// WAL is the write-ahead log; a core uses it to find the recovery tail
	// (spec 07).
	WAL WAL
	// Cache is the decompressed/decrypted logical-block cache (spec 12).
	Cache BlockCache
	// Codec compresses and decompresses blocks (spec 13).
	Codec Codec
	// Crypto encrypts and decrypts pages at rest (spec 14).
	Crypto Crypto
	// Clock supplies the oldest readable version, for version GC (spec 10).
	Clock VersionSource
	// Metrics holds counters and histograms (spec 19).
	Metrics Metrics
	// Options carries engine-specific tunables (page size, filter policy,
	// compaction knobs).
	Options EngineOptions
	// Visible is the shared MVCC visibility rule (spec 10): it reports whether an
	// internal key is visible at a snapshot. Defined once above the seam so both
	// cores resolve versions identically.
	Visible func(internalKey []byte, snap Snapshot) bool
}

// Pager is the buffer-pool/page interface the engine sees (spec 03). The concrete
// pager in the pager package implements it; the model engine ignores it. Methods
// are added as the milestones that need them land.
type Pager interface {
	// PageSize is the usable bytes per page.
	PageSize() int
}

// WAL is the write-ahead-log interface the engine sees (spec 07). The engine uses
// it only to locate the recovery tail; appends and group commit are driven above
// the seam.
type WAL interface {
	// LastLSN reports the highest durable log sequence number.
	LastLSN() uint64
}

// BlockCache caches decompressed, decrypted logical blocks shared by both cores
// (spec 12).
type BlockCache interface {
	// Get returns a cached block by key, or nil if absent.
	Get(key uint64) ([]byte, bool)
	// Put inserts a block.
	Put(key uint64, block []byte)
}

// Codec is the block compression seam (spec 13).
type Codec interface {
	Compress(dst, src []byte) []byte
	Decompress(dst, src []byte) ([]byte, error)
}

// Crypto is the at-rest encryption seam (spec 14).
type Crypto interface {
	Encrypt(dst, src []byte, pageNo uint32) []byte
	Decrypt(dst, src []byte, pageNo uint32) ([]byte, error)
}

// VersionSource supplies the watermark below which dead versions may be garbage
// collected (spec 10).
type VersionSource interface {
	// OldestReadable returns the oldest version any live reader can still see;
	// versions older than this and superseded are collectible.
	OldestReadable() uint64
}

// Metrics is the counter/histogram sink (spec 19).
type Metrics interface {
	Inc(name string, delta int64)
	Observe(name string, value float64)
}

// FilterKind selects the per-segment membership filter the LSM core builds (spec 06
// §5). FilterBloom, the zero value and default, is the classic Bloom filter, fast to
// probe on the hot levels. FilterRibbon is the opt-in Ribbon filter, which reaches the
// same false-positive rate in meaningfully less space, attractive on the deep cold
// levels where filters dominate the resident set. The B-tree core ignores it.
type FilterKind uint8

const (
	FilterBloom  FilterKind = iota // the default Bloom filter
	FilterRibbon                   // the opt-in Ribbon filter
)

// EngineOptions carries engine-specific tunables that travel from the header and
// configuration into a core at Open (spec 04 §5, spec 22). Fields are populated
// per engine; unset fields take engine defaults.
type EngineOptions struct {
	// PageSize is the database page size in bytes.
	PageSize int

	// B-tree knobs.
	FillFactor      float64 // target leaf/interior fill on split
	MaxInlineValue  int     // bytes before a value overflows
	BufferedInserts bool    // Bε buffered-insert mode

	// LSM knobs.
	MemtableSize      int        // bytes before a memtable is flushed
	LevelSizeRatio    int        // size multiplier between levels
	ValueSepThreshold int        // value size above which WiscKey separates to vLog
	RangeIndex        bool       // build the REMIX ordered index for scan-heavy workloads
	Filter            FilterKind // per-segment membership filter: Bloom (default) or Ribbon
	Compression       bool       // heat-tiered block compression on the LSM data pages (spec 13)

	// DisableAutoCompaction turns off the LSM core's background compaction scheduler, so
	// compaction runs only when the host calls Maintain. Off by default (the engine
	// self-schedules compaction to keep read fan-out and space amp bounded under sustained
	// writes); a test that drives compaction by hand to observe a precise segment shape
	// sets it. The B-tree core ignores it.
	DisableAutoCompaction bool
}
