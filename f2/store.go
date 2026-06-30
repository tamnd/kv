// Package f2 is a resident-index key/value engine in the FASTER v2 shape, the
// memory-lean sibling of package hashlog. It keeps hashlog's two strengths, a
// lock-free read path (an atomic-load probe straight to the value, no mutex on a
// hot key) and a per-shard hybrid log, and pays down the one cost that caps how
// far hashlog scales: the size of the resident index.
//
// hashlog stores a full index entry per live key (the key bytes, a 64-bit hash,
// and a value location), so a billion 16-byte keys cost tens of gigabytes of RAM
// in the index alone before a single value is held. f2 follows FASTER and stores
// only an eight-byte atomic word per slot: a 24-bit fingerprint and a 39-bit
// logical offset into the shard's log. The key itself is not resident; a lookup
// probes by fingerprint, reads the candidate record from the log, and verifies
// the full key there. The record is self-describing (it already carries its key
// for recovery and compaction), so the verify costs nothing the read was not
// already paying. At the 0.8 load factor that is roughly 10 to 13 bytes of index
// per key regardless of key length (8 bytes per slot, the spread is where a
// shard's table sits in its doubling cycle), roughly a sixth of hashlog's cost on
// realistic keys: the difference between a billion keys fitting in around 15 GiB
// and not fitting at all.
//
// The index never evicts, so resident RAM scales with the live key count, and
// that is what sets the practical ceiling: about a billion keys is the supported
// target (~15 GiB of index), and the default 256 shards hold roughly 11.7M keys
// each before a shard's table passes the 2^24-slot read-free grow range. A store
// aiming past a billion keys should raise Shards so no single shard approaches
// that range; ten billion keys is reachable on a large host (~150 GiB of index)
// but is past the routinely exercised envelope.
//
// The model, per shard, mirrors hashlog so the two are a fair comparison:
//
//   - A lock-free open-addressing index of atomic 64-bit slots maps a key's hash
//     to a logical log offset. A reader probes with atomic loads only and never
//     touches a shared mutex; a writer publishes a slot with one atomic store
//     under the shard write lock.
//   - The log is a sequence of fixed-size pages behind an atomic page directory.
//     A logical offset names the page and the byte within it. In the full
//     resident profile every page stays in RAM and is immutable once written, so
//     a reader slices the value straight out of the page with no lock and no
//     copy. An overwrite appends a new record and atomically repoints the slot
//     (read-copy-update), so the bytes a reader already holds never change under
//     it.
//
// The engine has exactly two profiles, chosen by whether Path is set:
//
//   - memory-only (no Path): the full-resident in-memory ceiling. Every page
//     stays in RAM, nothing syncs, nothing is reread from disk.
//   - single file (Path set): one file does both larger-than-memory and
//     durability. ResidentPagesPerShard bounds how much of each shard's log stays
//     in RAM (an evicted page is just a page already written to the file, reread
//     by offset), and the Durability dial decides when the file is fsynced. There
//     is no separate scratch design: the same file that holds an evicted page is
//     the one a crash recovers from.
package f2

import (
	"context"
	"errors"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tamnd/kv/crypto"
	"github.com/tamnd/kv/vfs"
)

// Durability is the per-store durability dial, the same contract hashlog uses so
// an adapter maps the kvbench dial onto either engine identically. It is
// meaningful only in the durable single-file mode; a memory-only store is always
// DurabilityNone, the benchmarked ceiling.
type Durability int

const (
	// DurabilityNone never fsyncs. It is the memory-only benchmark regime and the
	// larger-than-memory speed ceiling. It is the zero value, so the default store
	// is None.
	DurabilityNone Durability = iota
	// DurabilityNormal fsyncs on a byte cadence and at checkpoints, not on every SET
	// and not once per sealed page. A seal writes its page through to the file and a
	// device barrier is issued when the unsynced bytes cross SyncBytes, so the fsync
	// rate is set by bytes written rather than by the page-roll rate (which a smaller
	// page would inflate). The crash-loss window is at most SyncBytes of writes, or one
	// SyncInterval when the optional wall-time backstop is enabled; a clean Close loses
	// nothing because it checkpoints.
	DurabilityNormal
	// DurabilityFull fsyncs before every SET returns: nothing acknowledged is lost.
	DurabilityFull
)

// Tunables holds the knobs that shape a Store. It is the subset of hashlog's
// Tunables this engine honors, with the same meanings, so the two open through
// the same configuration. The zero value is not valid; use DefaultTunables and
// override.
type Tunables struct {
	// Shards is the number of independent index+log shards. Must be a power of two
	// so the shard for a key is a mask of its hash. More shards cut write-lock
	// contention and shrink each shard's table, so a grow touches fewer slots. The
	// selector takes the top log2(Shards) hash bits, so any power of two scales
	// (256 is the historical default, larger counts serve billions of keys without
	// any one shard's table approaching the read-free grow range; see the package
	// doc on the ceiling).
	Shards int

	// PageSize is the byte size of one log page. A record must fit in a page.
	PageSize int

	// ResidentPagesPerShard caps how many log pages each shard keeps in RAM before
	// the oldest is evicted (dropped from RAM, reread from its file block on
	// demand). Zero means unbounded: nothing is evicted, the full-resident mode.
	// Honored only with a Path; a memory-only store has nowhere to evict to and
	// requires zero. A single-file store with a budget must keep at least the tail
	// page resident, so a non-zero budget is at least one.
	ResidentPagesPerShard int

	// MutableWindowPages is how many pages at each shard's tail stay rewritable in
	// place, FASTER's mutable region. Zero or one keeps the historical behavior, only
	// the tail page taking same-size overwrites in place. A larger value lets a hot key
	// keep rewriting in place across a page roll, raising the in-place hit rate on a band
	// wider than one page at the cost of sealing each page later (under Normal, deferring
	// its sync to when it leaves the window or a checkpoint flushes it). It is honored
	// only on the budgeted in-place profile (a Path, a resident budget, not the Full
	// dial) and is clamped to the resident budget so a window page is never evicted
	// before it is sealed. Other profiles ignore it and keep a window of one.
	MutableWindowPages int

	// Path selects the single-file mode: one file that is both the
	// larger-than-memory backing and, at Normal or Full, the crash-recoverable
	// store. Empty keeps the memory-only mode, the benchmarked ceiling.
	Path string

	// FS is the filesystem the single file is opened on. It lets the host run f2 on
	// its own vfs backend: the in-memory backend for a fast, deterministic test, or
	// the OS backend for a real file. It is honored only with a Path; an empty Path is
	// memory-only and touches no file. Nil with a Path defaults to the OS backend, so a
	// caller that just sets a Path gets a real file with no extra wiring.
	FS vfs.FS

	// Durability is the durability dial. It is meaningful only when a Path is set;
	// selecting Normal or Full without a Path is an error because there is nowhere
	// to sync. The zero value is None.
	Durability Durability

	// Crypto, when set, seals each data and snapshot block's records region at rest
	// with the database's AEAD page envelope (D17). It is honored only with a Path; a
	// memory-only store holds nothing on disk to encrypt. The host passes the same
	// scheme the main file uses, so the f2 sidecar is encrypted under the same key. Nil
	// keeps the file plaintext and the record-granular fast paths.
	Crypto *crypto.Scheme

	// CheckpointBytes bounds the durable record bytes appended before a checkpoint
	// is due, capping recovery replay. Zero defaults to 256 MiB in durable mode.
	CheckpointBytes int64

	// SyncBytes bounds the durable bytes a Normal-dial store may seal to the file
	// without a device barrier: when a page seal pushes the unsynced total past it,
	// the sealing writer issues one group-commit barrier covering every shard's
	// pending writes. It decouples the fsync cadence from the page-seal cadence, so a
	// smaller page (which seals more often) does not multiply device barriers. Zero
	// defaults to 16 MiB. It has no effect under None (never syncs) or Full (a barrier
	// per SET). A value of one byte reproduces the old behavior, a barrier per seal.
	SyncBytes int64

	// SyncInterval, when positive, adds a wall-time bound to the Normal cadence: a
	// background flusher issues one barrier per interval whenever any bytes are dirty,
	// so a store that stalls just short of SyncBytes does not hold the sealed bytes
	// unsynced indefinitely. Zero (the default) leaves the cadence byte-only, where the
	// loss window is bounded by SyncBytes and by the next checkpoint rather than by
	// time. Set it when an idle-then-crash loss window must be bounded in seconds; a
	// steady writer crosses SyncBytes first and rarely sees the timer. It costs one
	// device barrier per interval while any bytes are dirty, so a small interval taxes a
	// slow-but-steady writer; the byte bound alone is enough for most workloads. It has
	// no effect under None or Full.
	SyncInterval time.Duration

	// CompactionThreshold is the dead fraction at which a shard is rewritten to
	// reclaim its stranded bytes. Zero defaults to 0.5 (half the shard's log bytes
	// dead). It bounds steady-state space at the cost of rewrite work; a higher
	// value tolerates more dead space for less rewriting.
	CompactionThreshold float64

	// CompactionInterval, when positive in durable mode, runs a background loop that
	// calls Compact at that period so the file stays bounded under churn without an
	// operator calling it. Zero leaves compaction manual, which is the default so a
	// benchmark is never perturbed by a background rewrite.
	CompactionInterval time.Duration
}

// windowPages is the effective in-place mutable-window page count for these tunables.
// Only the budgeted in-place profile widens past one page (a resident budget is the gate,
// and the window is clamped to it so a window page is sealed before it can be evicted);
// every other profile keeps a window of one, its historical sealing behavior unchanged.
func windowPages(t Tunables) int {
	if t.ResidentPagesPerShard <= 0 {
		return 1
	}
	w := t.MutableWindowPages
	if w < 1 {
		w = 1
	}
	if w > t.ResidentPagesPerShard {
		w = t.ResidentPagesPerShard
	}
	return w
}

// DefaultTunables returns a full-resident, memory-only configuration: 256 shards,
// 1 MiB pages, no spill. This is the in-memory ceiling shape, matching hashlog's
// default so the two engines are compared on the same geometry.
func DefaultTunables() Tunables {
	return Tunables{Shards: 256, PageSize: 1 << 20, ResidentPagesPerShard: 0}
}

// defaultCheckpointBytes is the durable-mode checkpoint interval when Tunables
// leaves CheckpointBytes zero: a checkpoint fsyncs sealed pages and advances the
// superblock, bounding how much a crash must replay.
const defaultCheckpointBytes = 256 << 20

// defaultSyncBytes is the Normal-dial seal-sync cadence when Tunables leaves SyncBytes
// zero: a barrier is issued every 16 MiB of sealed bytes, so the crash-loss window is
// bounded by data written while the fsync rate stays far below one barrier per sealed
// page (redesign-v2 doc 09). The wall-time backstop (SyncInterval) is off by default,
// since a periodic barrier taxes a steady writer and the byte bound already caps loss.
const defaultSyncBytes = 16 << 20

var (
	errBadShards        = errors.New("f2: Shards must be a power of two greater than zero")
	errBadPageSize      = errors.New("f2: PageSize must leave room for a block header and a record")
	errDurabilityNoPath = errors.New("f2: Durability other than None requires a Path")
	errBudgetNoPath     = errors.New("f2: ResidentPagesPerShard requires a Path")
	errBadBudget        = errors.New("f2: ResidentPagesPerShard must be zero or at least one")
	errValueTooBig      = errors.New("f2: record does not fit in a page")
	errPageMismatch     = errors.New("f2: file page size or shard count differs from tunables")
	errEncryptMismatch  = errors.New("f2: file encryption state differs from the supplied key")
	errClosed           = errors.New("f2: store is closed")
)

// minPageSize is the smallest page f2 accepts. A durable page must hold its
// header and at least a minimal record; a memory page needs room for a record's
// length prefixes. The floor keeps maxRecord positive so a misconfigured tiny
// page is rejected at New instead of making every Set fail with a confusing
// size error.
const minPageSize = blockHeaderSize + 64

// Stats reports the engine's space accounting, the data a memory and scalability
// study needs. Counts are summed across shards.
type Stats struct {
	Keys        int64 // live keys
	IndexSlots  int64 // total index slots allocated across shards
	IndexBytes  int64 // resident index cost: IndexSlots * 8
	LogBytes    int64 // bytes appended to the logs (live plus stranded)
	DeadBytes   int64 // bytes stranded by overwrites and deletes
	LiveBytes   int64 // LogBytes - DeadBytes, the bytes the index still references
	ResidentLog int64 // log bytes held in RAM (all of LogBytes in memory-only mode)
	EvictedLog  int64 // log bytes dropped from RAM, present only in the file
	ResidentMem int64 // total resident footprint estimate: IndexBytes + ResidentLog

	// SpaceAmplification is LogBytes over LiveBytes: 1.0 with no garbage, rising as dead
	// bytes accumulate between compactions, back toward 1.0 after a compaction. It is 1.0
	// when there is no live data to amplify.
	SpaceAmplification float64

	// FsyncCount is device barriers issued since open; FsyncAvgLatency is the mean wall
	// time per barrier, the signal that the Full dial is disk-bound. Both are 0 in
	// memory-only mode and under DurabilityNone.
	FsyncCount      int64
	FsyncAvgLatency time.Duration

	// MinShardKeys and MaxShardKeys bound the per-shard live key counts. A wide gap means
	// a skewed hash or too few shards for the key space, which concentrates lock
	// contention and compaction work on one shard.
	MinShardKeys int64
	MaxShardKeys int64

	// CompactionBacklog is blocks compaction has retired but the epoch guard has not yet
	// released for reuse, the disk space that frees once the readers that could still see
	// them drain.
	CompactionBacklog int

	// RecoveryDuration and RecoveryRecords report what the open-time recovery did: the
	// wall time it took and how many records it replayed. Both are 0 for a memory-only
	// store and a freshly created file that replayed nothing.
	RecoveryDuration time.Duration
	RecoveryRecords  int64
}

// BytesPerKey is the resident index cost per live key, the headline scalability
// number. It is IndexBytes / Keys, the value that stays near a small constant as
// the store grows because the index holds no key bytes.
func (s Stats) BytesPerKey() float64 {
	if s.Keys == 0 {
		return 0
	}
	return float64(s.IndexBytes) / float64(s.Keys)
}

// Store is the f2 engine. It is safe for concurrent use: each shard carries its
// own write lock and the shard for a key is fixed by the key's hash.
type Store struct {
	shards     []*shard
	df         *durableFile // the one shared file in single-file mode, nil in memory-only
	ep         *epochs      // shared epoch state in durable mode, nil in memory-only
	mask       uint64
	shardShift uint // right-shift that selects a shard from a key's top hash bits
	t          Tunables

	ckptBytes int64        // checkpoint interval, 0 disables auto-checkpoint
	sinceCkpt atomic.Int64 // durable bytes appended since the last checkpoint
	closed    atomic.Bool  // set once by Close, makes later calls return errClosed

	// ckptMu serializes checkpoints so the background loop, an explicit Checkpoint, and
	// the close-time checkpoint never run two commits at once (which would race the
	// snapshot chain's allocate-and-free). ckptSig is a depth-1 trigger: the Set that
	// crosses the byte threshold pokes it without blocking, so the checkpoint runs off
	// the write path and many crossings coalesce into one queued run (single-flight).
	ckptMu  sync.Mutex
	ckptSig chan struct{}

	// recoverNanos and recoverRecords record what the open-time recovery did, so Stats
	// can report the replay cost. Both are 0 for a memory-only store and a fresh file.
	recoverNanos   atomic.Int64
	recoverRecords atomic.Int64

	bgStop chan struct{}  // closed by Close to stop the background compactor
	bgWG   sync.WaitGroup // waits for the background compactor to exit
}

// New opens a Store with the given tunables. With no Path it is the
// full-resident, memory-only core. With a Path it is the single-file mode: one
// file that is both the larger-than-memory backing (bounded by
// ResidentPagesPerShard) and, under a Normal or Full dial, the crash-recoverable
// store. Opening an existing file replays it: the compact index is rebuilt from
// the file's records, so the store comes back with every key it acknowledged. It
// is NewContext with a background context, so a large replay cannot be cancelled.
func New(t Tunables) (*Store, error) {
	return NewContext(context.Background(), t)
}

// NewContext opens a Store like New, threading ctx through the replay so opening an
// existing file with a large log tail can be cancelled or bounded by a deadline. The
// context covers recovery only; the open store does not retain it. Cancellation is
// observed at shard and page boundaries during replay, so it bounds the work rather than
// aborting mid-record. A memory-only store has nothing to replay and ignores ctx.
func NewContext(ctx context.Context, t Tunables) (*Store, error) {
	if t.Shards <= 0 || t.Shards&(t.Shards-1) != 0 {
		return nil, errBadShards
	}
	if t.PageSize < minPageSize {
		return nil, errBadPageSize
	}
	if t.Path == "" {
		// Memory-only: nothing to sync and nowhere to evict to.
		if t.Durability != DurabilityNone {
			return nil, errDurabilityNoPath
		}
		if t.ResidentPagesPerShard != 0 {
			return nil, errBudgetNoPath
		}
		return newMemory(t), nil
	}
	if t.ResidentPagesPerShard < 0 {
		return nil, errBadBudget
	}
	return newDurable(ctx, t)
}

// newMemory builds the memory-only store: no file, every page resident.
func newMemory(t Tunables) *Store {
	s := &Store{
		shards:     make([]*shard, t.Shards),
		mask:       uint64(t.Shards - 1),
		shardShift: shardShiftFor(t.Shards),
		t:          t,
	}
	for i := range s.shards {
		s.shards[i] = newShard(t.PageSize, nil, i, 0, 1, nil)
	}
	return s
}

// newDurable opens or creates the single file, builds the shards over it, and
// replays an existing file into the index. A fresh file gets an initial
// superblock so a later open always finds one.
func newDurable(ctx context.Context, t Tunables) (*Store, error) {
	fs := t.FS
	if fs == nil {
		fs = vfs.NewOS()
	}
	f, err := fs.Open(t.Path, vfs.OpenReadWrite|vfs.OpenCreate)
	if err != nil {
		return nil, err
	}
	// One writer per file: a second process opening the same path would write the
	// superblock and append blocks independently and corrupt it. The exclusion is the
	// vfs backend's: the OS backend takes an advisory file lock where it implements
	// one, and the in-memory backend is single-process by construction.
	if err := f.Lock(vfs.LockExclusive); err != nil {
		_ = f.Close()
		return nil, err
	}
	ckpt := t.CheckpointBytes
	if ckpt == 0 {
		ckpt = defaultCheckpointBytes
	}
	s := &Store{
		shards:     make([]*shard, t.Shards),
		mask:       uint64(t.Shards - 1),
		shardShift: shardShiftFor(t.Shards),
		t:          t,
		ckptBytes:  ckpt,
	}
	df := &durableFile{f: f, pageSize: int64(t.PageSize), shards: t.Shards, dial: t.Durability, snapRoot: -1, enc: t.Crypto}
	df.gcCond = sync.NewCond(&df.smu)
	// Set the Normal-dial seal-sync cadence: a barrier every syncEvery sealed bytes,
	// with a background timer bounding the window when writes stall short of it. The
	// dial gate keeps None (never syncs) and Full (a barrier per record) on their
	// existing paths, where the cadence is meaningless.
	if t.Durability == DurabilityNormal {
		df.syncEvery = t.SyncBytes
		if df.syncEvery == 0 {
			df.syncEvery = defaultSyncBytes
		}
	}
	s.df = df
	s.ep = newEpochs()

	sb := readSuperblock(f)
	if sb.valid && (sb.pageSize != df.pageSize || sb.shards != df.shards) {
		_ = f.Close()
		return nil, errPageMismatch
	}
	// Refuse a key/file mismatch: a key supplied for a plaintext file, or no key for an
	// encrypted one, would otherwise seal or decode against the wrong expectation. The
	// pager guards the main file the same way; this guards the standalone f2 path.
	if sb.valid && sb.encrypted != (df.enc != nil) {
		_ = f.Close()
		return nil, errEncryptMismatch
	}
	df.seq = sb.seq
	window := windowPages(t)
	for i := range s.shards {
		s.shards[i] = newShard(t.PageSize, df, i, t.ResidentPagesPerShard, window, s.ep)
	}
	if sb.valid {
		if err := s.recover(ctx); err != nil {
			_ = f.Close()
			return nil, err
		}
	} else { // stamp a fresh file so a later open always finds a superblock
		if err := df.writeSuperblock(); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	if t.CompactionInterval > 0 || (df != nil && s.ckptBytes > 0) {
		s.bgStop = make(chan struct{})
	}
	if t.CompactionInterval > 0 {
		s.bgWG.Add(1)
		go s.compactLoop(t.CompactionInterval)
	}
	if df != nil && s.ckptBytes > 0 {
		s.ckptSig = make(chan struct{}, 1)
		s.bgWG.Add(1)
		go s.checkpointLoop()
	}
	// The seal-sync time flusher bounds the Normal loss window in wall time when an
	// interval is set. It is off by default (the byte cadence bounds loss without a
	// periodic-barrier tax). It is its own goroutine on the durable file, not the
	// store's background group, so Close can stop it before the final checkpoint
	// without ordering it against the compactor.
	if t.Durability == DurabilityNormal && t.SyncInterval > 0 {
		df.startSyncLoop(t.SyncInterval)
	}
	return s, nil
}

// checkpointLoop runs a checkpoint whenever the write path signals the byte threshold was
// crossed, until Close stops it. Keeping the checkpoint off the crossing Set removes the
// periodic latency spike that an inline checkpoint puts on one unlucky writer. A failed
// background checkpoint is dropped, the same as a failed background compaction: the data
// is no less durable (a checkpoint only bounds recovery replay, it never acknowledges a
// write), so the only cost of a miss is a longer replay, and the next trigger retries.
func (s *Store) checkpointLoop() {
	defer s.bgWG.Done()
	for {
		select {
		case <-s.bgStop:
			return
		case <-s.ckptSig:
			if s.closed.Load() {
				return
			}
			_ = s.checkpoint(context.Background())
		}
	}
}

// compactLoop runs Compact every interval until Close stops it, keeping the file
// bounded under churn. A Compact error is dropped rather than crashing the store: a
// background reclaim that fails (a transient write error) leaves the store correct,
// just not yet reclaimed, and the next tick retries.
func (s *Store) compactLoop(interval time.Duration) {
	defer s.bgWG.Done()
	tk := time.NewTicker(interval)
	defer tk.Stop()
	for {
		select {
		case <-s.bgStop:
			return
		case <-tk.C:
			if s.closed.Load() {
				return
			}
			_ = s.Compact()
		}
	}
}

// shardShiftFor returns the right-shift that drops a key's top log2(shards) hash
// bits into the low position the shard mask reads. Shards is a power of two, so it
// is 64 - log2(shards): 256 shards keep the historical shift of 56, and a larger
// count simply consumes more of the high bits, so Shards scales past 256 instead
// of leaving every shard above the 256th empty. The selector takes the top bits
// while the index home reads the low bits of the hash mix, so the two stay
// independent for any shard count whose bits do not reach down into the index's
// range (true for any practical count: a 2^24-slot index uses the low 39 hash
// bits, so independence holds up to 2^25 shards, far past a useful number).
func shardShiftFor(shards int) uint {
	return 64 - uint(bits.TrailingZeros(uint(shards)))
}

func (s *Store) shardFor(h uint64) *shard { return s.shards[(h>>s.shardShift)&s.mask] }

// Get returns the value for key and whether it was found. The returned slice may
// alias the log page, so the caller must not mutate it. In the memory-only
// profile the slice stays valid for the life of the store because resident pages
// are never freed or rewritten; in the single-file profile a page can be evicted,
// so a slice held across later calls is not guaranteed to stay valid. Use GetCopy
// when the value must be retained or mutated.
func (s *Store) Get(key []byte) ([]byte, bool, error) {
	if s.closed.Load() {
		return nil, false, errClosed
	}
	h := hash64(key)
	return s.shardFor(h).get(h, key)
}

// GetCopy is Get returning a freshly allocated copy of the value that the caller
// owns outright: safe to mutate and to keep for the life of the store regardless
// of eviction. It trades one allocation for that freedom, so prefer Get on a hot
// read path that consumes the value before the next call.
func (s *Store) GetCopy(key []byte) ([]byte, bool, error) {
	v, ok, err := s.Get(key)
	if err != nil || !ok {
		return nil, ok, err
	}
	return append([]byte(nil), v...), true, nil
}

// Scan calls fn for every live key in the store, in an unspecified order, until
// fn returns false or every key has been visited. It is the only enumeration the
// engine offers: a hash index has no key order, so this is not a range or sorted
// scan. The key and value passed to fn alias the log page and must not be mutated
// or retained past the call; copy what you need to keep. Scan is lock-free per
// shard and reflects writes concurrent with it the same way Get does, so it is a
// near-consistent snapshot, not a point-in-time one.
func (s *Store) Scan(fn func(key, value []byte) bool) error {
	if s.closed.Load() {
		return errClosed
	}
	for _, sh := range s.shards {
		if !sh.scan(fn) {
			return nil
		}
	}
	return nil
}

// Set stores value under key, appending a new record and repointing the index
// slot. value is copied into the log, so the caller may reuse its buffer. In
// durable mode the record carries a CRC and, past the checkpoint interval, the
// write triggers a checkpoint so recovery replay stays bounded.
func (s *Store) Set(key, value []byte) error {
	if s.closed.Load() {
		return errClosed
	}
	var n int
	if s.df != nil {
		n = durableRecordLen(key, value)
		limit := s.t.PageSize - blockHeaderSize
		if s.df.enc != nil {
			limit -= cryptoOverhead // the sealed envelope reserves the page tail
		}
		if n > limit {
			return errValueTooBig
		}
	} else {
		n = recordLen(key, value)
		if n > s.t.PageSize {
			return errValueTooBig
		}
	}
	h := hash64(key)
	if err := s.shardFor(h).set(h, key, value); err != nil {
		return err
	}
	if s.df != nil && s.ckptBytes > 0 && s.sinceCkpt.Add(int64(n)) >= s.ckptBytes {
		s.sinceCkpt.Store(0)
		// Poke the background checkpoint without blocking this writer. A full channel
		// means a checkpoint is already queued or running, so this crossing coalesces
		// into it (single-flight) rather than stacking a second one.
		select {
		case s.ckptSig <- struct{}{}:
		default:
		}
	}
	return nil
}

// Delete removes key. It is a no-op if the key is absent.
func (s *Store) Delete(key []byte) error {
	if s.closed.Load() {
		return errClosed
	}
	h := hash64(key)
	return s.shardFor(h).del(h, key)
}

// Checkpoint is a durability barrier. In the memory-only core there is nothing to
// flush, so it is a no-op. In single-file mode it flushes every shard's tail page to
// the file, captures each shard's live index slots and frontier as the cut, then writes
// a durable index snapshot and advances the superblock to point at it. Recovery after
// this point loads the index from the snapshot and replays only the records appended
// past each shard's frontier, so a reopen costs the live key count rather than the whole
// operation history since the last compaction. Under a non-None dial the tail flush, the
// snapshot chain, and the superblock are each fsynced.
func (s *Store) Checkpoint() error {
	return s.CheckpointContext(context.Background())
}

// CheckpointContext runs a checkpoint like Checkpoint, threading ctx so a checkpoint over
// a store with many shards can be cancelled or bounded by a deadline. Cancellation is
// observed between shard captures, before the snapshot is written or the superblock
// advanced, so a cancelled checkpoint commits nothing and leaves the store unchanged.
func (s *Store) CheckpointContext(ctx context.Context) error {
	if s.closed.Load() {
		return errClosed
	}
	return s.checkpoint(ctx)
}

// checkpoint is the checkpoint body without the closed guard, shared by the public
// Checkpoint, the background loop, and Close. ckptMu makes it single-flight: at most one
// commit runs at a time, so two triggers never race the snapshot chain's allocate-and-free.
func (s *Store) checkpoint(ctx context.Context) error {
	if s.df == nil {
		return nil
	}
	s.ckptMu.Lock()
	defer s.ckptMu.Unlock()
	// Flush each shard's tail and capture its section under the same lock, so the
	// frontier names bytes already on disk and the slots match that frontier. The
	// per-shard captures are independent, the same non-atomic cut the high-water has
	// always taken, because recovery rebuilds each shard on its own.
	snaps := make([]shardSnap, len(s.shards))
	for i, sh := range s.shards {
		// Observe cancellation before each capture, while nothing is committed: a cancel
		// here returns without writing a snapshot, so the store is unchanged.
		if err := ctx.Err(); err != nil {
			return err
		}
		sh.mu.Lock()
		err := sh.log.flushTail()
		if err == nil {
			snaps[i] = sh.captureSnap()
		}
		sh.mu.Unlock()
		if err != nil {
			return err
		}
	}
	// Barrier the tail pages before the snapshot records frontiers that name them, so a
	// crash cannot leave a snapshot pointing past bytes that never reached disk.
	if s.df.dial != DurabilityNone {
		if err := s.df.sync(); err != nil {
			return err
		}
	}
	return s.df.commitSnapshot(snaps)
}

// Close releases the store. In single-file mode it checkpoints first so a clean
// shutdown loses nothing even under the None dial, then closes the file. The file
// is the durable store and is never removed. The memory-only core holds no OS
// resources and only drops its shards.
func (s *Store) Close() error {
	if s.closed.Swap(true) {
		return nil // idempotent: a second Close is a no-op, not a double free
	}
	// Stop the background compactor and checkpoint loop and wait for them to exit before
	// touching the file, so the final checkpoint never races a background one.
	if s.bgStop != nil {
		close(s.bgStop)
		s.bgWG.Wait()
	}
	if s.df != nil {
		// Stop the seal-sync time flusher before the final checkpoint so no background
		// barrier races the close-time commit; the checkpoint below issues its own.
		s.df.stopSyncLoop()
		// A final checkpoint flushes every tail and writes a fresh index snapshot even
		// under None, so a clean close is always fully recoverable and the reopen is
		// delta-bound rather than replaying the whole generation; only a crash exposes
		// the dial's loss window.
		if err := s.checkpoint(context.Background()); err != nil {
			return err
		}
		_ = s.df.f.Unlock(vfs.LockNone)
		return s.df.f.Close()
	}
	return nil
}

// Stats sums the per-shard accounting into one snapshot. It takes each shard's
// read lock briefly, so it is consistent per shard but not a global instant.
// InPlaceUpdates returns the total number of overwrites taken on the in-place path
// (FASTER's same-size overwrite of a record still in the resident, unflushed tail),
// summed across shards. It is zero on every profile but the durable evicting non-Full
// one, and on a hot same-size workload there it climbs while LogBytes and the space
// amplification stay flat, which is the in-place win made measurable.
func (s *Store) InPlaceUpdates() int64 {
	var n int64
	for _, sh := range s.shards {
		sh.mu.RLock()
		n += sh.inPlaceCount
		sh.mu.RUnlock()
	}
	return n
}

func (s *Store) Stats() Stats {
	st := Stats{MinShardKeys: -1}
	for _, sh := range s.shards {
		sh.mu.RLock()
		t := sh.index.Load()
		keys := int64(t.live)
		st.Keys += keys
		st.IndexSlots += int64(len(t.slots))
		st.LogBytes += int64(sh.logBytes)
		st.DeadBytes += int64(sh.deadBytes)
		st.CompactionBacklog += len(sh.deferred)
		evicted := int64(sh.log.evict) * sh.log.pageSize
		resident := int64(sh.logBytes) - evicted
		if resident < 0 {
			resident = 0
		}
		st.EvictedLog += evicted
		st.ResidentLog += resident
		if st.MinShardKeys < 0 || keys < st.MinShardKeys {
			st.MinShardKeys = keys
		}
		if keys > st.MaxShardKeys {
			st.MaxShardKeys = keys
		}
		sh.mu.RUnlock()
	}
	if st.MinShardKeys < 0 {
		st.MinShardKeys = 0
	}
	st.IndexBytes = st.IndexSlots * 8
	st.ResidentMem = st.IndexBytes + st.ResidentLog
	st.LiveBytes = st.LogBytes - st.DeadBytes
	st.SpaceAmplification = 1.0
	if st.LiveBytes > 0 {
		st.SpaceAmplification = float64(st.LogBytes) / float64(st.LiveBytes)
	}
	if s.df != nil {
		st.FsyncCount = s.df.syncCount.Load()
		if st.FsyncCount > 0 {
			st.FsyncAvgLatency = time.Duration(s.df.syncNanos.Load() / st.FsyncCount)
		}
	}
	st.RecoveryDuration = time.Duration(s.recoverNanos.Load())
	st.RecoveryRecords = s.recoverRecords.Load()
	return st
}
