// Package hashlog is an in-memory-first key/value engine: a resident sharded hash
// index over a per-shard hybrid log, modelled on Microsoft FASTER and Garnet (and
// adopted from the tamnd/aki v2 spike). It exists to answer one question the kv
// tree cores cannot: what does a point read cost when it is a single resident hash
// probe straight to the value, with no tree descent, no cell decode, and no MVCC
// fold.
//
// The shipped kv cores (btree, lsm, betree) answer a GET by descending an ordered
// structure, decoding a record, and folding a version group to one visible value.
// That is the right shape for ordered scans, snapshots, and transactions, but it
// is several times the per-core CPU of the resident hash probe that Valkey, Redis,
// and FASTER do, and no micro-lever changes the shape of a descend-plus-decode
// read. This engine keeps the resident fast path while staying larger than memory,
// by borrowing FASTER's hybrid log.
//
// The model, per shard:
//
//   - A resident index maps a key to a logical address: a monotonically growing
//     byte offset into that shard's log. The index is a lock-free open-addressing
//     table, so a reader probes it with atomic loads only and never touches a shared
//     mutex word; a writer publishes a slot with one atomic store under the shard
//     write lock. The keys are resident; only the resident page budget's worth of
//     values is.
//   - The log is a sequence of fixed-size pages held behind an atomic page
//     directory. Recent pages live in RAM (the mutable tail plus the read-only
//     region); once the resident page budget is exceeded the oldest resident page is
//     flushed to the shard's log file and dropped from memory (the stable region). A
//     logical address therefore tells a reader, with no extra lookup, whether the
//     record is in RAM or on disk.
//   - GET hashes the key and, in the full-resident profile, probes the index and
//     slices the value straight out of the resident page with no lock at all: the
//     read path is atomic loads only, so reads of one hot key scale across cores
//     instead of serialising on a reader-count cache line. Once eviction is possible
//     a GET enters an epoch (a couple of atomic stores to a striped slot, not a
//     reader-count read-modify-write) so a concurrent evictor cannot recycle a page
//     out from under it, copies a resident value or resolves a spilled one's stable
//     offset under the epoch, and reads a spilled value back with one ReadAt after
//     leaving the epoch.
//   - SET appends a new record to the tail page under the shard write lock and
//     publishes the index slot with one atomic store. When the tail page fills it is
//     sealed and a fresh page begins; when the resident budget is exceeded the oldest
//     page spills. In the durable eviction-possible profile a same-size SET to a key
//     whose record is still in the mutable tail window overwrites the value in place and
//     re-stamps the record LSN instead of appending, so a hot same-size key does not
//     grow the log (FASTER's overwrite win); the full-resident lock-free profile always
//     appends, because its reader aliases the page and an in-place overwrite would mutate
//     bytes under it.
//
// This is the read path Valkey has (one probe to the value) without giving up the
// larger-than-memory property the tree cores gave kv: only the resident page
// budget's worth of values, plus the key index, has to fit in RAM. The on-disk
// spill here is a scratch region for the larger-than-memory benchmark, not yet a
// recovery journal; the durable single-file layout is a later, first-principles
// design step taken only once the in-memory ceiling is proven.
package hashlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"sync/atomic"
)

// Durability is the per-store durability dial (spec 2070 doc 04 section 5, D6). It is
// set once when the store opens, not per write.
type Durability int

const (
	// DurabilityNone never fsyncs. A sealed page reaches the OS at flush, but no device
	// barrier is issued, so the on-disk bytes are a spill, not a crash-safe journal.
	// This is the memory-only benchmark regime and the larger-than-memory speed ceiling
	// (doc 04 section 5.2). It is the zero value, so the default store is None.
	DurabilityNone Durability = iota
	// DurabilityNormal fsyncs on seal boundaries (and, from M4, at checkpoints), not on
	// every SET. The loss window on a crash is the writes since the last seal sync, a
	// bounded window (doc 04 section 5.3).
	DurabilityNormal
	// DurabilityFull fsyncs before every SET returns: a SET does not acknowledge until
	// its record is in a synced extent, so nothing acknowledged is lost (doc 04 section
	// 5.4). It sits on the fsync floor at concurrency one.
	DurabilityFull
)

// Tunables holds the knobs that shape a Store. The zero value is not valid; use
// DefaultTunables and override.
type Tunables struct {
	// Shards is the number of independent index+log shards. Must be a power of two
	// so the shard for a key is a mask of its hash. More shards cut write-lock
	// contention.
	Shards int

	// PageSize is the byte size of one log page. A record must fit in a page.
	PageSize int

	// ResidentPagesPerShard caps how many log pages each shard keeps in RAM. Once a
	// shard holds more than this, its oldest resident page is flushed to the log
	// file and evicted. The total resident value budget is therefore
	// Shards * ResidentPagesPerShard * PageSize. Zero means unbounded (nothing ever
	// spills), the full-resident, fastest, RAM-bound mode.
	ResidentPagesPerShard int

	// Dir is where each shard writes its on-disk log file. Empty means the engine
	// runs memory-only: spilling is disabled even if ResidentPagesPerShard is set,
	// so an over-budget Set keeps the page resident rather than losing it.
	Dir string

	// Path selects the durable single-file mode (spec 2070): the Store is backed by
	// one file at this path that survives a crash with no lost acknowledged write.
	// Empty keeps the memory-only mode, the benchmarked ceiling. Mutually exclusive
	// with Dir. The durable mode is built behind this knob and is off by default; the
	// memory-only DefaultTunables never sets it.
	Path string

	// ExtentSize is the durable extent size in bytes. It must equal PageSize and be a
	// power of two. Zero defaults to PageSize. Ignored in memory-only mode.
	ExtentSize int

	// Durability is the durability dial (None, Normal, Full). It is meaningful only in
	// durable mode (a Path is set); selecting Normal or Full without a Path is an error
	// because there is nowhere to sync. The zero value is None, so a memory-only store
	// is always None, the benchmarked ceiling.
	Durability Durability

	// CheckpointBytes is how many durable record bytes may be appended before a
	// checkpoint is due (doc 05 section 5). It bounds the recovery replay delta. Zero
	// defaults to 256 MiB in durable mode. M4 records and exposes this and the
	// bytes-since-checkpoint counter; the automatic background scheduler that fires a
	// checkpoint when the threshold is crossed is a later milestone, so at M4 a
	// checkpoint is taken by calling Checkpoint.
	CheckpointBytes int64

	// MutableWindowPages is how many trailing log pages stay mutable (eligible for an
	// in-place same-size overwrite) in the durable eviction-possible profile (doc 04
	// section 1.3, the ReadOnlyAddress boundary). A record older than this window has
	// begun flushing and must be updated by append, never in place (doc 04 section 7.2).
	// Zero defaults to 1, the conservative window of the tail page alone. A larger window
	// admits more in-place updates at the cost of a larger volatile region a crash can
	// lose under the looser dials. It is meaningful only in durable mode.
	MutableWindowPages int

	// CompactionThreshold is the dead-byte fraction at which a sealed extent becomes
	// eligible for compaction (doc 06 section 4.2). At the default 0.5 the compactor
	// copies one live byte for every byte it frees (write amplification about 1.0, total
	// store about 2x), the balanced point between copy cost and space overhead. A higher
	// value compacts more lazily (less copying, more dead space carried); a lower value
	// compacts more eagerly (more copying, tighter file). Zero or out of (0,1] defaults
	// to 0.5. It is meaningful only in durable mode.
	CompactionThreshold float64
}

// DefaultTunables returns a full-resident, memory-only configuration: 256 shards,
// 1 MiB pages, no spill. This is the in-memory ceiling shape.
func DefaultTunables() Tunables {
	return Tunables{Shards: 256, PageSize: 1 << 20, ResidentPagesPerShard: 0, Dir: ""}
}

// Store is the hashlog engine. It is safe for concurrent use: each shard carries
// its own lock and the shard for a key is fixed by the key's hash.
type Store struct {
	shards []*shard
	mask   uint64
	t      Tunables

	// df is the durable single-file backing, non-nil only when a Path is set. In the
	// memory-only default it stays nil and no shard touches it.
	df *durableFile

	// rec holds what the open-time recovery did (M5), exposed through RecoveryStats. It
	// is the zero value for a memory-only store and for a freshly created durable file
	// (nothing to recover).
	rec recoveryStats

	// Epoch reclamation (M6, doc 07). globalEpoch is the single monotonic epoch
	// counter; it starts at 1 so the slot sentinel 0 means "not inside any epoch". slots
	// is the striped participant pool a reader enters before its lookup-and-slice.
	// nextStripe hands out stripes round-robin to Reader handles and to bare-Get calls
	// on the evicting path. The full-resident memory-only path touches none of these:
	// nothing is freed there, so there is no reclamation to protect (doc 07 section 5.1).
	globalEpoch atomic.Uint64
	slots       *slotPool
	nextStripe  atomic.Uint64

	// inPlaceUpdates counts SETs resolved by an in-place same-size overwrite instead of
	// an append (M7, doc 04 section 7). It is observability: a test or an operator can
	// confirm a hot same-size workload is taking the in-place path that keeps the log
	// from growing, rather than silently falling through to append. It stays zero in the
	// memory-only and full-resident profiles, which never overwrite in place.
	inPlaceUpdates atomic.Int64

	// Compaction observability (M8, doc 06 section 10, doc 08 section 1). compactedExtents
	// counts extents retired by a compaction pass; freedExtents counts those returned to
	// the allocator once the checkpoint that captured their repoint committed;
	// relocatedRecords and copiedBytes count the live records (and their source bytes)
	// copied forward; abandonedCopies counts copies a racing overwrite stranded (the
	// compare-and-publish abandoned them, doc 06 section 5.4); discardedTombstones counts
	// tombstones the compactor dropped under the section 3.4 rule. All stay zero on the
	// memory-only and full-resident profiles, which never compact.
	compactedExtents    atomic.Int64
	freedExtents        atomic.Int64
	relocatedRecords    atomic.Int64
	copiedBytes         atomic.Int64
	abandonedCopies     atomic.Int64
	discardedTombstones atomic.Int64

	// oversizeValues counts values stored as a cont chain rather than inline (M9, doc 03
	// section 7). It is observability: a test or an operator can confirm the oversize path
	// is taken for values that span an extent. It stays zero on the memory-only and
	// full-resident profiles, which reject an over-page value rather than span it.
	oversizeValues atomic.Int64

	// pendingRetry holds extents a compaction retired but a checkpoint could not durably
	// free this round (a commit error, or the inline free list was full, doc 06 section
	// 7.3). The next checkpoint retries them. They stay holes in their shard's directory
	// until freed, so they are not leaked and not reused early. Touched only by
	// Checkpoint, which is not called concurrently with itself.
	pendingRetry []int64
}

// New builds a Store. It returns an error if the tunables are invalid, when a Dir is
// set and a shard log file cannot be created, or when a Path is set and the durable
// file cannot be opened.
func New(t Tunables) (*Store, error) {
	if t.Shards <= 0 || t.Shards&(t.Shards-1) != 0 {
		return nil, errors.New("hashlog: Shards must be a power of two")
	}
	if t.PageSize <= 64 {
		return nil, errors.New("hashlog: PageSize too small")
	}
	if t.Durability != DurabilityNone && t.Path == "" {
		return nil, errors.New("hashlog: Normal or Full durability needs a Path")
	}

	var df *durableFile
	if t.Path != "" {
		var err error
		t, err = validateDurableTunables(t)
		if err != nil {
			return nil, err
		}
		df, err = openDurableFile(t.Path, t.Shards, int64(t.ExtentSize))
		if err != nil {
			return nil, err
		}
	}

	s := &Store{
		shards: make([]*shard, t.Shards),
		mask:   uint64(t.Shards - 1),
		t:      t,
		df:     df,
		slots:  newSlotPool(defaultSlotStripes()),
	}
	s.globalEpoch.Store(1)
	for i := range s.shards {
		sh, err := newShard(t, df, i)
		if err != nil {
			for j := 0; j < i; j++ {
				s.shards[j].close()
			}
			if df != nil {
				df.Close()
			}
			return nil, err
		}
		sh.store = s
		s.shards[i] = sh
	}
	// A durable file that already held a checkpoint (or a log written before any
	// checkpoint) is recovered now: rebuild every shard's index from the last valid
	// checkpoint plus the durable log tail (doc 05 section 6, D9), before the store
	// serves a single request. A brand-new file has nothing to recover.
	if df != nil && df.existed {
		if err := s.recover(); err != nil {
			s.Close()
			return nil, err
		}
	}
	return s, nil
}

// Close releases every shard's log file and, in durable mode, the single file. The
// Store must not be used afterward.
func (s *Store) Close() error {
	var first error
	for _, sh := range s.shards {
		if err := sh.close(); err != nil && first == nil {
			first = err
		}
	}
	if s.df != nil {
		if err := s.df.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// shardFor returns the shard that owns a key.
func (s *Store) shardFor(key []byte) *shard {
	return s.shards[hash64(key)&s.mask]
}

// Set stores value under key, replacing any previous value. The value bytes are
// copied into the log, so the caller may reuse the slice after Set returns.
func (s *Store) Set(key, value []byte) error {
	return s.shardFor(key).set(key, value)
}

// Delete removes key. It is a no-op if the key is absent. The log record is left
// in place as garbage for a later compaction to reclaim; only the index entry is
// dropped, so the key reads back as absent immediately.
func (s *Store) Delete(key []byte) error {
	return s.shardFor(key).delete(key)
}

// Get returns the value stored under key. found is false if the key is absent. In
// full-resident mode the returned slice aliases the log page and must not be
// mutated; once eviction is possible (a resident budget plus a spill dir) the
// value is copied and the slice is caller-owned.
func (s *Store) Get(key []byte) (value []byte, found bool, err error) {
	return s.shardFor(key).get(key)
}

// Len returns the number of live keys across all shards.
func (s *Store) Len() int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		n += sh.idxLive
		sh.mu.RUnlock()
	}
	return n
}

// Spilled reports how many pages have been flushed to disk across all shards. It
// is zero until a Set pushes a shard past its resident page budget, so a benchmark
// can confirm whether a run stayed in RAM or exercised the disk path.
func (s *Store) Spilled() int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		n += sh.spilledPages
		sh.mu.RUnlock()
	}
	return n
}

// InPlaceUpdates returns how many SETs the store resolved by a same-size in-place
// overwrite instead of an append (M7, doc 04 section 7). It is zero in the memory-only
// and full-resident profiles, which always append. A hot same-size workload should show
// it climbing while the log stays small, which is the FASTER overwrite win at the
// log-growth level (doc 04 section 7.3).
func (s *Store) InPlaceUpdates() int64 {
	return s.inPlaceUpdates.Load()
}

// OversizeValues returns how many values the store stored as a cont chain because they did
// not fit one extent (M9, doc 03 section 7). It is zero in the memory-only and
// full-resident profiles, which reject an over-page value. A workload with large values
// should show it climbing, confirming the spanning path is exercised rather than the value
// silently rejected.
func (s *Store) OversizeValues() int64 {
	return s.oversizeValues.Load()
}

// CompactionStats reports the compaction observability counters (M8, doc 06 section 10).
// CompactedExtents is how many extents compaction retired; FreedExtents how many of those
// a checkpoint returned to the allocator; RelocatedRecords and CopiedBytes the live
// records (and their source bytes) copied forward; AbandonedCopies the copies a racing
// overwrite stranded (the compare-and-publish abandoned them); DiscardedTombstones the
// tombstones dropped under the discard rule. It is the zero value for a memory-only store.
type CompactionStats struct {
	CompactedExtents    int64
	FreedExtents        int64
	RelocatedRecords    int64
	CopiedBytes         int64
	AbandonedCopies     int64
	DiscardedTombstones int64
}

// CompactionStats returns the current compaction counters.
func (s *Store) CompactionStats() CompactionStats {
	return CompactionStats{
		CompactedExtents:    s.compactedExtents.Load(),
		FreedExtents:        s.freedExtents.Load(),
		RelocatedRecords:    s.relocatedRecords.Load(),
		CopiedBytes:         s.copiedBytes.Load(),
		AbandonedCopies:     s.abandonedCopies.Load(),
		DiscardedTombstones: s.discardedTombstones.Load(),
	}
}

// CheckpointStats reports the checkpoint observability counters (doc 08 section 1.4):
// the committed checkpoint generation, the last snapshot's byte size, and the durable
// bytes appended since that checkpoint (which drives the cadence). It is the zero value
// for a memory-only store.
type CheckpointStats struct {
	Generation           uint64
	SnapshotBytes        uint64
	BytesSinceCheckpoint int64
}

// CheckpointStats returns the current checkpoint counters.
func (s *Store) CheckpointStats() CheckpointStats {
	if s.df == nil {
		return CheckpointStats{}
	}
	return CheckpointStats{
		Generation:           s.df.sb.generation,
		SnapshotBytes:        uint64(s.df.snapBytes.Load()),
		BytesSinceCheckpoint: s.df.bytesSinceCkpt.Load(),
	}
}

// RecoveryStats reports what the open-time recovery did (doc 08 section 1.5): how many
// delta records were replayed across all shards, how many log bytes each shard
// replayed past its checkpoint frontier, and where each shard's CRC-stop fired (the
// torn-tail logical address, or -1 when the scan reached a clean end with no torn
// record). It is the zero value for a memory-only store and for a freshly created
// durable file. The per-shard slices are indexed by shard id.
type RecoveryStats struct {
	ReplayedRecords int64
	BytesReplayed   []int64
	TornTailOffset  []int64
}

// RecoveryStats returns the counters recorded by the open-time recovery.
func (s *Store) RecoveryStats() RecoveryStats {
	return RecoveryStats{
		ReplayedRecords: s.rec.replayedRecords,
		BytesReplayed:   s.rec.bytesReplayed,
		TornTailOffset:  s.rec.tornTailOff,
	}
}

// Checkpoint writes a durable index snapshot and commits it through the superblock
// double-slot (doc 05 section 4, D8). It captures each shard's consistent cut, writes
// the snapshot extents and fsyncs them, then flips the superblock generation in one
// barrier, the atomic commit point. It is a no-op error in memory-only mode. M4
// provides the explicit call; a background cadence scheduler is a later milestone.
func (s *Store) Checkpoint() error {
	if s.df == nil {
		return errors.New("hashlog: Checkpoint requires durable mode")
	}
	sections := make([]snapSection, len(s.shards))
	frontiers := make([]shardFrontier, len(s.shards))
	// Extents compaction retired and a prior checkpoint could not yet durably free are
	// retried first, ahead of this round's freshly retired ones (doc 06 section 7.3).
	pending := s.pendingRetry
	s.pendingRetry = nil
	for i, sh := range s.shards {
		var pf []int64
		sections[i], frontiers[i], pf = sh.captureCut()
		pending = append(pending, pf...)
	}
	barrier := s.t.Durability != DurabilityNone
	stream := encodeSnapshot(len(s.shards), s.df.sb.generation+1, sections)
	root, err := s.df.writeSnapshot(stream, barrier)
	if err != nil {
		// The commit was not attempted; keep the retired extents pending so a later
		// checkpoint frees them. They remain holes in their directories, not leaked.
		s.pendingRetry = pending
		return err
	}
	sync := s.df.f.Sync
	if barrier {
		sync = s.df.syncData
	}
	overflow, err := s.df.commitCheckpoint(root, uint64(len(stream)), frontiers, pending, sync)
	if err != nil {
		s.pendingRetry = pending
		return err
	}
	// The checkpoint committed: record each shard's durable frontier as the lower bound a
	// tombstone discard checks against (doc 06 section 3.4), count the extents this
	// checkpoint freed to the allocator, and carry forward any the inline free list could
	// not hold for the next checkpoint to retry.
	for i, sh := range s.shards {
		sh.ckptFrontier.Store(int64(frontiers[i].frontierLSN))
	}
	s.freedExtents.Add(int64(len(pending) - len(overflow)))
	s.pendingRetry = overflow
	return nil
}

// captureCut takes shard sh's consistent cut for a checkpoint (doc 05 section 3): it
// briefly holds the write lock, copies the live tuples into a private slice, and reads
// the durable frontier as the cut LSN F_shard. It first writes the shard's resident log
// pages to disk so every value the snapshot tuples point at has an on-disk home: a
// checkpoint is a recovery point, and recovery reads those values back by address (doc
// 05 section 3, the snapshot references durable value locations). Under Normal and Full
// the flush also syncs and advances the frontier so the recorded frontier is genuinely
// durable up to the cut (doc 05 section 5); under None the bytes are written but not
// barriered, matching the dial's no-per-write-sync contract while still leaving a clean
// reopen recoverable. The copy form gives an exact as-of-F_shard view with no post-cut
// bleed.
func (sh *shard) captureCut() (snapSection, shardFrontier, []int64) {
	sh.mu.Lock()
	if sh.df != nil {
		sh.flushDurable(sh.durability != DurabilityNone)
	}
	t := sh.index.Load()
	tuples := make([]snapTuple, 0, sh.idxLive)
	for j := range t.slots {
		e := t.slots[j].Load()
		if e == nil || e == tombstone {
			continue
		}
		tuples = append(tuples, snapTuple{key: append([]byte(nil), e.key...), loc: e.loc})
	}
	fr := shardFrontier{
		frontierLSN: uint64(sh.frontier.Load()),
		tailExtent:  sh.pageExtent[sh.tailPage],
		tailOff:     uint64(sh.pageFill[sh.tailPage]),
	}
	sec := snapSection{shard: sh.shardID, frontierLSN: fr.frontierLSN, tuples: tuples}
	// Gather the extents compaction retired since the last checkpoint under the same cut
	// lock that captured the snapshot above (M8, doc 06 section 7.3). Both reflect the same
	// moment: every retired extent has had its records repointed off in the index this
	// snapshot records, so the checkpoint that durably frees these extents is the same one
	// that captured the index moving off them, the interlock that excludes the
	// dangling-pointer case (doc 06 section 7.2).
	pending := sh.pendingFree
	sh.pendingFree = nil
	sh.mu.Unlock()
	return sec, fr, pending
}

// valLoc points the index straight at a value in the log: addr is the logical
// address of the value's first byte, vlen its length. Pointing at the value
// instead of the record header lets a resident GET return the value with no varint
// decode.
type valLoc struct {
	addr int64
	vlen uint32
}

// entry is one published index slot. It is immutable once stored: an overwrite or a
// delete stores a fresh *entry (or the tombstone) into the slot rather than
// mutating this one, so a lock-free reader that loaded the pointer always sees a
// consistent key/loc pair. thash is the table hash, kept inline so a probe rejects
// a non-matching slot without touching the key bytes.
type entry struct {
	thash uint64
	key   []byte
	loc   valLoc
}

// tombstone marks a slot whose key was deleted. It keeps the open-addressing probe
// chain intact (a lookup must not stop on it) until the next grow drops it.
var tombstone = &entry{}

// idxTable is a power-of-two open-addressing hash table with atomically published
// slots. Readers probe it with atomic loads and no lock; the writer mutates it only
// under the shard write lock. A grow builds a new table and swaps the shard's
// pointer, so a reader holding the old table still sees every key that existed when
// it loaded the pointer.
type idxTable struct {
	slots []atomic.Pointer[entry]
	mask  uint64
}

func newIdxTable(min int) *idxTable {
	size := 8
	for size < min {
		size <<= 1
	}
	return &idxTable{slots: make([]atomic.Pointer[entry], size), mask: uint64(size - 1)}
}

// lookup probes for key and returns its location. It does atomic loads only, so it
// is safe to call without any lock concurrently with a writer publishing slots.
func (t *idxTable) lookup(thash uint64, key []byte) (valLoc, bool) {
	if e := t.lookupEntry(thash, key); e != nil {
		return e.loc, true
	}
	return valLoc{}, false
}

// lookupEntry probes for key and returns its published *entry, or nil if absent. It
// does atomic loads only, so it is safe without any lock concurrently with a writer. The
// in-place fast path (tryInPlace) uses it to read the current location and value length
// under the shard write lock before deciding whether a same-size overwrite qualifies.
func (t *idxTable) lookupEntry(thash uint64, key []byte) *entry {
	i := thash & t.mask
	for {
		e := t.slots[i].Load()
		if e == nil {
			return nil
		}
		if e != tombstone && e.thash == thash && bytes.Equal(e.key, key) {
			return e
		}
		i = (i + 1) & t.mask
	}
}

// pageSet is the log's page directory, published behind an atomic pointer so the
// lock-free read path can index it without holding the shard lock. A structural
// change (rolling a new page, or nil-ing a spilled one) builds a fresh pageSet and
// stores it; writing record bytes into an existing page does not, and is made
// visible to readers by the atomic store of the index entry that points at it.
//
// diskOff is parallel to pages: where pages[pid] is nil (the page was spilled),
// diskOff[pid] is the byte offset in the backing file where the page's bytes live, so
// a lock-free evicting read resolves a spilled value's offset from the same immutable
// snapshot it read the page directory from (M6). A resident page's diskOff entry is
// unread (the reader uses the buffer), and it is published only when eviction nils the
// page, after which it is immutable because a spilled page's offset never moves (page
// ids are never reused and an extent's offset is fixed once allocated).
type pageSet struct {
	pages   [][]byte
	diskOff []int64
}

// shard is one index+log partition. The log bookkeeping is guarded by mu; the index
// and page directory are published through atomic pointers so reads in the
// full-resident profile take no lock.
type shard struct {
	mu sync.RWMutex

	// index is the lock-free hash index. Readers load it and probe with atomic
	// loads; the writer publishes slots and swaps the table under mu.
	index atomic.Pointer[idxTable]
	// idxLive is the live key count and idxOcc the count of occupied slots including
	// tombstones; both are maintained under mu and drive the grow threshold.
	idxLive int
	idxOcc  int

	// pages is the log's page directory behind an atomic pointer; pageID indexes it and
	// a nil entry means the page was spilled (its file offset is in the same pageSet's
	// diskOff). store is the back-pointer to the owning Store, for the global epoch and
	// the slot pool; it is set once in New before the store serves a request.
	pages atomic.Pointer[pageSet]
	store *Store

	tailPage int64 // pageID currently being appended to
	tailPos  int   // append offset within the tail page

	// residentOrder lists resident pageIDs oldest-first, so eviction pops the front.
	residentOrder []int64

	spilledPages int

	pageSize    int
	residentCap int // ResidentPagesPerShard; 0 means unbounded
	evicts      bool
	// inPlace gates the durable same-size in-place overwrite (M7, doc 04 section 7); it
	// is true only on the durable eviction-possible profile. mutableWindow is how many
	// trailing pages stay in-place-eligible, the ReadOnlyAddress lag (doc 04 section 1.3).
	inPlace       bool
	mutableWindow int
	file          *os.File
	fileEnd     int64  // next free byte offset in the scratch log file (Dir mode)
	scratch     []byte // reusable record-encode buffer, only touched under mu

	// Durable single-file mode (spec 2070, set only when a Path is configured). df is
	// the shared file; shardID tags this shard's extents; pageExtent maps a spilled
	// page id to the extent that holds it (parallel to diskOff), or -1 while the page
	// is resident and not yet spilled. The memory-only path leaves df nil and never
	// touches any of this.
	df         *durableFile
	shardID    int
	pageExtent []int64

	// Compaction state (M8, doc 06). deadBytes is the per-page in-memory dead-byte tally
	// (parallel to pageExtent), credited under the write lock by the writer that kills a
	// record (an overwrite or a delete, doc 06 section 2) and recomputed exactly on
	// recovery; it drives which sealed extent to compact. compactionThreshold is the dead
	// fraction at which a sealed extent is eligible. pendingFree holds extents this shard
	// retired by compaction that are not yet durably free: they are holes in the directory
	// and the next checkpoint commits their free (doc 06 section 7.3). ckptFrontier is the
	// frontier LSN of the last committed checkpoint for this shard, the safe lower bound a
	// tombstone discard checks against (doc 06 section 3.4). compactMu serialises
	// compaction passes on this shard so two never retire the same extent. All are unused
	// on the memory-only and full-resident profiles.
	deadBytes           []int64
	compactionThreshold float64
	pendingFree         []int64
	ckptFrontier        atomic.Int64
	compactMu           sync.Mutex

	// liveOversizeExtents is how many oversize-cont extents this shard's live values
	// currently occupy (M9, doc 03 section 7). It is maintained under the write lock: an
	// oversize SET adds its chain length, and superseding or deleting an oversize value
	// moves its chain to pendingFree and subtracts it. It is the conservation term for the
	// cont extents, which are allocated but live in no page directory, so a test can prove
	// every extent is accounted for (in a page, free, in the snapshot, a hole, or a live
	// cont chain). It stays zero where oversize is rejected.
	liveOversizeExtents int64

	// Durable frontier state (M3, doc 04 section 4). durability is the dial; frontier
	// is the highest LSN known to sit in a synced extent of this shard, advanced only
	// after a real fsync. The parallel per-page arrays track, for each page, how many
	// bytes hold records (pageFill), how many of those are already written to the
	// page's extent (pageFlushed), and the highest LSN on the page (pageMaxLSN). All
	// are maintained under mu; frontier is atomic so a reader observes a published
	// advance without the lock.
	durability  Durability
	frontier    atomic.Int64
	pageFill    []int
	pageFlushed []int
	pageMaxLSN  []int64

	// Epoch reclamation state (M6, doc 07). deferred holds page buffers retired by
	// eviction, each stamped with the global epoch at retire time; the reclaimer moves an
	// entry to freeBufs only once the safe epoch passes its retire epoch, so a buffer is
	// recycled only after every reader that could be slicing it has left. freeBufs is the
	// recycle pool a fresh tail page draws from instead of allocating, keeping the
	// fixed-resident-budget engine near zero allocation on the page path. Both are
	// maintained under mu (eviction and rolling both hold the write lock). A memory-only
	// shard never evicts, so both stay empty and the page path keeps allocating fresh,
	// bit-for-bit with the benchmarked ceiling.
	deferred []deferredFree
	freeBufs [][]byte
}

func newShard(t Tunables, df *durableFile, shardID int) (*shard, error) {
	mutableWindow := t.MutableWindowPages
	if mutableWindow <= 0 {
		mutableWindow = 1
	}
	threshold := t.CompactionThreshold
	if threshold <= 0 || threshold > 1 {
		threshold = 0.5
	}
	sh := &shard{
		pageSize:            t.PageSize,
		residentCap:         t.ResidentPagesPerShard,
		scratch:             make([]byte, 0, 256),
		df:                  df,
		shardID:             shardID,
		durability:          t.Durability,
		mutableWindow:       mutableWindow,
		compactionThreshold: threshold,
	}
	sh.index.Store(newIdxTable(1024))
	// Page 0 starts resident and empty.
	sh.pages.Store(&pageSet{pages: [][]byte{make([]byte, t.PageSize)}, diskOff: []int64{0}})
	sh.pageExtent = append(sh.pageExtent, -1)
	sh.pageFill = append(sh.pageFill, 0)
	sh.pageFlushed = append(sh.pageFlushed, 0)
	sh.pageMaxLSN = append(sh.pageMaxLSN, 0)
	sh.deadBytes = append(sh.deadBytes, 0)
	sh.residentOrder = append(sh.residentOrder, 0)
	if df == nil && t.Dir != "" {
		f, err := os.CreateTemp(t.Dir, "hashlog-shard-*.log")
		if err != nil {
			return nil, err
		}
		sh.file = f
	}
	sh.evicts = sh.residentCap > 0 && (sh.file != nil || df != nil)
	// In-place same-size update is enabled only on the durable eviction-possible profile
	// (doc 04 section 7.3): the reader there copies the value out (under the epoch guard),
	// so an in-place overwrite never mutates bytes a reader still aliases. The full-resident
	// lock-free profile (evicts false) aliases the page on read and always appends, and the
	// Dir scratch profile (df nil) is not a recovery journal so re-stamping an LSN is moot.
	sh.inPlace = sh.evicts && df != nil
	return sh, nil
}

func (sh *shard) close() error {
	// In durable mode the file is shared and owned by the Store, so the shard does not
	// close it. Only a private scratch file (Dir mode) is closed and removed here.
	if sh.file == nil {
		return nil
	}
	name := sh.file.Name()
	err := sh.file.Close()
	_ = os.Remove(name)
	return err
}

// recordLen returns the encoded size of a key/value record.
func recordLen(key, value []byte) int {
	return uvarintLen(uint64(len(key))) + len(key) +
		uvarintLen(uint64(len(value))) + len(value)
}

// encodeRecord writes a key/value record into dst (which must be at least
// recordLen long) and returns the number of bytes written.
func encodeRecord(dst, key, value []byte) int {
	n := binary.PutUvarint(dst, uint64(len(key)))
	n += copy(dst[n:], key)
	n += binary.PutUvarint(dst[n:], uint64(len(value)))
	n += copy(dst[n:], value)
	return n
}

// rollFor makes room in the tail page for a record of rl bytes. When the record does
// not fit, it seals the current tail page and starts a fresh one, publishing the new
// directory before any record lands on it so a reader that later sees the record's index
// entry also sees the page it lives on. Under the Normal dial the seal is a group-commit
// flush point, so the sealed pages are made durable here (doc 04 sections 5.3, 6.2). It
// runs under the shard write lock and is shared by the durable SET and DELETE appends.
func (sh *shard) rollFor(rl int) {
	if sh.tailPos+rl <= sh.pageSize {
		return
	}
	if sh.df != nil && sh.durability == DurabilityNormal {
		sh.flushDurable(true)
	}
	ps := sh.pages.Load()
	sh.tailPage++
	sh.tailPos = 0
	np := make([][]byte, len(ps.pages)+1)
	copy(np, ps.pages)
	np[sh.tailPage] = sh.newPageBuf()
	nd := make([]int64, len(ps.diskOff)+1)
	copy(nd, ps.diskOff)
	sh.pages.Store(&pageSet{pages: np, diskOff: nd})
	sh.pageExtent = append(sh.pageExtent, -1)
	sh.pageFill = append(sh.pageFill, 0)
	sh.pageFlushed = append(sh.pageFlushed, 0)
	sh.pageMaxLSN = append(sh.pageMaxLSN, 0)
	sh.deadBytes = append(sh.deadBytes, 0)
	sh.residentOrder = append(sh.residentOrder, sh.tailPage)
	sh.evictIfNeeded()
}

func (sh *shard) set(key, value []byte) error {
	// In durable mode the record carries the self-describing header (lsn, flags, CRC)
	// the log needs for recovery; the memory-only store keeps the leaner record so its
	// benchmarked ceiling does not move. Either way the index ends up pointing straight
	// at the value, so the read path is identical.
	durable := sh.df != nil
	var rl int
	if durable {
		rl = durableRecordLen(key, value)
	} else {
		rl = recordLen(key, value)
	}
	// A value whose inline record overflows a page is oversize (M9, doc 03 section 7): in
	// durable eviction-possible mode it is stored as a cont chain; anywhere else it is
	// rejected. The full-resident lock-free profile aliases the page on read and cannot
	// return a spanning value zero-copy, and the memory-only profile has no file to span
	// into, so both reject it rather than carry the oversize branch.
	oversize := durable && rl > sh.pageSize
	if oversize && !sh.inPlace {
		return errors.New("hashlog: value too large for this store profile; oversize values need a durable store with a resident page budget")
	}
	if !oversize && rl > sh.pageSize {
		return errors.New("hashlog: record larger than page size")
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if oversize {
		return sh.setOversizeLocked(key, value)
	}

	// In-place fast case (M7, doc 04 section 7, D7): a same-size SET to a key whose record
	// is still in the mutable tail window overwrites the value bytes and re-stamps the LSN
	// instead of appending, so a hot same-size key does not grow the log. It is gated to the
	// durable eviction-possible profile (sh.inPlace), where the reader copies the value out
	// rather than aliasing the page. A miss (absent key, different size, or a record that has
	// fallen out of the mutable window) falls through to the append path unchanged.
	if sh.inPlace && sh.tryInPlace(key, value) {
		sh.store.inPlaceUpdates.Add(1)
		return nil
	}

	thash := tableHash(key)
	// In durable mode an overwrite kills the previous record: before the index repoints
	// off it, credit its bytes to its extent's dead tally (M8, doc 06 section 2.1). The
	// old entry is still in the index here (indexPut below republishes it), and it carries
	// the key and the old value length, which size the dead record with no value read.
	if durable {
		if old := sh.index.Load().lookupEntry(thash, key); old != nil {
			sh.supersedeOldLocked(old)
		}
	}
	sh.rollFor(rl)
	ps := sh.pages.Load()
	page := ps.pages[sh.tailPage]
	recStart := sh.tailPage*int64(sh.pageSize) + int64(sh.tailPos)
	// Encode the record and compute the value's offset from the record start. The
	// value sits after the header and the key, so point the index straight at it and
	// reads skip the record decode. The index publish (an atomic store) is the release
	// that makes the record bytes above visible to a lock-free reader's acquiring load.
	var valOff int
	if durable {
		lsn := sh.df.nextLSN()
		n := encodeDurableRecord(page[sh.tailPos:], lsn, key, value, 0)
		sh.tailPos += n
		valOff = durableValOff(key, value)
		// Record where the tail page now ends and the highest LSN it holds, so a flush
		// knows how much to write and how far the frontier may advance.
		sh.pageFill[sh.tailPage] = sh.tailPos
		sh.pageMaxLSN[sh.tailPage] = int64(lsn)
		sh.df.bytesSinceCkpt.Add(int64(n))
	} else {
		n := encodeRecord(page[sh.tailPos:], key, value)
		sh.tailPos += n
		valOff = uvarintLen(uint64(len(key))) + len(key) + uvarintLen(uint64(len(value)))
	}
	sh.indexPut(thash, key, valLoc{addr: recStart + int64(valOff), vlen: uint32(len(value))})
	// Under Full the SET does not return until its record is in a synced extent: flush
	// the tail's new bytes and fsync, which advances the frontier past this LSN (doc 04
	// section 5.4). Under None and Normal the SET returns here without a per-write sync.
	if durable && sh.durability == DurabilityFull {
		sh.flushDurable(true)
	}
	return nil
}

// creditDeadLocked adds the byte length of the record entry e points at to its page's
// dead tally (M8, doc 06 section 2.1). It is called by the writer that kills the record,
// an overwrite or a delete, under the shard write lock. The record's geometry comes from
// the key and the stored value length alone, so no value byte is read: the header offset
// recovers the record start from the value pointer, and the record length is the fixed
// overhead plus the key and value. A location outside the current page range is ignored
// rather than panicking, so a stale or relocated entry never indexes out of bounds.
func (sh *shard) creditDeadLocked(e *entry) {
	keyLen := len(e.key)
	// length() masks the oversize marker, so an oversize home record is sized by its 24-byte
	// descriptor (its inline value), exactly the bytes it occupies in the log; the cont
	// extents are freed separately by supersedeOldLocked, not counted as log dead space.
	valLen := int(e.loc.length())
	headerStart := e.loc.addr - int64(durableValOffFor(keyLen, valLen))
	pid := headerStart / int64(sh.pageSize)
	if pid < 0 || pid >= int64(len(sh.deadBytes)) {
		return
	}
	sh.deadBytes[pid] += int64(durableRecordLenFor(keyLen, valLen))
}

// readOnlyAddress is the lowest logical address still eligible for an in-place update
// (doc 04 section 1.3, FASTER's ReadOnlyAddress). It is the start of the oldest page in
// the mutable window: the tail page and the mutableWindow-1 pages behind it. A record at
// or above it is purely resident and not yet flushing, so an in-place overwrite cannot
// race a flush or diverge from a durable copy (doc 04 section 7.2). It is a pure function
// of the tail page and the window, computed under the shard write lock, no stored state.
func (sh *shard) readOnlyAddress() int64 {
	lo := sh.tailPage - int64(sh.mutableWindow-1)
	if lo < 0 {
		lo = 0
	}
	return lo * int64(sh.pageSize)
}

// tryInPlace attempts the same-size in-place overwrite of key's current record and
// reports whether it took it (doc 04 section 7.1, the decision procedure). It runs under
// the shard write lock. It overwrites only when the key is present, the new value is
// exactly the current size, the record is in the mutable window (at or above
// readOnlyAddress), and the record's bytes are not already flushed to disk (so the Full
// dial, which syncs the tail per write, naturally never qualifies, doc 04 section 7.2).
// On a hit it re-encodes the record in place with a fresh LSN (which rewrites the value
// and the CRC over the same byte span, since the key and value size are unchanged) and
// advances the page's max LSN. The index entry already points at this value's location,
// which an in-place overwrite leaves unchanged, so there is nothing to republish: the read
// path serializes against this write with the shard read lock (doc 04 section 7.3), so the
// write lock's release publishes the rewritten bytes to a reader's acquiring read lock.
func (sh *shard) tryInPlace(key, value []byte) bool {
	thash := tableHash(key)
	e := sh.index.Load().lookupEntry(thash, key)
	if e == nil || int(e.loc.vlen) != len(value) {
		return false
	}
	headerStart := e.loc.addr - int64(durableValOff(key, value))
	if headerStart < sh.readOnlyAddress() {
		return false
	}
	pid := headerStart / int64(sh.pageSize)
	off := int(headerStart % int64(sh.pageSize))
	if off < sh.pageFlushed[pid] {
		// The record's bytes are already on disk; overwriting them in place would be the
		// forbidden in-place durable mutation (doc 04 section 7.2). Append instead.
		return false
	}
	page := sh.pages.Load().pages[pid]
	if page == nil {
		return false
	}
	lsn := sh.df.nextLSN()
	encodeDurableRecord(page[off:], lsn, key, value, 0)
	if int64(lsn) > sh.pageMaxLSN[pid] {
		sh.pageMaxLSN[pid] = int64(lsn)
	}
	return true
}

// indexPut publishes a key/location into the index, growing the table first when it
// is about to cross the load-factor threshold. It runs under the shard write lock.
func (sh *shard) indexPut(thash uint64, key []byte, loc valLoc) {
	t := sh.index.Load()
	if sh.idxOcc+1 > int((t.mask+1)*7/10) {
		t = sh.growIndex()
	}
	i := thash & t.mask
	firstTomb := int64(-1)
	for {
		e := t.slots[i].Load()
		if e == nil {
			slot := i
			if firstTomb >= 0 {
				slot = uint64(firstTomb) // reclaim a tombstone, occupancy unchanged
			} else {
				sh.idxOcc++
			}
			t.slots[slot].Store(&entry{thash: thash, key: append([]byte(nil), key...), loc: loc})
			sh.idxLive++
			return
		}
		if e == tombstone {
			if firstTomb < 0 {
				firstTomb = int64(i)
			}
		} else if e.thash == thash && bytes.Equal(e.key, key) {
			// Overwrite: republish with the new location, reusing the stored key.
			t.slots[i].Store(&entry{thash: thash, key: e.key, loc: loc})
			return
		}
		i = (i + 1) & t.mask
	}
}

// growIndex builds a larger table sized to the live key count, drops tombstones,
// and swaps it in. A concurrent lock-free reader still holding the old table sees
// every key that existed when it loaded the pointer; keys inserted after the swap
// land only in the new table, which is the ordinary get-versus-concurrent-put race.
func (sh *shard) growIndex() *idxTable {
	old := sh.index.Load()
	nt := newIdxTable((sh.idxLive + 1) * 2)
	for j := range old.slots {
		e := old.slots[j].Load()
		if e == nil || e == tombstone {
			continue
		}
		i := e.thash & nt.mask
		for nt.slots[i].Load() != nil {
			i = (i + 1) & nt.mask
		}
		nt.slots[i].Store(e)
	}
	sh.idxOcc = sh.idxLive
	sh.index.Store(nt)
	return nt
}

// indexDeleteLocked tombstones key's slot when present and reports whether it was found.
// It runs under the shard write lock. The tombstone (rather than a cleared slot) keeps
// the open-addressing probe chain intact for a concurrent lock-free reader.
func (sh *shard) indexDeleteLocked(thash uint64, key []byte) bool {
	t := sh.index.Load()
	i := thash & t.mask
	for {
		e := t.slots[i].Load()
		if e == nil {
			return false
		}
		if e != tombstone && e.thash == thash && bytes.Equal(e.key, key) {
			t.slots[i].Store(tombstone)
			sh.idxLive--
			return true
		}
		i = (i + 1) & t.mask
	}
}

func (sh *shard) delete(key []byte) error {
	thash := tableHash(key)
	if sh.df == nil {
		// Memory-only: drop the index entry. There is no log to make the delete durable,
		// and appending nothing keeps the benchmarked ceiling untouched.
		sh.mu.Lock()
		sh.indexDeleteLocked(thash, key)
		sh.mu.Unlock()
		return nil
	}
	// Durable: a delete appends a tombstone record so it survives a crash (D7). The
	// tombstone carries a fresh LSN, strictly greater than the key's last SET, so replay
	// resolves the key absent last-writer-wins (doc 05 section 6). A delete of an absent
	// key has nothing to make durable, so it appends no record.
	rl := durableRecordLen(key, nil)
	if rl > sh.pageSize {
		return errors.New("hashlog: tombstone larger than page size")
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	// Credit the data record this delete kills to its extent's dead tally before dropping
	// the index entry (M8, doc 06 section 2.1). A delete of an absent key has no record to
	// kill and nothing to make durable, so it appends no tombstone.
	old := sh.index.Load().lookupEntry(thash, key)
	if old == nil {
		return nil
	}
	sh.supersedeOldLocked(old)
	sh.indexDeleteLocked(thash, key)
	sh.rollFor(rl)
	ps := sh.pages.Load()
	page := ps.pages[sh.tailPage]
	lsn := sh.df.nextLSN()
	n := encodeDurableRecord(page[sh.tailPos:], lsn, key, nil, flagTombstone)
	sh.tailPos += n
	sh.pageFill[sh.tailPage] = sh.tailPos
	sh.pageMaxLSN[sh.tailPage] = int64(lsn)
	sh.df.bytesSinceCkpt.Add(int64(n))
	// Under Full the delete does not return until its tombstone is in a synced extent.
	if sh.durability == DurabilityFull {
		sh.flushDurable(true)
	}
	return nil
}

// evictIfNeeded flushes resident pages to disk until the shard is back within its
// resident page budget. With no budget or no backing (neither a scratch file nor a
// durable file) it does nothing and pages stay resident. It runs under the shard
// write lock, so the slow-path readers it coordinates with (which hold the read
// lock) never observe a half-updated directory.
//
// In durable mode the spilled page goes into an extent allocated from the one file's
// pool rather than appended to a per-shard scratch file: that is the M1 substrate
// swap, changing where the bytes live, not what they are.
func (sh *shard) evictIfNeeded() {
	if sh.residentCap <= 0 || (sh.file == nil && sh.df == nil) {
		return
	}
	// Keep the tail page resident always (it is still being appended), so the
	// effective cap on older pages is residentCap, evicting from the front.
	evicted := false
	for len(sh.residentOrder) > sh.residentCap {
		pid := sh.residentOrder[0]
		ps := sh.pages.Load()
		page := ps.pages[pid]
		if page == nil {
			sh.residentOrder = sh.residentOrder[1:]
			continue
		}

		var dOff int64
		if sh.df != nil {
			// Durable mode: write the page's record bytes into its extent (allocating one
			// the first time), then drop the resident copy. writePageRemainder writes
			// only the bytes not already flushed by a durability sync, so a page the Full
			// dial already flushed costs no second write, and it returns the extent base so
			// a spilled read is the same single ReadAt as the scratch path.
			base, err := sh.writePageRemainder(pid, page)
			if err != nil {
				return
			}
			dOff = base
		} else {
			off := sh.fileEnd
			if _, err := sh.file.WriteAt(page, off); err != nil {
				// On a write error keep the page resident rather than lose data; stop
				// evicting this round and leave the order untouched.
				return
			}
			sh.fileEnd += int64(len(page))
			dOff = off
		}

		// Publish the page directory with this page nilled and its disk offset filled, in
		// one atomic store: a reader that loads the new pageSet sees nil at pid and the
		// offset in the same snapshot, and a reader that loaded the old pageSet still sees
		// the resident buffer. Publish before the retire (doc 07 section 6.1) so no new
		// reader can reach the buffer when it is retired; only readers that loaded the old
		// directory can still slice it, and those are exactly the readers the epoch waits on.
		np := make([][]byte, len(ps.pages))
		copy(np, ps.pages)
		nd := make([]int64, len(ps.diskOff))
		copy(nd, ps.diskOff)
		np[pid] = nil
		nd[pid] = dOff
		sh.pages.Store(&pageSet{pages: np, diskOff: nd})
		sh.retirePageBufLocked(page)
		sh.residentOrder = sh.residentOrder[1:]
		sh.spilledPages++
		evicted = true
	}
	if evicted {
		// Drain anything whose retire epoch the safe epoch has now passed, then advance
		// the global epoch so the next retire batch is stamped higher and the safe epoch
		// can move past this round's retires once their readers leave (doc 07 section 2.5).
		sh.reclaimLocked()
		sh.store.globalEpoch.Add(1)
	}
}

// newPageBuf returns a page-size buffer for a fresh tail page. It reuses a buffer the
// reclaimer freed (a previously evicted page whose readers have all left, doc 07
// section 6.1, 6.4), clearing it so it reads as a fresh make, and falls back to a new
// allocation when the recycle pool is empty. A memory-only shard never evicts, so its
// pool stays empty and this always allocates fresh, keeping the benchmarked ceiling.
func (sh *shard) newPageBuf() []byte {
	if n := len(sh.freeBufs); n > 0 {
		buf := sh.freeBufs[n-1]
		sh.freeBufs[n-1] = nil
		sh.freeBufs = sh.freeBufs[:n-1]
		clear(buf)
		return buf
	}
	return make([]byte, sh.pageSize)
}

// retirePageBufLocked defers an evicted page buffer behind the epoch instead of
// recycling it immediately (doc 07 section 2.3, 6.1). It stamps the buffer with the
// current global epoch and pushes it onto the shard's deferred-free list; the buffer
// is recycled only once the safe epoch passes that epoch, by which point no reader can
// still be slicing it. It runs under the shard write lock.
func (sh *shard) retirePageBufLocked(buf []byte) {
	r := sh.store.globalEpoch.Load()
	sh.deferred = append(sh.deferred, deferredFree{kind: retirePageBuf, buf: buf, retireEpoch: r})
}

// reclaimLocked frees every deferred object whose retire epoch the safe epoch has
// strictly passed (doc 07 section 2.3, the reclaim step). The safe epoch is the
// minimum epoch any active reader is inside, so an object retired at r is freed only
// when every reader that could hold a reference into it has left, which is the I-epoch
// no-use-after-free invariant (doc 07 section 9.4). A freed page buffer returns to the
// recycle pool; a freed extent (M8) returns to the allocator. It runs under the shard
// write lock.
func (sh *shard) reclaimLocked() {
	if len(sh.deferred) == 0 {
		return
	}
	safe := sh.store.slots.safeEpoch()
	kept := sh.deferred[:0]
	for _, d := range sh.deferred {
		if d.retireEpoch < safe {
			if d.kind == retirePageBuf {
				sh.freeBufs = append(sh.freeBufs, d.buf)
			}
			// Compaction (M8) frees its retired extents through the checkpoint-gated
			// pending-free path (compact.go and Checkpoint), not this epoch deferred list:
			// on the durable evicting profile reads take the shard read lock, so a retired
			// extent has no in-flight lock-free reader and needs no epoch drain (doc 06
			// section 6). The retireExtent deferred kind stays reserved for a future
			// lock-free-profile compactor.
			continue
		}
		kept = append(kept, d)
	}
	sh.deferred = kept
}

// ensureExtent assigns an extent to page pid if it has none, growing the file to cover
// it and writing the extent's self-describing header, and returns the extent's body
// byte offset (past the header). It runs under the shard write lock. A page keeps the
// same extent for its life, so a partially flushed tail page and its later eviction
// write into the one extent.
//
// The header carries the owning shard, the page's logical base address, and the
// previous extent in the shard's chain, written before any record body lands in the
// extent so recovery can find and order the shard's extents from the file alone (doc 03
// section 5). The next link is left unset: recovery orders a shard's extents by their
// base address, so it never needs the forward link, and the compactor that splices the
// chain is M8 (recorded in the implementation spec-resolution note).
func (sh *shard) ensureExtent(pid int64) (int64, error) {
	ext := sh.pageExtent[pid]
	if ext < 0 {
		id, _ := sh.df.alloc.alloc()
		if err := sh.df.growExtent(id); err != nil {
			sh.df.alloc.freeExtent(id)
			return 0, err
		}
		prev := int64(-1)
		if pid > 0 {
			prev = sh.pageExtent[pid-1]
		}
		if err := sh.df.writeLogExtentHeader(id, sh.shardID, prev, pid*int64(sh.pageSize)); err != nil {
			sh.df.alloc.freeExtent(id)
			return 0, err
		}
		ext = id
		sh.pageExtent[pid] = id
	}
	return sh.df.logBodyOffset(ext), nil
}

// writePageRemainder writes the record bytes of page pid that are not yet on disk into
// its extent and marks them flushed, then returns the extent body base so the value
// bytes read back at base plus the in-page offset. It writes only the unflushed suffix,
// so re-flushing a growing tail page costs one write per appended record, not a rewrite
// of the whole page. The caller that evicts the page publishes the returned base into
// the page directory's diskOff (a flush of a still-resident page discards it, because a
// resident read uses the buffer, not the offset).
func (sh *shard) writePageRemainder(pid int64, page []byte) (int64, error) {
	base, err := sh.ensureExtent(pid)
	if err != nil {
		return 0, err
	}
	from := sh.pageFlushed[pid]
	to := sh.pageFill[pid]
	if from < to {
		if _, err := sh.df.f.WriteAt(page[from:to], base+int64(from)); err != nil {
			return 0, err
		}
		sh.pageFlushed[pid] = to
	}
	return base, nil
}

// flushDurable writes the unflushed record bytes of every page up to the tail into
// their extents in seal order and, when doSync is set, issues one device barrier and
// advances the frontier to the highest LSN now durable. The frontier advances only on
// the sync, never on a bare write, so a crash can never leave the frontier ahead of
// what reached the device (doc 04 section 4.2, the I4 monotonic watermark). It runs
// under the shard write lock.
func (sh *shard) flushDurable(doSync bool) {
	ps := sh.pages.Load()
	maxLSN := sh.frontier.Load()
	for pid := int64(0); pid <= sh.tailPage; pid++ {
		// A page's records are all durable once their bytes are synced, so the frontier
		// may reach this page's highest LSN. Account it whether or not the page is still
		// resident: an evicted page was already written to its extent.
		if sh.pageMaxLSN[pid] > maxLSN {
			maxLSN = sh.pageMaxLSN[pid]
		}
		page := ps.pages[pid]
		if page == nil || sh.pageFlushed[pid] >= sh.pageFill[pid] {
			continue
		}
		if _, err := sh.writePageRemainder(pid, page); err != nil {
			// A write failed: keep what is flushed and do not sync or advance, so the
			// frontier never claims bytes that did not reach the file.
			return
		}
	}
	if !doSync {
		return
	}
	if err := sh.df.syncData(); err != nil {
		return
	}
	sh.frontier.Store(maxLSN)
}

func (sh *shard) get(key []byte) ([]byte, bool, error) {
	if !sh.evicts {
		// Full-resident: pages are never freed and values never move, so the read
		// path is lock-free. Probe the index with atomic loads and slice straight out
		// of the resident page. The returned slice aliases the log page and the caller
		// must not mutate it. This is the benchmarked ceiling path; it takes no epoch
		// guard, because nothing is freed here so there is no reclamation to protect
		// (doc 07 section 5.1, 5.2). It is preserved bit-for-bit.
		thash := tableHash(key)
		loc, ok := sh.index.Load().lookup(thash, key)
		if !ok {
			return nil, false, nil
		}
		pages := sh.pages.Load().pages
		pid := loc.addr / int64(sh.pageSize)
		off := int(loc.addr % int64(sh.pageSize))
		page := pages[pid]
		return page[off : off+int(loc.vlen)], true, nil
	}
	// Eviction is possible, so a concurrent evictor can recycle a page out from under
	// us. The bare Get draws a fresh round-robin stripe; a hot read loop should hold a
	// Reader, whose cached stripe avoids the shared counter (doc 07 section 4.5, 4.6).
	return sh.getGuarded(key, sh.store.nextStripe.Add(1))
}

// getGuarded is the lock-free eviction-possible read (M6, doc 07 section 5.3). It
// replaces the shard read lock with an epoch guard: the reader enters its epoch before
// resolving the address, so a concurrent evictor cannot recycle the page buffer it is
// about to slice until it leaves (doc 07 section 2.4). A resident value is copied out
// under the epoch (the buffer is stable, not recyclable, while the guard is held); a
// spilled value's stable disk offset is resolved under the epoch, the epoch is released
// before the ReadAt (so a slow disk read never pins the safe epoch, doc 07 section 8.2),
// and the bytes are read back outside the guard. M6 frees no extent, so a spilled
// offset cannot be recycled; M8 adds the post-ReadAt index re-check when compaction can
// free the extent under a resolved offset (doc 07 section 10.1).
func (sh *shard) getGuarded(key []byte, stripe uint64) ([]byte, bool, error) {
	if sh.inPlace {
		// In-place profile: a same-size overwrite rewrites live value bytes under the write
		// lock, so a lock-free read of those bytes is a genuine data race, not just a formal
		// one (doc 04 section 7.3). Take the shard read lock for the read, the formally clean
		// alternative the spec sanctions: it excludes the in-place writer and the evictor
		// (both hold the write lock), so the bytes are stable for the copy and no page buffer
		// can be reclaimed underneath, which is why this path needs no epoch guard. A
		// lock-free in-place read (a record seqlock) is the M10 optimization; correctness
		// comes first here. Off this profile the read stays the M6 lock-free epoch path below.
		return sh.getLocked(key)
	}
	thash := tableHash(key)
	g := sh.store.slots.enter(&sh.store.globalEpoch, stripe)
	e := sh.index.Load().lookupEntry(thash, key)
	if e == nil {
		g.leave()
		return nil, false, nil
	}
	loc := e.loc
	ps := sh.pages.Load()
	pid := loc.addr / int64(sh.pageSize)
	off := int(loc.addr % int64(sh.pageSize))
	if page := ps.pages[pid]; page != nil {
		val := make([]byte, loc.vlen)
		copy(val, page[off:off+int(loc.vlen)])
		g.leave()
		return val, true, nil
	}
	// Spilled: the page is on disk at a stable offset (an extent's offset is fixed once
	// allocated and page ids are never reused), read from the same immutable pageSet
	// snapshot the page directory came from. Resolve under the epoch, then leave before
	// the syscall. In durable mode the file is the shared one; in Dir mode the scratch.
	dOff := ps.diskOff[pid]
	f := sh.file
	if sh.df != nil {
		f = sh.df.f
	}
	g.leave()
	if f == nil {
		return nil, false, errors.New("hashlog: address neither resident nor on disk")
	}
	val := make([]byte, loc.vlen)
	nr, err := f.ReadAt(val, dOff+int64(off))
	if err != nil && nr == 0 {
		return nil, false, err
	}
	return val[:nr], true, nil
}

// getLocked reads a key under the shard read lock, the in-place-profile read path. The
// read lock holds off the single writer (which mutates value bytes in place and rolls or
// evicts pages only under the write lock), so the resident copy is race free and any page
// it touches stays resident for the copy. The spilled branch reads from disk while still
// holding the read lock; the writer is briefly excluded, which is acceptable on this cold
// path and keeps the directory snapshot and the file offset consistent.
func (sh *shard) getLocked(key []byte) ([]byte, bool, error) {
	thash := tableHash(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	loc, ok := sh.index.Load().lookup(thash, key)
	if !ok {
		return nil, false, nil
	}
	ps := sh.pages.Load()
	// An oversize value's bytes span a cont chain, not one page, so assemble it from the
	// chain (M9, doc 03 section 7). The marker is one already-loaded bit, so an inline read
	// pays only this single branch before slicing as before.
	if loc.isOversize() {
		return sh.readOversizeLocked(ps, loc)
	}
	pid := loc.addr / int64(sh.pageSize)
	off := int(loc.addr % int64(sh.pageSize))
	if page := ps.pages[pid]; page != nil {
		val := make([]byte, loc.vlen)
		copy(val, page[off:off+int(loc.vlen)])
		return val, true, nil
	}
	dOff := ps.diskOff[pid]
	f := sh.file
	if sh.df != nil {
		f = sh.df.f
	}
	if f == nil {
		return nil, false, errors.New("hashlog: address neither resident nor on disk")
	}
	val := make([]byte, loc.vlen)
	nr, err := f.ReadAt(val, dOff+int64(off))
	if err != nil && nr == 0 {
		return nil, false, err
	}
	return val[:nr], true, nil
}

// uvarintLen returns the number of bytes binary.PutUvarint would write for x.
func uvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}

// hash64 is a fast FNV-1a over the key, used to pick a shard from the low bits of
// the hash.
func hash64(b []byte) uint64 {
	const (
		offset = 1469598103934665603
		prime  = 1099511628211
	)
	h := uint64(offset)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime
	}
	return h
}

// tableHash mixes the FNV hash with a splitmix64 finalizer before the index uses
// it. The shard mask already consumed the low bits of the FNV hash, so every key in
// a shard shares those bits; the finalizer spreads them back across the word so the
// open-addressing table inside the shard does not cluster.
func tableHash(b []byte) uint64 {
	h := hash64(b)
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h
}
