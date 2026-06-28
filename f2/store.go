// Package f2 is a resident-index key/value engine in the FASTER v2 shape, the
// memory-lean sibling of package hashlog. It keeps hashlog's two strengths, a
// lock-free read path (an atomic-load probe straight to the value, no mutex on a
// hot key) and a per-shard hybrid log, and pays down the one cost that caps how
// far hashlog scales: the size of the resident index.
//
// hashlog stores a full index entry per live key (the key bytes, a 64-bit hash,
// and a value location), so a billion 16-byte keys cost tens of gigabytes of RAM
// in the index alone before a single value is held. f2 follows FASTER and stores
// only an eight-byte atomic word per slot: a 15-bit tag and a 48-bit logical
// offset into the shard's log. The key itself is not resident; a lookup probes
// by tag, reads the candidate record from the log, and verifies the full key
// there. The record is self-describing (it already carries its key for recovery
// and compaction), so the verify costs nothing the read was not already paying.
// At a 0.7 load factor that is about 11 bytes of index per key regardless of key
// length, roughly a sixth of hashlog's cost on realistic keys, which is the
// difference between a billion keys fitting in ~11 GiB and not fitting at all.
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
	"errors"
	"os"
	"sync/atomic"
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
	// DurabilityNormal fsyncs on seal boundaries and at checkpoints, not on every
	// SET. The loss window on a crash is the writes since the last seal sync.
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
	// contention.
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

	// Path selects the single-file mode: one file that is both the
	// larger-than-memory backing and, at Normal or Full, the crash-recoverable
	// store. Empty keeps the memory-only mode, the benchmarked ceiling.
	Path string

	// Durability is the durability dial. It is meaningful only when a Path is set;
	// selecting Normal or Full without a Path is an error because there is nowhere
	// to sync. The zero value is None.
	Durability Durability

	// CheckpointBytes bounds the durable record bytes appended before a checkpoint
	// is due, capping recovery replay. Zero defaults to 256 MiB in durable mode.
	CheckpointBytes int64
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

var (
	errBadTunables  = errors.New("f2: invalid tunables")
	errValueTooBig  = errors.New("f2: record does not fit in a page")
	errPageMismatch = errors.New("f2: file page size or shard count differs from tunables")
)

// Stats reports the engine's space accounting, the data a memory and scalability
// study needs. Counts are summed across shards.
type Stats struct {
	Keys        int64 // live keys
	IndexSlots  int64 // total index slots allocated across shards
	IndexBytes  int64 // resident index cost: IndexSlots * 8
	LogBytes    int64 // bytes appended to the logs (live plus stranded)
	DeadBytes   int64 // bytes stranded by overwrites and deletes
	ResidentLog int64 // log bytes held in RAM (all of LogBytes in memory-only mode)
	EvictedLog  int64 // log bytes dropped from RAM, present only in the file
	ResidentMem int64 // total resident footprint estimate: IndexBytes + ResidentLog
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
	shards []*shard
	df     *durableFile // the one shared file in single-file mode, nil in memory-only
	mask   uint64
	t      Tunables

	ckptBytes int64        // checkpoint interval, 0 disables auto-checkpoint
	sinceCkpt atomic.Int64 // durable bytes appended since the last checkpoint
}

// New opens a Store with the given tunables. With no Path it is the
// full-resident, memory-only core. With a Path it is the single-file mode: one
// file that is both the larger-than-memory backing (bounded by
// ResidentPagesPerShard) and, under a Normal or Full dial, the crash-recoverable
// store. Opening an existing file replays it: the compact index is rebuilt from
// the file's records, so the store comes back with every key it acknowledged.
func New(t Tunables) (*Store, error) {
	if t.Shards <= 0 || t.Shards&(t.Shards-1) != 0 {
		return nil, errBadTunables
	}
	if t.PageSize <= 0 {
		return nil, errBadTunables
	}
	if t.Path == "" {
		// Memory-only: nothing to sync and nowhere to evict to.
		if t.Durability != DurabilityNone || t.ResidentPagesPerShard != 0 {
			return nil, errBadTunables
		}
		return newMemory(t), nil
	}
	if t.ResidentPagesPerShard != 0 && t.ResidentPagesPerShard < 1 {
		return nil, errBadTunables
	}
	return newDurable(t)
}

// newMemory builds the memory-only store: no file, every page resident.
func newMemory(t Tunables) *Store {
	s := &Store{
		shards: make([]*shard, t.Shards),
		mask:   uint64(t.Shards - 1),
		t:      t,
	}
	for i := range s.shards {
		s.shards[i] = newShard(t.PageSize, nil, i, 0)
	}
	return s
}

// newDurable opens or creates the single file, builds the shards over it, and
// replays an existing file into the index. A fresh file gets an initial
// superblock so a later open always finds one.
func newDurable(t Tunables) (*Store, error) {
	f, err := os.OpenFile(t.Path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	ckpt := t.CheckpointBytes
	if ckpt == 0 {
		ckpt = defaultCheckpointBytes
	}
	s := &Store{
		shards:    make([]*shard, t.Shards),
		mask:      uint64(t.Shards - 1),
		t:         t,
		ckptBytes: ckpt,
	}
	df := &durableFile{f: f, pageSize: int64(t.PageSize), shards: t.Shards, dial: t.Durability}
	s.df = df

	sb := readSuperblock(f)
	if sb.valid && (sb.pageSize != df.pageSize || sb.shards != df.shards) {
		_ = f.Close()
		return nil, errPageMismatch
	}
	df.seq = sb.seq
	for i := range s.shards {
		s.shards[i] = newShard(t.PageSize, df, i, t.ResidentPagesPerShard)
	}
	if sb.valid {
		if err := s.recover(); err != nil {
			_ = f.Close()
			return nil, err
		}
	} else if err := df.writeSuperblock(); err != nil { // stamp a fresh file
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) shardFor(h uint64) *shard { return s.shards[(h>>shardShift)&s.mask] }

// Get returns the value for key and whether it was found. In the full-resident
// profile the returned slice aliases the log page and the caller must not mutate
// it; it stays valid as long as the store is open because resident pages are
// never freed or rewritten in this profile.
func (s *Store) Get(key []byte) ([]byte, bool, error) {
	h := hash64(key)
	return s.shardFor(h).get(h, key)
}

// Set stores value under key, appending a new record and repointing the index
// slot. value is copied into the log, so the caller may reuse its buffer. In
// durable mode the record carries a CRC and, past the checkpoint interval, the
// write triggers a checkpoint so recovery replay stays bounded.
func (s *Store) Set(key, value []byte) error {
	var n int
	if s.df != nil {
		n = durableRecordLen(key, value)
		if n > s.t.PageSize-blockHeaderSize {
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
		return s.Checkpoint()
	}
	return nil
}

// Delete removes key. It is a no-op if the key is absent.
func (s *Store) Delete(key []byte) error {
	h := hash64(key)
	s.shardFor(h).del(h, key)
	return nil
}

// Checkpoint is a durability barrier. In the memory-only core there is nothing to
// flush, so it is a no-op. In single-file mode it flushes every shard's tail page
// to the file and advances the superblock to the current allocator high-water, so
// recovery after this point replays only what follows. Under a non-None dial the
// flush and the superblock are fsynced.
func (s *Store) Checkpoint() error {
	if s.df == nil {
		return nil
	}
	for _, sh := range s.shards {
		sh.mu.Lock()
		sh.log.flushTail()
		sh.mu.Unlock()
	}
	if s.df.dial != DurabilityNone {
		if err := s.df.f.Sync(); err != nil {
			return err
		}
	}
	return s.df.writeSuperblock()
}

// Close releases the store. In single-file mode it checkpoints first so a clean
// shutdown loses nothing even under the None dial, then closes the file. The file
// is the durable store and is never removed. The memory-only core holds no OS
// resources and only drops its shards.
func (s *Store) Close() error {
	if s.df != nil {
		// Flush and stamp the superblock even under None, so a clean close is always
		// fully recoverable; only a crash exposes the dial's loss window.
		for _, sh := range s.shards {
			sh.mu.Lock()
			sh.log.flushTail()
			sh.mu.Unlock()
		}
		_ = s.df.f.Sync()
		if err := s.df.writeSuperblock(); err != nil {
			return err
		}
		err := s.df.f.Close()
		s.shards = nil
		s.df = nil
		return err
	}
	s.shards = nil
	return nil
}

// Stats sums the per-shard accounting into one snapshot. It takes each shard's
// read lock briefly, so it is consistent per shard but not a global instant.
func (s *Store) Stats() Stats {
	var st Stats
	for _, sh := range s.shards {
		sh.mu.RLock()
		t := sh.index.Load()
		st.Keys += int64(t.live)
		st.IndexSlots += int64(len(t.slots))
		st.LogBytes += int64(sh.logBytes)
		st.DeadBytes += int64(sh.deadBytes)
		evicted := int64(sh.log.evict) * sh.log.pageSize
		resident := int64(sh.logBytes) - evicted
		if resident < 0 {
			resident = 0
		}
		st.EvictedLog += evicted
		st.ResidentLog += resident
		sh.mu.RUnlock()
	}
	st.IndexBytes = st.IndexSlots * 8
	st.ResidentMem = st.IndexBytes + st.ResidentLog
	return st
}
