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
// This file is the memory-only, full-resident core (the in-memory ceiling). The
// larger-than-memory spill path and the durable single-file layout build on top
// of it in later files, behind the same Tunables hashlog uses, so an adapter can
// open either engine through the same knobs.
package f2

import "errors"

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
	// the oldest spills to disk. Zero means unbounded (nothing ever spills), the
	// full-resident, fastest, RAM-bound mode. Honored by the spill path; the
	// memory-only core treats every value as resident.
	ResidentPagesPerShard int

	// Dir is where each shard writes its spill file in the larger-than-memory,
	// non-durable mode. Empty keeps the store memory-only.
	Dir string

	// Path selects the durable single-file mode: one file that survives a crash
	// with no lost acknowledged write. Empty keeps the memory-only mode, the
	// benchmarked ceiling. Mutually exclusive with Dir.
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
	return Tunables{Shards: 256, PageSize: 1 << 20, ResidentPagesPerShard: 0, Dir: ""}
}

var (
	errBadTunables = errors.New("f2: invalid tunables")
	errValueTooBig = errors.New("f2: record does not fit in a page")
	// errDurableUnbuilt guards the modes layered on top of this core until their
	// files land, so opening one fails loudly rather than silently running
	// memory-only.
	errDurableUnbuilt = errors.New("f2: durable single-file mode not built yet")
)

// Stats reports the engine's space accounting, the data a memory and scalability
// study needs. Counts are summed across shards.
type Stats struct {
	Keys       int64 // live keys
	IndexSlots int64 // total index slots allocated across shards
	IndexBytes int64 // resident index cost: IndexSlots * 8
	LogBytes   int64 // bytes appended to the logs (live plus stranded)
	DeadBytes  int64 // bytes stranded by overwrites and deletes
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
	mask   uint64
	t      Tunables
}

// New opens a Store with the given tunables. The memory-only core requires only a
// power-of-two shard count and a positive page size; the spill and durable modes
// validate their own extra knobs when those files build them.
func New(t Tunables) (*Store, error) {
	if t.Shards <= 0 || t.Shards&(t.Shards-1) != 0 {
		return nil, errBadTunables
	}
	if t.PageSize <= 0 {
		return nil, errBadTunables
	}
	if t.Path != "" {
		return nil, errDurableUnbuilt
	}
	if t.Durability != DurabilityNone && t.Path == "" {
		return nil, errBadTunables // nowhere to sync
	}
	s := &Store{
		shards: make([]*shard, t.Shards),
		mask:   uint64(t.Shards - 1),
		t:      t,
	}
	for i := range s.shards {
		s.shards[i] = newShard(t.PageSize)
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
// slot. value is copied into the log, so the caller may reuse its buffer.
func (s *Store) Set(key, value []byte) error {
	if recordLen(key, value) > s.t.PageSize {
		return errValueTooBig
	}
	h := hash64(key)
	return s.shardFor(h).set(h, key, value)
}

// Delete removes key. It is a no-op if the key is absent.
func (s *Store) Delete(key []byte) error {
	h := hash64(key)
	s.shardFor(h).del(h, key)
	return nil
}

// Checkpoint is a durability barrier. In the memory-only core there is nothing to
// flush, so it is a no-op; the durable mode overrides the work behind it.
func (s *Store) Checkpoint() error { return nil }

// Close releases the store. The memory-only core holds no OS resources, so it
// only drops its shards.
func (s *Store) Close() error {
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
		sh.mu.RUnlock()
	}
	st.IndexBytes = st.IndexSlots * 8
	return st
}
