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
//     a GET takes the shard read lock so a concurrent flush cannot pull a page out
//     from under it, and a spilled value is read back with one ReadAt.
//   - SET appends a new record to the tail page under the shard write lock and
//     publishes the index slot with one atomic store. When the tail page fills it is
//     sealed and a fresh page begins; when the resident budget is exceeded the oldest
//     page spills.
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
	}
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
		s.shards[i] = sh
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
	i := thash & t.mask
	for {
		e := t.slots[i].Load()
		if e == nil {
			return valLoc{}, false
		}
		if e != tombstone && e.thash == thash && bytes.Equal(e.key, key) {
			return e.loc, true
		}
		i = (i + 1) & t.mask
	}
}

// pageSet is the log's page directory, published behind an atomic pointer so the
// lock-free read path can index it without holding the shard lock. A structural
// change (rolling a new page, or nil-ing a spilled one) builds a fresh pageSet and
// stores it; writing record bytes into an existing page does not, and is made
// visible to readers by the atomic store of the index entry that points at it.
type pageSet struct {
	pages [][]byte
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

	// pages is the log's page directory behind an atomic pointer; pageID indexes it
	// and a nil entry means the page was spilled (its file offset is in diskOff).
	pages   atomic.Pointer[pageSet]
	diskOff []int64 // pageID -> byte offset in file, valid where pages[pid] is nil

	tailPage int64 // pageID currently being appended to
	tailPos  int   // append offset within the tail page

	// residentOrder lists resident pageIDs oldest-first, so eviction pops the front.
	residentOrder []int64

	spilledPages int

	pageSize    int
	residentCap int // ResidentPagesPerShard; 0 means unbounded
	evicts      bool
	file        *os.File
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
}

func newShard(t Tunables, df *durableFile, shardID int) (*shard, error) {
	sh := &shard{
		pageSize:    t.PageSize,
		residentCap: t.ResidentPagesPerShard,
		scratch:     make([]byte, 0, 256),
		df:          df,
		shardID:     shardID,
	}
	sh.index.Store(newIdxTable(1024))
	// Page 0 starts resident and empty.
	sh.pages.Store(&pageSet{pages: [][]byte{make([]byte, t.PageSize)}})
	sh.diskOff = append(sh.diskOff, 0)
	sh.pageExtent = append(sh.pageExtent, -1)
	sh.residentOrder = append(sh.residentOrder, 0)
	if df == nil && t.Dir != "" {
		f, err := os.CreateTemp(t.Dir, "hashlog-shard-*.log")
		if err != nil {
			return nil, err
		}
		sh.file = f
	}
	sh.evicts = sh.residentCap > 0 && (sh.file != nil || df != nil)
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
	if rl > sh.pageSize {
		return errors.New("hashlog: record larger than page size")
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()

	ps := sh.pages.Load()
	// Roll to a fresh page when the record does not fit in the tail page. The sealed
	// page becomes eligible for flushing once it leaves the resident cap. Publish the
	// new directory before writing the record into it so a reader that later sees the
	// record's index entry also sees the page it lives on.
	if sh.tailPos+rl > sh.pageSize {
		sh.tailPage++
		sh.tailPos = 0
		np := make([][]byte, len(ps.pages)+1)
		copy(np, ps.pages)
		np[sh.tailPage] = make([]byte, sh.pageSize)
		ps = &pageSet{pages: np}
		sh.pages.Store(ps)
		sh.diskOff = append(sh.diskOff, 0)
		sh.pageExtent = append(sh.pageExtent, -1)
		sh.residentOrder = append(sh.residentOrder, sh.tailPage)
		sh.evictIfNeeded()
		ps = sh.pages.Load()
	}
	page := ps.pages[sh.tailPage]
	recStart := sh.tailPage*int64(sh.pageSize) + int64(sh.tailPos)
	// Encode the record and compute the value's offset from the record start. The
	// value sits after the header and the key, so point the index straight at it and
	// reads skip the record decode. The index publish (an atomic store) is the release
	// that makes the record bytes above visible to a lock-free reader's acquiring load.
	var valOff int
	if durable {
		n := encodeDurableRecord(page[sh.tailPos:], sh.df.nextLSN(), key, value, 0)
		sh.tailPos += n
		valOff = durableValOff(key, value)
	} else {
		n := encodeRecord(page[sh.tailPos:], key, value)
		sh.tailPos += n
		valOff = uvarintLen(uint64(len(key))) + len(key) + uvarintLen(uint64(len(value)))
	}
	sh.indexPut(tableHash(key), key, valLoc{addr: recStart + int64(valOff), vlen: uint32(len(value))})
	return nil
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

func (sh *shard) delete(key []byte) error {
	thash := tableHash(key)
	sh.mu.Lock()
	t := sh.index.Load()
	i := thash & t.mask
	for {
		e := t.slots[i].Load()
		if e == nil {
			break
		}
		if e != tombstone && e.thash == thash && bytes.Equal(e.key, key) {
			t.slots[i].Store(tombstone)
			sh.idxLive--
			break
		}
		i = (i + 1) & t.mask
	}
	sh.mu.Unlock()
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
	for len(sh.residentOrder) > sh.residentCap {
		pid := sh.residentOrder[0]
		ps := sh.pages.Load()
		page := ps.pages[pid]
		if page == nil {
			sh.residentOrder = sh.residentOrder[1:]
			continue
		}

		var off int64
		if sh.df != nil {
			// Durable mode: allocate an extent for this page, ensure the file is large
			// enough, and write the page into it. pageExtent records the home so a
			// spilled read finds the bytes; diskOff caches the extent's byte offset so
			// the read path is the same single ReadAt as the scratch path.
			id, _ := sh.df.alloc.alloc()
			if err := sh.df.growExtent(id); err != nil {
				return
			}
			off = sh.df.extentOffset(id)
			if _, err := sh.df.f.WriteAt(page, off); err != nil {
				sh.df.alloc.freeExtent(id)
				return
			}
			sh.pageExtent[pid] = id
		} else {
			off = sh.fileEnd
			if _, err := sh.file.WriteAt(page, off); err != nil {
				// On a write error keep the page resident rather than lose data; stop
				// evicting this round and leave the order untouched.
				return
			}
			sh.fileEnd += int64(len(page))
		}

		sh.diskOff[pid] = off
		np := make([][]byte, len(ps.pages))
		copy(np, ps.pages)
		np[pid] = nil
		sh.pages.Store(&pageSet{pages: np})
		sh.residentOrder = sh.residentOrder[1:]
		sh.spilledPages++
	}
}

func (sh *shard) get(key []byte) ([]byte, bool, error) {
	thash := tableHash(key)
	if !sh.evicts {
		// Full-resident: pages are never freed and values never move, so the read
		// path is lock-free. Probe the index with atomic loads and slice straight out
		// of the resident page. The returned slice aliases the log page and the caller
		// must not mutate it.
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

	// Eviction is possible, so a concurrent flush can pull a page out from under us.
	// Take the read lock to coordinate with it, copy a resident value out, or read a
	// spilled one back from disk.
	sh.mu.RLock()
	loc, ok := sh.index.Load().lookup(thash, key)
	if !ok {
		sh.mu.RUnlock()
		return nil, false, nil
	}
	pages := sh.pages.Load().pages
	pid := loc.addr / int64(sh.pageSize)
	off := int(loc.addr % int64(sh.pageSize))
	if page := pages[pid]; page != nil {
		val := make([]byte, loc.vlen)
		copy(val, page[off:off+int(loc.vlen)])
		sh.mu.RUnlock()
		return val, true, nil
	}
	// Spilled: the page is on disk. Its byte offset is stable (an extent's offset is
	// fixed once allocated and pageIDs are never reused), so read exactly the value
	// bytes back outside the lock. In durable mode the file is the shared one; in Dir
	// mode it is the shard's scratch file.
	dOff := sh.diskOff[pid]
	f := sh.file
	if sh.df != nil {
		f = sh.df.f
	}
	sh.mu.RUnlock()
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
