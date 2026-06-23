// Package pager is the layer between the storage cores and the file (spec 03). It
// turns page numbers into pinned in-memory frames, owns the buffer pool and its
// replacement policy, allocates and frees pages against the freelist, and
// mediates every read and write to the main file through the vfs seam. Nothing
// above the pager touches bytes on disk; nothing in the pager knows what a page
// means (interior vs leaf vs segment block is the core's concern).
//
// Concurrency: the buffer pool is sharded by page number into independent
// shards, each with its own mutex, CLOCK hand, and resident-frame index (spec
// 03 §6, perf/05 F1). A Get or Unpin contends only with other access to the
// same shard, so reads of different pages run on different cores without
// serializing on one global lock. Page allocation (the freelist and the
// high-water mark) and the file header sit under a separate metaMu, which is a
// leaf lock: code paths acquire shards first (in ascending index order) and
// metaMu last, and nothing acquires a shard while holding metaMu, so the order
// is total and the pool cannot deadlock.
package pager

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/tamnd/kv/crypto"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/vfs"
)

// Intent declares whether a pinned frame will be read or written.
type Intent int

const (
	// Read pins a frame for reading only.
	Read Intent = iota
	// Write pins a frame the caller intends to mutate.
	Write
)

// Frame is the in-memory home of one page. Its data slice is a window into the
// pool's arena, stable for the pool's lifetime; frames are reused in place.
type Frame struct {
	pgno       uint32
	data       []byte
	pins       atomic.Int32
	dirty      bool
	ref        atomic.Bool // CLOCK reference bit
	slot       int         // index into the pool, -1 if not pooled
	decoded    atomic.Pointer[decodedNode]
	writePinned atomic.Bool // true between a write-intent Get and its Unpin
}

// decodedNode boxes a caller-supplied decoded view of the frame's page so the
// engine can cache the structure it unmarshals from the bytes (a B-tree node, say)
// and reuse it on the next read of the same resident page instead of decoding the
// bytes again. The pager treats the value as opaque; it only guarantees the box is
// cleared before the page's bytes can change or the frame is rebound to another
// page, so a non-nil Decoded always describes the bytes currently in the frame.
type decodedNode struct{ v any }

// PageNo returns the frame's page number.
func (f *Frame) PageNo() uint32 { return f.pgno }

// Data returns the frame's page bytes. The caller must hold a pin and, for
// writes, must have pinned with Write intent and unpin with dirty=true.
func (f *Frame) Data() []byte { return f.data }

// Decoded returns the cached decoded view the engine last attached to this frame
// with SetDecoded, or nil if none is cached or it was invalidated. It is safe to
// call under a read pin without the shard lock: the pager only ever stores nil into
// the slot while a write-intent pin or a rebind holds the page, which a concurrent
// reader is excluded from, so a non-nil result describes the frame's current bytes.
func (f *Frame) Decoded() any {
	if b := f.decoded.Load(); b != nil {
		return b.v
	}
	return nil
}

// SetDecoded caches v as the decoded view of the frame's current bytes. The engine
// calls it after unmarshaling a page it just read so the next read of the same
// resident page can skip the decode. v must be treated as immutable by every reader
// that retrieves it, since they all share the one instance; a writer that mutates the
// page decodes its own private copy and never calls this.
func (f *Frame) SetDecoded(v any) { f.decoded.Store(&decodedNode{v: v}) }

// clearDecoded drops any cached decoded view. The pager calls it at every point the
// frame's bytes may change (a write-intent pin) or the frame stops representing its
// page (rebind or free), which is what keeps a cached view honest.
func (f *Frame) clearDecoded() { f.decoded.Store(nil) }

// Options configure a pager at open.
type Options struct {
	// PageSize is used only when creating a fresh database; an existing file's
	// page size is read from its header.
	PageSize int
	// CacheFrames is the buffer-pool capacity in frames. Zero selects the default.
	CacheFrames int
	// Engine and Checksum are stamped into a fresh file's header.
	Engine   format.EngineKind
	Checksum format.ChecksumAlgo
	// Flags are header flag bits for a fresh file (e.g. format.FlagWAL).
	Flags byte
	// Encryption, when non-nil, transparently encrypts every data page the pager
	// writes and decrypts every data page it reads (spec 14). Page 1 (the header and
	// the cleartext encryption descriptor) and the freelist trunk pages stay in the
	// clear. On Create the pager widens the reserved trailer to fit the AEAD envelope,
	// sets the encryption flag, and writes Descriptor onto page 1. On Open the caller
	// is expected to have built the scheme from the on-disk descriptor and verified the
	// key first; the pager trusts the scheme it is handed.
	Encryption *crypto.Scheme
	// Descriptor is the encoded cleartext encryption descriptor (crypto.Descriptor.Encode)
	// written onto page 1 after the header at Create when Encryption is set. Ignored on Open.
	Descriptor []byte
}

const defaultCacheFrames = 2000

// Buffer-pool sharding parameters. maxShards caps the shard count so the per-shard
// bookkeeping stays small; minFramesPerShard is the floor on frames each shard owns,
// chosen comfortably above the most pages any one operation pins simultaneously in a
// single shard (a tree-height descent plus a cursor stack), so a shard never starves an
// admission while the rest of the pool sits idle. A pool below the floor collapses to a
// single shard and behaves exactly like the old global pool, which keeps the tiny-pool
// regime (the streaming-flush tests) working.
const (
	maxShards         = 64
	minFramesPerShard = 16
)

// shard is one independent slice of the buffer pool: the frames whose page numbers hash
// to it, a CLOCK hand over just those frames, and the index that finds a resident page
// among them. Every field is guarded by mu. A page number maps to exactly one shard for
// the pool's lifetime, so a frame in this shard only ever caches pages that belong here.
type shard struct {
	mu      sync.RWMutex
	index   map[uint32]*Frame // resident frames by page number, this shard only
	frames  []*Frame          // frames owned by this shard, pinned or free
	hand    int               // CLOCK hand into frames
	scratch []byte            // pageSize crypto staging buffer, lazily allocated
}

// Pager owns the buffer pool and the main file.
type Pager struct {
	fs   vfs.FS
	file vfs.File
	path string

	pageSize int

	// metaMu guards the page-allocation state (free, dbSize) and the mutable header
	// fields written at checkpoint and truncate. It is the leaf of the lock order: a
	// caller may hold one or more shard locks and then take metaMu, never the reverse.
	metaMu sync.Mutex
	header *format.Header

	// ckptGate keeps a checkpoint from running while a page producer that is not under the
	// host's single write lock is mid-write. Almost every page write the pager sees is
	// serialized behind that lock, which a checkpoint also takes, so the dirty-flag and
	// frame-buffer writers never overlap (see Unpin). The LSM core's background flusher is
	// the one exception: it turns a sealed memtable into segment pages off the foreground
	// path, so it holds this gate in shared mode (BeginExternalWrite) while it writes, and
	// Checkpoint holds it exclusively. Shared among producers, exclusive against the
	// checkpoint, so concurrent producers still proceed but a checkpoint waits for the gap
	// between them.
	ckptGate sync.RWMutex

	// crypto, when set, encrypts data pages on the way to disk and decrypts them on the
	// way back (spec 14). It is nil for an unencrypted file, the default. It is an atomic
	// pointer because the sharded read and write paths load it without a global lock while
	// Rekey may swap it live (spec 14 §5); each shard keeps its own scratch buffer so the
	// encrypted paths never share staging memory across cores.
	crypto atomic.Pointer[crypto.Scheme]

	shards    []*shard
	shardMask uint32 // len(shards)-1; len(shards) is always a power of two
	arena     []byte

	dbSize uint32   // page count (high-water mark); pages are 1-based; under metaMu
	free   []uint32 // in-memory freelist, persisted to trunk pages at checkpoint; under metaMu

	// pageReads and cacheHits count the buffer pool's traffic for the read-amplification
	// and cache-hit-ratio observability the spec asks for (spec 19, spec 21 §1). pageReads
	// is the number of physical page reads issued against the main file to satisfy a Get
	// miss; cacheHits is the number of Gets served from a resident frame. They are atomics
	// so a Stats reader can sample them without taking a pager lock off the hot path.
	pageReads atomic.Uint64
	cacheHits atomic.Uint64
}

// shardFor returns the shard that owns pgno. The mapping is a fixed mask over the page
// number, so a page lives in the same shard for the pool's lifetime.
func (p *Pager) shardFor(pgno uint32) *shard { return p.shards[pgno&p.shardMask] }

// cryptoScheme loads the live encryption scheme, or nil for an unencrypted file.
func (p *Pager) cryptoScheme() *crypto.Scheme { return p.crypto.Load() }

// IOStats is the buffer pool's cumulative traffic since open. PageReads is physical reads
// of a page from the main file (cache misses that hit disk); CacheHits is Gets served from
// a resident frame. Their ratio is the cache hit rate, and PageReads over the number of
// logical reads a workload issued is its read amplification (spec 21 §1).
type IOStats struct {
	PageReads uint64
	CacheHits uint64
}

// IOStats samples the pager's cumulative I/O counters. It is lock-free and cheap, safe to
// poll from a Stats path while the pager serves reads.
func (p *Pager) IOStats() IOStats {
	return IOStats{PageReads: p.pageReads.Load(), CacheHits: p.cacheHits.Load()}
}

// Create initializes a fresh database file and returns an open pager.
func Create(fs vfs.FS, path string, opts Options) (*Pager, error) {
	ps := opts.PageSize
	if ps == 0 {
		ps = format.DefaultPageSize
	}
	if !format.ValidPageSize(ps) {
		return nil, format.ErrBadPageSize
	}
	f, err := fs.Open(path, vfs.OpenReadWrite|vfs.OpenCreate)
	if err != nil {
		return nil, err
	}
	engine := opts.Engine
	if engine == 0 {
		engine = format.EngineBTree
	}
	checksum := opts.Checksum
	h := format.NewHeader(ps, engine, opts.Flags, checksum)
	p := newPager(fs, f, path, ps, opts.CacheFrames)
	p.header = h
	p.dbSize = 1
	if opts.Encryption != nil {
		// Widen the reserved trailer to cover the per-page AEAD envelope on top of the
		// checksum, so every data page's plaintext fits in UsablePageSize and its
		// ciphertext, tag, and nonce land in the reserved tail (spec 14 §3). Record the
		// encryption flag so a reader knows to expect a descriptor and a key.
		h.ReservedPerPage += byte(crypto.Overhead)
		h.Flags |= format.FlagEncryption
		p.crypto.Store(opts.Encryption)
		if format.HeaderSize+len(opts.Descriptor) > ps-checksum.ChecksumSize() {
			f.Close()
			return nil, fmt.Errorf("pager: encryption descriptor does not fit on page 1")
		}
	}
	// Write page 1 (the header page) so the file is non-empty and valid, stamping its
	// per-page checksum into the reserved trailer so a fresh file verifies on reopen.
	// Page 1 itself is never encrypted: the header and the descriptor must stay readable
	// without the key.
	page1 := make([]byte, ps)
	h.Encode(page1)
	if opts.Encryption != nil {
		copy(page1[format.HeaderSize:], opts.Descriptor)
	}
	format.StampPageChecksum(page1, h.Checksum)
	if _, err := f.WriteAt(page1, 0); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Sync(vfs.SyncFull); err != nil {
		f.Close()
		return nil, err
	}
	return p, nil
}

// Open opens an existing database file and returns a pager. It reads and
// validates the header from page 1 and loads the freelist.
func Open(fs vfs.FS, path string, opts Options) (*Pager, error) {
	f, err := fs.Open(path, vfs.OpenReadWrite)
	if err != nil {
		return nil, err
	}
	// Read enough of page 1 to decode the header.
	hbuf := make([]byte, format.HeaderSize)
	if _, err := f.ReadAt(hbuf, 0); err != nil {
		f.Close()
		return nil, fmt.Errorf("pager: read header: %w", err)
	}
	h, err := format.DecodeHeader(hbuf)
	if err != nil {
		f.Close()
		return nil, err
	}
	size, err := f.Size()
	if err != nil {
		f.Close()
		return nil, err
	}
	ps := h.PageSize
	// Verify page 1's checksum now that the page size is known: a torn or bit-rotted
	// header page must be caught at open, not silently trusted. A short read (a file
	// smaller than one page) leaves the page zero-padded, which the all-zero guard in
	// verifyPageChecksum skips, so this only rejects a materialized, corrupt page 1.
	if h.Checksum != format.ChecksumNone {
		page1 := make([]byte, ps)
		if _, err := f.ReadAt(page1, 0); err == nil {
			if err := verifyPageChecksum(page1, h.Checksum); err != nil {
				f.Close()
				return nil, fmt.Errorf("pager: header page: %w", err)
			}
		}
	}
	p := newPager(fs, f, path, ps, opts.CacheFrames)
	p.header = h
	if h.Flags&format.FlagEncryption != 0 {
		// The file is encrypted: the caller must supply the scheme it built from the
		// on-disk descriptor and verified against the key. Without it the data pages are
		// unreadable, so refuse rather than hand back ciphertext.
		if opts.Encryption == nil {
			f.Close()
			return nil, ErrKeyRequired
		}
		p.crypto.Store(opts.Encryption)
	}
	p.dbSize = uint32(size / int64(ps))
	if p.dbSize == 0 {
		p.dbSize = 1
	}
	if err := p.loadFreelist(); err != nil {
		f.Close()
		return nil, err
	}
	return p, nil
}

// ErrKeyRequired is returned by Open when the file's header marks it encrypted but the
// caller supplied no encryption scheme: the data pages cannot be read without the key.
var ErrKeyRequired = fmt.Errorf("pager: file is encrypted, an encryption key is required")

// ErrNotEncrypted is returned by ReadDescriptor when the file carries no encryption flag:
// there is no descriptor to read, and a supplied key does not belong to this file.
var ErrNotEncrypted = fmt.Errorf("pager: file is not encrypted")

// ReadDescriptor reads the cleartext encryption descriptor from an existing file's first
// page without opening a full pager (spec 14 §3, §4). The db layer calls it before Open
// so it can build the encryption scheme from the on-disk cipher, epoch, and KDF
// parameters and verify the key, then hand the verified scheme back through Options. It
// returns ErrNotEncrypted when the file has no encryption flag.
func ReadDescriptor(fs vfs.FS, path string) (*crypto.Descriptor, error) {
	f, err := fs.Open(path, vfs.OpenReadWrite)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	hbuf := make([]byte, format.HeaderSize)
	if _, err := f.ReadAt(hbuf, 0); err != nil {
		return nil, fmt.Errorf("pager: read header: %w", err)
	}
	h, err := format.DecodeHeader(hbuf)
	if err != nil {
		return nil, err
	}
	if h.Flags&format.FlagEncryption == 0 {
		return nil, ErrNotEncrypted
	}
	page1 := make([]byte, h.PageSize)
	if _, err := f.ReadAt(page1, 0); err != nil {
		return nil, fmt.Errorf("pager: read page 1: %w", err)
	}
	return crypto.DecodeDescriptor(page1[format.HeaderSize:])
}

func newPager(fs vfs.FS, f vfs.File, path string, pageSize, cacheFrames int) *Pager {
	if cacheFrames <= 0 {
		cacheFrames = defaultCacheFrames
	}
	nShards := shardCount(cacheFrames)
	p := &Pager{
		fs:        fs,
		file:      f,
		path:      path,
		pageSize:  pageSize,
		arena:     make([]byte, cacheFrames*pageSize),
		shards:    make([]*shard, nShards),
		shardMask: uint32(nShards - 1),
	}
	// Carve frames out of the arena up front; data slices are stable for life. Frames are
	// handed to shards in contiguous runs, with the first (cacheFrames mod nShards) shards
	// getting one extra, so every shard owns at least cacheFrames/nShards frames. A frame's
	// data window is fixed by its global slot regardless of which shard owns it.
	base, extra := cacheFrames/nShards, cacheFrames%nShards
	slot := 0
	for s := 0; s < nShards; s++ {
		n := base
		if s < extra {
			n++
		}
		sh := &shard{
			index:  make(map[uint32]*Frame, n),
			frames: make([]*Frame, 0, n),
		}
		for i := 0; i < n; i++ {
			fr := &Frame{slot: slot, data: p.arena[slot*pageSize : (slot+1)*pageSize : (slot+1)*pageSize]}
			sh.frames = append(sh.frames, fr)
			slot++
		}
		p.shards[s] = sh
	}
	return p
}

// shardCount picks the number of pool shards: the largest power of two no greater than
// maxShards that still leaves every shard at least minFramesPerShard frames, and at least
// one. A pool below the floor uses a single shard, restoring the old global-pool behavior
// (which the tiny-pool streaming-flush regime relies on).
func shardCount(cacheFrames int) int {
	n := 1
	for n*2 <= maxShards && cacheFrames/(n*2) >= minFramesPerShard {
		n *= 2
	}
	return n
}

// PageSize reports the full on-disk page size in bytes.
func (p *Pager) PageSize() int { return p.pageSize }

// UsablePageSize reports the page bytes a storage core may use, the full page
// minus the reserved trailer the per-page checksum occupies (spec 02 §3.2). A core
// must keep every node body within this bound so the pager can stamp the checksum
// into the trailer without clobbering data.
func (p *Pager) UsablePageSize() int { return p.header.UsablePageSize() }

// ChecksumAlgo reports the per-page checksum algorithm the file was created with,
// or format.ChecksumNone when the file carries no page checksums. The verifier's
// checksum sweep reads it to decide whether a torn-write/bit-rot class even exists
// for this file (spec 23 §3).
func (p *Pager) ChecksumAlgo() format.ChecksumAlgo { return p.header.Checksum }

// ReadRaw returns a copy of the page's on-disk bytes without verifying its
// checksum, so the integrity checker can inspect a possibly-corrupt page and report
// it rather than have the read abort (spec 23 §3). Normal page access goes through
// Get, which does verify. pgno must be within the page count.
func (p *Pager) ReadRaw(pgno uint32) ([]byte, error) {
	if pgno == 0 || pgno > p.DBSize() {
		return nil, fmt.Errorf("pager: page %d out of range", pgno)
	}
	buf := make([]byte, p.pageSize)
	off := int64(pgno-1) * int64(p.pageSize)
	if _, err := p.file.ReadAt(buf, off); err != nil {
		return nil, err
	}
	return buf, nil
}

// Header returns the live header. Callers must not retain it across Checkpoint.
func (p *Pager) Header() *format.Header { return p.header }

// DBSize reports the current page count (high-water mark).
func (p *Pager) DBSize() uint32 {
	p.metaMu.Lock()
	defer p.metaMu.Unlock()
	return p.dbSize
}

// FreeCount reports how many pages are currently on the in-memory freelist,
// available for reallocation before the file grows (spec 09 §2, §4).
func (p *Pager) FreeCount() int {
	p.metaMu.Lock()
	defer p.metaMu.Unlock()
	return len(p.free)
}

// FreePages returns a copy of the in-memory freelist, the page numbers available for
// reallocation. The structural verifier (spec 23 §3) uses it to prove no page is both
// free and reachable from the tree, and that the freelist, reachable, and metadata
// pages account for the whole file.
func (p *Pager) FreePages() []uint32 {
	p.metaMu.Lock()
	defer p.metaMu.Unlock()
	return append([]uint32(nil), p.free...)
}
