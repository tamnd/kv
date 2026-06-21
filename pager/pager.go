// Package pager is the layer between the storage cores and the file (spec 03). It
// turns page numbers into pinned in-memory frames, owns the buffer pool and its
// replacement policy, allocates and frees pages against the freelist, and
// mediates every read and write to the main file through the vfs seam. Nothing
// above the pager touches bytes on disk; nothing in the pager knows what a page
// means (interior vs leaf vs segment block is the core's concern).
//
// Concurrency in this milestone is single-mutex: correctness first. The
// lock-free sharded read path and per-frame hybrid latch the spec describes
// (spec 03 §6) are a later optimization that does not change this contract.
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
	pgno  uint32
	data  []byte
	pins  atomic.Int32
	dirty bool
	ref   bool // CLOCK reference bit
	slot  int  // index into the pool, -1 if not pooled
}

// PageNo returns the frame's page number.
func (f *Frame) PageNo() uint32 { return f.pgno }

// Data returns the frame's page bytes. The caller must hold a pin and, for
// writes, must have pinned with Write intent and unpin with dirty=true.
func (f *Frame) Data() []byte { return f.data }

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

// Pager owns the buffer pool and the main file.
type Pager struct {
	fs   vfs.FS
	file vfs.File
	path string

	mu       sync.Mutex
	pageSize int
	header   *format.Header

	// crypto, when set, encrypts data pages on the way to disk and decrypts them on
	// the way back (spec 14). It is nil for an unencrypted file, the default. encScratch
	// is a single pageSize staging buffer the encrypted read and write paths reuse; both
	// run under mu, so one buffer is enough.
	crypto     *crypto.Scheme
	encScratch []byte

	index map[uint32]*Frame // resident frames by page number
	pool  []*Frame          // all frames, pooled or free
	arena []byte
	hand  int // CLOCK hand

	dbSize uint32   // page count (high-water mark); pages are 1-based
	free   []uint32 // in-memory freelist, persisted to trunk pages at checkpoint

	// pageReads and cacheHits count the buffer pool's traffic for the read-amplification
	// and cache-hit-ratio observability the spec asks for (spec 19, spec 21 §1). pageReads
	// is the number of physical page reads issued against the main file to satisfy a Get
	// miss; cacheHits is the number of Gets served from a resident frame. They are atomics
	// so a Stats reader can sample them without taking the pager mutex off the hot path.
	pageReads atomic.Uint64
	cacheHits atomic.Uint64
}

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
		p.crypto = opts.Encryption
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
		p.crypto = opts.Encryption
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
	p := &Pager{
		fs:       fs,
		file:     f,
		path:     path,
		pageSize: pageSize,
		index:    make(map[uint32]*Frame, cacheFrames),
		pool:     make([]*Frame, 0, cacheFrames),
		arena:    make([]byte, cacheFrames*pageSize),
	}
	// Carve frames out of the arena up front; data slices are stable for life.
	for i := 0; i < cacheFrames; i++ {
		fr := &Frame{slot: i, data: p.arena[i*pageSize : (i+1)*pageSize : (i+1)*pageSize]}
		p.pool = append(p.pool, fr)
	}
	return p
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
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dbSize
}

// FreeCount reports how many pages are currently on the in-memory freelist,
// available for reallocation before the file grows (spec 09 §2, §4).
func (p *Pager) FreeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.free)
}

// FreePages returns a copy of the in-memory freelist, the page numbers available for
// reallocation. The structural verifier (spec 23 §3) uses it to prove no page is both
// free and reachable from the tree, and that the freelist, reachable, and metadata
// pages account for the whole file.
func (p *Pager) FreePages() []uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]uint32(nil), p.free...)
}
