package db

import (
	"sync"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
)

// scanBatchInitial and scanBatchMax bound the zero-copy fill. The first fill is small so a short
// bounded scan over-resolves almost nothing, and the size doubles each refill up to the cap so a
// long dense scan quickly reaches full amortization. The cap also bounds the engine-read-lock
// window to this many folds and the buffer to this many KV view headers, reused across refills.
const (
	scanBatchInitial = 8
	scanBatchMax     = 256
)

// scanBufPool recycles the per-cursor view buffer across scans. Every scan op is a fresh cursor, and
// a short bounded scan (the kvbench readseq shape reads ~50 keys then closes) used to grow its buffer
// geometrically from scanBatchInitial, allocating a new []engine.KV at 8, 16, 32, ... within that one
// op and throwing it away at Close. That per-op allocation, multiplied over every scan, fed the GC
// machinery that a read-window profile showed dominating the op. The pool hands each cursor one
// max-sized buffer and takes it back at Close, so a scan does no buffer allocation in steady state.
// It holds *[]engine.KV (not []engine.KV) so Get/Put move the slice header by pointer with no boxing
// allocation of their own. The buffer carries only view headers aliasing engine storage; Close clears
// it before returning it so a recycled buffer never pins a decoded leaf alive past the cursor.
var scanBufPool = sync.Pool{New: func() any { s := make([]engine.KV, scanBatchMax); return &s }}

// ScanCursor is a forward-only, zero-copy scan over a read snapshot. It is the fast path for a
// dense ascending scan: where the general Iterator copies every key and value onto the heap and
// accumulates the whole walked range in a buffer (so it can serve SeekLT/Prev and reverse), a
// ScanCursor only moves forward and never keeps an entry past the next advance, so it can hand back
// keys and values aliased straight to the engine's immutable internal storage with no per-entry
// allocation. A full B-tree scan through the Iterator spends nearly half its time in allocation and
// GC for those copies; the ScanCursor removes that cost entirely on the engines that can serve a
// zero-copy batch, and falls back to the Iterator unchanged on the ones that cannot.
//
// The returned Key and Value bytes are READ-ONLY and valid only until the next Next or Close: they
// alias the engine's shared decoded nodes, which a writer replaces wholesale rather than editing,
// so a consumer that reads each entry before advancing (the shape every scan already has) sees
// stable bytes, and one that needs to keep an entry must copy it. This mirrors GetZeroCopy on the
// point path. The cursor is single-consumer and must be Closed.
type ScanCursor struct {
	txn      *Txn
	rd       engine.Reader
	bc       engine.BatchCursor
	keysOnly bool
	lower    []byte

	buf     []engine.KV  // reused view buffer (cap scanBatchMax) borrowed from scanBufPool; entries alias engine storage, valid until the next refill
	bufp    *[]engine.KV // the pool handle for buf, returned at Close; nil on the Iterator fallback path
	batchN  int          // current fill size, grown geometrically toward scanBatchMax
	n       int          // number of valid entries in buf
	pos     int          // index of the current entry in buf
	drained bool
	err     error

	// iter is the fallback when the engine cannot serve a zero-copy batch (the LSM core, a
	// non-streamable tree) or the scan is not a plain read-only forward scan (a write transaction's
	// buffered-write overlay, a reverse scan): the cursor delegates to a general Iterator, copying
	// as that path always did. nil in the zero-copy fast path.
	iter    *Iterator
	started bool
}

// NewScanCursor returns a forward-only zero-copy cursor over the transaction's snapshot. Bounds and
// prefix come from opts exactly as NewIterator's do; Reverse forces the Iterator fallback since the
// zero-copy path is forward-only. The caller drives it with Next then Key/Value and must Close it.
func (t *Txn) NewScanCursor(opts engine.IterOptions) (*ScanCursor, error) {
	if t.done {
		return nil, ErrTxnDone
	}
	t.db.counters.scan.Add(1)
	lower, upper := opts.Lower, opts.Upper
	if len(opts.Prefix) > 0 {
		lower = opts.Prefix
		upper = format.PrefixSuccessor(opts.Prefix)
	}
	// Same serializable predicate tracking the Iterator does: the scan depends on every key in the
	// interval, tracked for commit-time validation (spec 10 §4).
	t.trackRange(lower, upper)

	// The zero-copy fast path needs a read-only forward scan over an engine that can serve a
	// zero-copy batch. A write transaction must overlay its buffered writes and a reverse scan must
	// walk backward, so both take the Iterator fallback; so does an engine without a BatchCursor.
	if len(t.ops) == 0 && !opts.Reverse {
		sh := t.db.rl.RLock()
		rd, err := t.db.eng.NewReader(engine.Snapshot{Version: t.readVersion, Clock: t.db.now})
		if err != nil {
			t.db.rl.RUnlock(sh)
			return nil, err
		}
		if fc, ok := rd.(engine.ForwardCursorer); ok {
			cur, err := fc.NewForwardCursor(lower, upper)
			if err != nil {
				rd.Close()
				t.db.rl.RUnlock(sh)
				return nil, err
			}
			if bc, ok := cur.(engine.BatchCursor); ok {
				t.db.rl.RUnlock(sh)
				bufp := scanBufPool.Get().(*[]engine.KV)
				return &ScanCursor{
					txn:      t,
					rd:       rd,
					bc:       bc,
					keysOnly: opts.KeysOnly,
					lower:    lower,
					buf:      *bufp,
					bufp:     bufp,
					pos:      -1,
				}, nil
			}
		}
		// Reader has no zero-copy batch: drop it and fall back to the Iterator, which builds its own.
		rd.Close()
		t.db.rl.RUnlock(sh)
	}

	it, err := t.NewIterator(opts)
	if err != nil {
		return nil, err
	}
	return &ScanCursor{txn: t, keysOnly: opts.KeysOnly, lower: lower, iter: it, pos: -1}, nil
}

// Next advances to the next entry and reports whether the cursor is positioned on one. The first
// Next seeks to the lower bound; each later one steps forward.
func (s *ScanCursor) Next() bool {
	if s.iter != nil {
		if !s.started {
			s.started = true
			return s.iter.SeekGE(s.lower)
		}
		return s.iter.Next()
	}
	if s.err != nil {
		return false
	}
	s.pos++
	if s.pos < s.n {
		return true
	}
	if s.drained {
		return false
	}
	s.refill()
	return s.n > 0
}

// refill pulls the next zero-copy batch under one engine read lock, recycling the view buffer. The
// previous batch's views are invalid after this returns; the consumer has already advanced past
// them (refill only runs once Next has consumed every buffered entry).
func (s *ScanCursor) refill() {
	if s.batchN == 0 {
		s.batchN = scanBatchInitial
	}
	// buf is a pooled buffer of cap scanBatchMax, and batchN never exceeds that cap, so the slice
	// reuses the same backing array on every refill with no allocation.
	dst := s.buf[:s.batchN]
	sh := s.txn.db.rl.RLock()
	n, err := s.bc.NextBatch(dst, s.keysOnly)
	s.txn.db.rl.RUnlock(sh)
	s.n = n
	s.pos = 0
	if err != nil {
		s.err = err
		s.drained = true
		return
	}
	if n < len(dst) {
		s.drained = true // short fill: range exhausted
		return
	}
	if s.batchN < scanBatchMax {
		s.batchN *= 2
		if s.batchN > scanBatchMax {
			s.batchN = scanBatchMax
		}
	}
}

// Key returns the current entry's user key, a read-only view valid until the next Next or Close.
func (s *ScanCursor) Key() []byte {
	if s.iter != nil {
		return s.iter.Key()
	}
	if s.pos < 0 || s.pos >= s.n {
		return nil
	}
	return s.buf[s.pos].Key
}

// Value returns the current entry's value, a read-only view valid until the next Next or Close, or
// nil for a key-only cursor.
func (s *ScanCursor) Value() []byte {
	if s.iter != nil {
		v, err := s.iter.Value()
		if err != nil {
			s.err = err
			return nil
		}
		return v
	}
	if s.pos < 0 || s.pos >= s.n {
		return nil
	}
	return s.buf[s.pos].Value
}

// Error reports the first error that ended iteration, if any.
func (s *ScanCursor) Error() error {
	if s.iter != nil {
		if s.err != nil {
			return s.err
		}
		return s.iter.Error()
	}
	return s.err
}

// Close releases the cursor's reader and returns its pooled view buffer. After Close the view bytes
// from Key/Value are invalid, which is why the buffer can be recycled here: the consumer has
// finished with every entry it referenced. The buffer is cleared before it goes back so a recycled
// slot never keeps a decoded leaf reachable past the cursor that aliased it.
func (s *ScanCursor) Close() error {
	if s.bufp != nil {
		clear(*s.bufp)
		scanBufPool.Put(s.bufp)
		s.bufp = nil
		s.buf = nil
	}
	if s.iter != nil {
		return s.iter.Close()
	}
	if s.rd != nil {
		err := s.rd.Close()
		s.rd = nil
		return err
	}
	return nil
}
