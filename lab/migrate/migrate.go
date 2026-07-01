// Package migrate is a frozen experiment: how deep should the seal-to-migrate pipeline be?
// In the tiered store a writer fills an in-memory hot segment and, when it is full, seals it
// and hands it to a background migrator that drains it to the cold tier. The drain is the slow
// step, it touches the file, so the question is what the writer does while a drain is in flight.
//
// The pipeline depth is how many sealed segments may be outstanding, counting the one the
// migrator is draining right now. At depth one the writer that seals a segment must wait until
// the migrator has finished the previous drain before it can seal again, so the fill and the
// drain run one after the other and the write path pays fill plus drain per segment. At depth
// two or more the writer seals and fills the next segment while the migrator drains the last, so
// the fill overlaps the drain and the write path pays only the slower of the two per segment.
// Depth past two buys burst headroom, room to seal a few segments ahead when the drain stutters,
// not more steady-state overlap, since a single migrator can only drain one segment at a time.
//
// The two candidates the board contrasts are depth one, the serial design, and depth two and up,
// the pipelined design. The cost of depth is bounded memory: at most depth segments are resident,
// so depth trades RAM for overlap. The drain here is a fixed CPU cost over the segment's bytes, a
// deterministic stand-in for the cold-tier write, sized so a drain costs about what filling a
// segment costs, which is the regime where the overlap win is real and measurable. The engine
// carries depth four (maxSealed): two for the overlap, a little more for burst headroom, which is
// the decision impl note 184 settles.
package migrate

import (
	"hash/crc32"
	"sync"
)

const (
	// recordSize is the bytes one Append copies into the hot segment, the per-record fill cost.
	recordSize = 64
	// segRecords is how many records fill one segment before it seals. Kept small so a short
	// benchmark seals many times and the per-seal overlap, the thing under test, dominates the
	// average rather than being amortized away by a giant segment.
	segRecords = 512
	// segBytes is one segment's buffer size, what the migrator drains.
	segBytes = recordSize * segRecords
)

// drainSegment is the cost of migrating one sealed segment to the cold tier, modeled as a CRC
// over the segment's bytes: a deterministic stand-in for the file write the real migrator does,
// proportional to the segment size just as a real cold write is. Returning the checksum keeps the
// compiler from eliding the work.
func drainSegment(buf []byte) uint32 {
	return crc32.ChecksumIEEE(buf)
}

// Pipeline is the seal-to-migrate path with a bounded number of sealed segments outstanding,
// counting the one being drained. A depth of one serializes fill and drain; a larger depth lets
// the writer fill ahead while one migrator drains in seal order, the same single-consumer shape
// the engine uses. The outstanding count is released only after a drain completes, so depth here
// means what maxSealed means in the engine: segments not yet drained, in flight included.
type Pipeline struct {
	work  chan []byte   // sealed segment buffers handed to the migrator
	slots chan struct{} // one token per allowed outstanding segment; the depth bound
	wg    sync.WaitGroup
	sink  uint32 // accumulates drain checksums so the work is not optimized out

	seg  []byte      // the current hot segment buffer being filled
	off  int         // fill offset into seg
	free chan []byte // recycled segment buffers, so steady state does not allocate
}

// NewPipeline starts one migrator draining sealed segments with at most depth outstanding. Depth
// one is the serial baseline where the writer waits for each drain; depth four is what the engine
// carries. The migrator releases a slot only after it has finished draining a segment, so the
// writer blocks on seal exactly when depth segments are still undrained.
func NewPipeline(depth int) *Pipeline {
	if depth < 1 {
		depth = 1
	}
	p := &Pipeline{
		work:  make(chan []byte, depth),
		slots: make(chan struct{}, depth),
		free:  make(chan []byte, depth+1),
	}
	for range depth {
		p.slots <- struct{}{} // depth available tokens: depth segments may be outstanding at once
	}
	p.seg = make([]byte, segBytes)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		var acc uint32
		for buf := range p.work {
			acc += drainSegment(buf)
			// Recycle the buffer, then release a token only now, after the drain is done, so the
			// depth bound counts the in-flight segment. This release-after-drain is the off-by-one
			// that separates a true serial depth-one from a channel that frees its slot the instant
			// the migrator dequeues.
			select {
			case p.free <- buf:
			default:
			}
			p.slots <- struct{}{}
		}
		p.sink = acc
	}()
	return p
}

// Append copies one record into the hot segment and seals it when full. The seal waits for a free
// slot, which blocks the writer when depth segments are still undrained, then hands the buffer to
// the migrator and takes a fresh buffer to fill. The record bytes are a fixed pattern; only the
// copy cost matters to the fill-versus-drain balance under test.
func (p *Pipeline) Append() {
	copy(p.seg[p.off:p.off+recordSize], record)
	p.off += recordSize
	if p.off >= segBytes {
		<-p.slots // acquire a token: blocks when depth segments are already outstanding (drain backpressure)
		p.work <- p.seg
		p.seg = p.takeBuf()
		p.off = 0
	}
}

// takeBuf reuses a drained buffer when one is ready and allocates otherwise, so steady state does
// not allocate a fresh segment per seal.
func (p *Pipeline) takeBuf() []byte {
	select {
	case b := <-p.free:
		return b
	default:
		return make([]byte, segBytes)
	}
}

// Close drains the pipeline and stops the migrator, returning the accumulated checksum so a caller
// can keep the drain work live.
func (p *Pipeline) Close() uint32 {
	close(p.work)
	p.wg.Wait()
	return p.sink
}

var record = func() []byte {
	b := make([]byte, recordSize)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}()
