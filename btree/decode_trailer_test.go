package btree

import (
	"fmt"
	"testing"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// TestInteriorFillingUsableSurvivesChecksumStamp guards the page-decode contract against a
// data-corruption bug: an interior node whose pivots fill the usable area exactly used to
// read back as ErrCorrupt after a checkpoint.
//
// The node decoders consume the engine's usable-sized node image, the same size the
// marshalers produce. The pager reserves the page's last ChecksumSize bytes for a per-page
// checksum it stamps during checkpoint (the trailer, spec 02 §3.2). When the read path fed
// the decoder the full physical page instead of the usable image, unmarshalInterior read the
// Bε message-count varint, which it expects to find as zero padding right after the pivots,
// straight off the stamped checksum bytes when the pivots ended exactly at the usable
// boundary. The non-zero checksum decoded as a bogus message count and the page was rejected.
// A leaf packed to the same boundary was unaffected (its decoder never reads trailing
// padding), so this is interior-only.
//
// The test builds an interior whose marshaled body length is exactly usable, stores it, runs
// a checkpoint to stamp the trailer, and decodes it again. It fails with ErrCorrupt on the
// pre-fix read path and passes once the decoder is bounded to the usable area.
func TestInteriorFillingUsableSurvivesChecksumStamp(t *testing.T) {
	const (
		pageSize = 512
		sepLen   = 3 // separator length; uvarint(3) is one byte, so each pivot cell is 4+1+3 = 8 bytes
	)
	fs := vfs.NewMem()
	p, err := pager.Create(fs, "test.kv", pager.Options{
		PageSize:    pageSize,
		CacheFrames: 64,
		Engine:      format.EngineBTree,
		Checksum:    format.ChecksumCRC32C, // reserves a 4-byte trailer, so usable = pageSize - 4
	})
	if err != nil {
		t.Fatalf("create pager: %v", err)
	}
	bt := New(p)
	if err := bt.Open(&engine.Env{}); err != nil {
		t.Fatalf("open btree: %v", err)
	}
	usable := p.UsablePageSize()

	// Build an interior with N pivots so the marshaled body lands exactly on usable:
	//   body = nodeHeaderSize + N*(4 + 1 + sepLen)
	cell := 4 + 1 + sepLen
	if (usable-nodeHeaderSize)%cell != 0 {
		t.Fatalf("pick a sepLen that divides usable-header evenly: usable=%d header=%d cell=%d", usable, nodeHeaderSize, cell)
	}
	n := (usable - nodeHeaderSize) / cell
	in := &interior{}
	for i := 0; i < n; i++ {
		in.seps = append(in.seps, []byte(fmt.Sprintf("%03d", i)))
		in.children = append(in.children, format.PageNo(i+2))
	}
	in.children = append(in.children, format.PageNo(n+2)) // the rightmost child

	if got := len(marshalInterior(in)); got != usable {
		t.Fatalf("crafted interior body = %d bytes, want exactly usable = %d", got, usable)
	}

	pgno, err := bt.storeInteriorNew(in)
	if err != nil {
		t.Fatalf("store interior: %v", err)
	}

	// Checkpoint stamps the per-page checksum into the trailer the pivots end against.
	if err := p.Checkpoint(0, 0); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	// Decode the page again. loadInterior decodes a fresh copy from the frame bytes, so it
	// exercises the same read path a reopen would. Before the fix this returned ErrCorrupt.
	got, err := bt.loadInterior(pgno)
	if err != nil {
		t.Fatalf("loadInterior after checkpoint: %v (the checksum trailer was misread as a Be message count)", err)
	}
	if len(got.seps) != n || len(got.children) != n+1 {
		t.Fatalf("decoded interior = %d seps / %d children, want %d / %d", len(got.seps), len(got.children), n, n+1)
	}
}
