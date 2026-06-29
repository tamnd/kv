package format

import (
	"bytes"
	"math"
	"testing"
)

func TestVarintRoundTrip(t *testing.T) {
	cases := []uint64{
		0, 1, 0x7f, 0x80, 0x3fff, 0x4000, 1 << 21, 1 << 28,
		1 << 35, 1 << 49, 1 << 56, 1<<56 + 1, math.MaxUint64 - 1, math.MaxUint64,
		0x0102030405060708,
	}
	var buf [MaxVarintLen]byte
	for _, x := range cases {
		n := PutUvarint(buf[:], x)
		if n != UvarintLen(x) {
			t.Fatalf("x=%d: PutUvarint wrote %d, UvarintLen says %d", x, n, UvarintLen(x))
		}
		got, k := Uvarint(buf[:n])
		if k != n || got != x {
			t.Fatalf("x=%d: round trip got %d (n=%d) want %d (n=%d)", x, got, k, x, n)
		}
	}
}

func TestUvarintShortBuffer(t *testing.T) {
	var buf [MaxVarintLen]byte
	n := PutUvarint(buf[:], math.MaxUint64)
	if _, k := Uvarint(buf[:n-1]); k != 0 {
		t.Fatalf("expected incomplete decode to return 0, got n=%d", k)
	}
}

func TestVarintOrderingMonotone(t *testing.T) {
	// Multi-byte varints are not byte-sortable in general, but the round trip and
	// length monotonicity must hold: a larger value never encodes shorter.
	prev := 0
	for _, x := range []uint64{0, 0x7f, 0x3fff, 0x1fffff, math.MaxUint64} {
		if l := UvarintLen(x); l < prev {
			t.Fatalf("UvarintLen(%d)=%d shorter than previous %d", x, l, prev)
		} else {
			prev = l
		}
	}
}

func TestSignedVarint(t *testing.T) {
	for _, n := range []int64{0, 1, -1, 63, -64, 1 << 40, -(1 << 40), math.MaxInt64, math.MinInt64} {
		var buf [MaxVarintLen]byte
		k := PutVarint(buf[:], n)
		got, m := Varint(buf[:k])
		if m != k || got != n {
			t.Fatalf("signed round trip n=%d got %d", n, got)
		}
	}
}

func TestInternalKeyOrdering(t *testing.T) {
	// Same user key, different versions: newer must sort first (smaller bytes).
	a := EncodeInternalKey([]byte("k"), 5, KindSet)
	b := EncodeInternalKey([]byte("k"), 9, KindSet)
	if CompareInternal(b, a) >= 0 {
		t.Fatalf("version 9 should sort before version 5")
	}
	// Different user keys sort by user key regardless of version.
	x := EncodeInternalKey([]byte("a"), 1, KindSet)
	y := EncodeInternalKey([]byte("b"), 100, KindSet)
	if CompareInternal(x, y) >= 0 {
		t.Fatalf("user key a should sort before b")
	}
}

func TestInternalKeyParse(t *testing.T) {
	ik := EncodeInternalKey([]byte("hello"), 1234, KindMerge)
	uk, v, kind, ok := ParseInternalKey(ik)
	if !ok || !bytes.Equal(uk, []byte("hello")) || v != 1234 || kind != KindMerge {
		t.Fatalf("parse mismatch: uk=%q v=%d kind=%v ok=%v", uk, v, kind, ok)
	}
	if !bytes.Equal(UserKey(ik), []byte("hello")) {
		t.Fatalf("UserKey mismatch")
	}
	if Version(ik) != 1234 || KindOf(ik) != KindMerge {
		t.Fatalf("Version/KindOf mismatch")
	}
}

func TestInternalKeyMaxVersionSeek(t *testing.T) {
	// A seek key built with MaxVersion must sort at or before any real version of
	// the same user key, so SeekGE lands on the newest version.
	seek := EncodeInternalKey([]byte("k"), MaxVersion, KindSet)
	real := EncodeInternalKey([]byte("k"), 1000, KindSet)
	if CompareInternal(seek, real) > 0 {
		t.Fatalf("MaxVersion seek key must not sort after a real version")
	}
}

func TestPrefixSuccessor(t *testing.T) {
	if got := PrefixSuccessor([]byte("ab")); !bytes.Equal(got, []byte("ac")) {
		t.Fatalf("successor of ab = %q, want ac", got)
	}
	if got := PrefixSuccessor([]byte{0x01, 0xff}); !bytes.Equal(got, []byte{0x02}) {
		t.Fatalf("successor with trailing 0xff = %q", got)
	}
	if got := PrefixSuccessor([]byte{0xff, 0xff}); got != nil {
		t.Fatalf("successor of all-0xff must be nil, got %q", got)
	}
}

func TestHeaderRoundTrip(t *testing.T) {
	page := make([]byte, DefaultPageSize)
	h := NewHeader(DefaultPageSize, EngineF2, FlagWAL, ChecksumCRC32C)
	h.UserVersion = 7
	h.ApplicationID = 0xdeadbeef
	h.EngineRoot = 2
	h.Encode(page)

	got, err := DecodeHeader(page)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.PageSize != DefaultPageSize || got.Engine != EngineF2 ||
		got.Flags != FlagWAL || got.Checksum != ChecksumCRC32C ||
		got.UserVersion != 7 || got.ApplicationID != 0xdeadbeef || got.EngineRoot != 2 {
		t.Fatalf("header field mismatch: %+v", got)
	}
	if !got.SizeValid() {
		t.Fatalf("fresh header should have valid size fields")
	}
}

func TestHeaderPageSize64K(t *testing.T) {
	page := make([]byte, MaxPageSize)
	h := NewHeader(MaxPageSize, EngineF2, 0, ChecksumNone)
	h.Encode(page)
	got, err := DecodeHeader(page)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.PageSize != MaxPageSize {
		t.Fatalf("64K page size did not round trip: %d", got.PageSize)
	}
}

func TestHeaderBadMagic(t *testing.T) {
	page := make([]byte, DefaultPageSize)
	if _, err := DecodeHeader(page); err != ErrBadMagic {
		t.Fatalf("expected ErrBadMagic, got %v", err)
	}
}

func TestChecksumDetectsCorruption(t *testing.T) {
	for _, algo := range []ChecksumAlgo{ChecksumCRC32C, ChecksumXXH64} {
		data := []byte("the quick brown fox jumps over the lazy dog")
		sum := algo.Sum(data)
		data[10] ^= 0x40
		if algo.Sum(data) == sum {
			t.Fatalf("algo %d failed to detect a single-bit flip", algo)
		}
	}
}

func TestFreelistTrunkRoundTrip(t *testing.T) {
	page := make([]byte, DefaultPageSize)
	in := TrunkPage{Next: 42, Leafs: []PageNo{3, 4, 5, 100, 101}}
	EncodeTrunk(page, in)
	out := DecodeTrunk(page, DefaultPageSize)
	if out.Next != in.Next || len(out.Leafs) != len(in.Leafs) {
		t.Fatalf("trunk header mismatch: %+v", out)
	}
	for i := range in.Leafs {
		if out.Leafs[i] != in.Leafs[i] {
			t.Fatalf("leaf %d mismatch: %d != %d", i, out.Leafs[i], in.Leafs[i])
		}
	}
	if DecodeCommonHeader(page).Type != PageFreelistTrunk {
		t.Fatalf("trunk page type tag wrong")
	}
}

func TestCommonHeaderRoundTrip(t *testing.T) {
	page := make([]byte, DefaultPageSize)
	in := CommonHeader{Type: PageBTreeLeaf, Flags: 0x5a, CellCount: 300, Overflow: 99}
	in.Encode(page)
	out := DecodeCommonHeader(page)
	if out != in {
		t.Fatalf("common header mismatch: %+v != %+v", out, in)
	}
}
