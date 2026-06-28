package hashlog

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"path/filepath"
	"testing"
)

// TestDurableRecordRoundTrip encodes a record and decodes it back, for a range of
// key and value sizes, asserting every field survives and n is the full record
// length.
func TestDurableRecordRoundTrip(t *testing.T) {
	cases := []struct {
		key, value []byte
		flags      byte
	}{
		{[]byte("k"), []byte("v"), 0},
		{[]byte(""), []byte(""), 0},
		{[]byte("a-longer-key"), bytes.Repeat([]byte("x"), 300), 0},
		{[]byte("tomb"), nil, flagTombstone},
		{[]byte("big"), bytes.Repeat([]byte("y"), 70000), 0}, // valLen needs a 3-byte uvarint
	}
	for i, c := range cases {
		buf := make([]byte, durableRecordLen(c.key, c.value))
		n := encodeDurableRecord(buf, uint64(i+1), c.key, c.value, c.flags)
		if n != len(buf) {
			t.Fatalf("case %d: encoded %d bytes, durableRecordLen said %d", i, n, len(buf))
		}
		lsn, flags, key, value, dn, err := decodeDurableRecord(buf)
		if err != nil {
			t.Fatalf("case %d: decode failed: %v", i, err)
		}
		if lsn != uint64(i+1) || flags != c.flags || dn != n {
			t.Fatalf("case %d: header mismatch lsn=%d flags=%d n=%d", i, lsn, flags, dn)
		}
		if !bytes.Equal(key, c.key) || !bytes.Equal(value, c.value) {
			t.Fatalf("case %d: body mismatch", i)
		}
	}
}

// TestDurableRecordValueOffset proves the preserved value-offset arithmetic (D3):
// durableValOff lands exactly on the value's first byte, so a resident GET slices the
// value with no decode.
func TestDurableRecordValueOffset(t *testing.T) {
	key := []byte("offset-key")
	value := []byte("the-value-bytes")
	buf := make([]byte, durableRecordLen(key, value))
	encodeDurableRecord(buf, 99, key, value, 0)
	off := durableValOff(key, value)
	if !bytes.Equal(buf[off:off+len(value)], value) {
		t.Fatalf("durableValOff %d does not point at the value", off)
	}
}

// TestDurableRecordCRCCatchesBitFlip flips one byte in every position of an encoded
// record and asserts the decode rejects each corruption.
func TestDurableRecordCRCCatchesBitFlip(t *testing.T) {
	key := []byte("crc-key")
	value := []byte("crc-value")
	buf := make([]byte, durableRecordLen(key, value))
	encodeDurableRecord(buf, 7, key, value, 0)
	for i := range buf {
		bad := append([]byte(nil), buf...)
		bad[i] ^= 0xFF
		if _, _, _, _, _, err := decodeDurableRecord(bad); err == nil {
			// A flip in the value or key still changes the CRC, so every position must
			// be caught. The one exception would be a flip the CRC cannot see, which the
			// format has none of: the CRC covers lsn through value.
			t.Fatalf("bit flip at byte %d was not caught", i)
		}
	}
}

// TestDurableRecordTombstone round-trips a tombstone: the flag is set, the value is
// empty, and decode reports both.
func TestDurableRecordTombstone(t *testing.T) {
	key := []byte("gone")
	buf := make([]byte, durableRecordLen(key, nil))
	encodeDurableRecord(buf, 3, key, nil, flagTombstone)
	_, flags, k, v, _, err := decodeDurableRecord(buf)
	if err != nil {
		t.Fatal(err)
	}
	if flags&flagTombstone == 0 {
		t.Fatal("tombstone flag not set")
	}
	if len(v) != 0 || !bytes.Equal(k, key) {
		t.Fatalf("tombstone body wrong: key=%q val=%q", k, v)
	}
}

// TestDurableRecordTruncatedRejected confirms decode of a record cut short at every
// length fails closed rather than reading past the buffer.
func TestDurableRecordTruncatedRejected(t *testing.T) {
	key := []byte("trunc-key")
	value := []byte("trunc-value-payload")
	buf := make([]byte, durableRecordLen(key, value))
	encodeDurableRecord(buf, 11, key, value, 0)
	for cut := 0; cut < len(buf); cut++ {
		if _, _, _, _, _, err := decodeDurableRecord(buf[:cut]); err == nil {
			t.Fatalf("truncation to %d bytes was accepted", cut)
		}
	}
}

// TestDurableLSNMonotonic drives a durable store and confirms the per-store LSN
// advances by exactly one per Set and is shared across shards.
func TestDurableLSNMonotonic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lsn.hlog")
	s, err := New(durableTunables(path))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const n = 5000
	for i := 0; i < n; i++ {
		if err := s.Set(key(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if got := s.df.lsn.Load(); got != n {
		t.Fatalf("after %d sets the LSN is %d, want %d", n, got, n)
	}
}

// TestDurableLSNPersistsAcrossCommit advances the LSN, commits, reopens, and confirms
// the high water survives via the superblock (the recovery of the log itself is M5;
// this is only the counter's persistence).
func TestDurableLSNPersistsAcrossCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lsnpersist.hlog")
	d, err := openDurableFile(path, 16, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 42; i++ {
		d.nextLSN()
	}
	if err := d.commit(); err != nil {
		t.Fatal(err)
	}
	d.Close()

	d2, err := openDurableFile(path, 16, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	if got := d2.lsn.Load(); got != 42 {
		t.Fatalf("reopened LSN high water %d, want 42", got)
	}
}

// TestDurableFullResidentRecordReads runs a durable store with no eviction (resident
// budget 0) so every read takes the lock-free resident path and slices a durable
// record straight at its value, proving durableValOff is correct end to end.
func TestDurableFullResidentRecordReads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "resident.hlog")
	tun := Tunables{Shards: 8, PageSize: 1 << 16, ExtentSize: 1 << 16, ResidentPagesPerShard: 0, Path: path}
	s, err := New(tun)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	m := newModel()
	rng := rand.New(rand.NewSource(5))
	for i := 0; i < 4000; i++ {
		k := key(rng.Intn(500))
		v := value(rng)
		if err := s.Set(k, v); err != nil {
			t.Fatal(err)
		}
		m.set(k, v)
	}
	if s.Spilled() != 0 {
		t.Fatalf("resident-budget-0 store spilled %d pages, want 0", s.Spilled())
	}
	for ks, wantV := range m.live {
		got, ok, err := s.Get([]byte(ks))
		if err != nil {
			t.Fatal(err)
		}
		if !ok || string(got) != string(wantV) {
			t.Fatalf("key %s mismatch on the resident durable path", ks)
		}
	}
}

// FuzzDecodeRecord asserts decodeDurableRecord never panics on arbitrary bytes and
// that anything it accepts re-encodes to a prefix that decodes back to the same
// fields (doc 08 section 4.4, fail-closed).
func FuzzDecodeRecord(f *testing.F) {
	seed := make([]byte, durableRecordLen([]byte("k"), []byte("v")))
	encodeDurableRecord(seed, 1, []byte("k"), []byte("v"), 0)
	f.Add(seed)
	f.Add([]byte{})
	f.Add(make([]byte, 32))
	f.Fuzz(func(t *testing.T, data []byte) {
		lsn, flags, key, value, n, err := decodeDurableRecord(data)
		if err != nil {
			return
		}
		if n < 0 || n > len(data) {
			t.Fatalf("accepted record reports length %d for a %d-byte buffer", n, len(data))
		}
		// A record decode accepted only because its CRC checked out, so re-encoding the
		// same fields must reproduce the same bytes and decode identically.
		re := make([]byte, durableRecordLen(key, value))
		encodeDurableRecord(re, lsn, key, value, flags)
		l2, fl2, k2, v2, n2, err := decodeDurableRecord(re)
		if err != nil || l2 != lsn || fl2 != flags || n2 != len(re) ||
			!bytes.Equal(k2, key) || !bytes.Equal(v2, value) {
			t.Fatalf("re-encode of an accepted record did not round-trip")
		}
	})
}

// sanity: a hand-built record with a deliberately wrong CRC is rejected, guarding
// against a decoder that forgets to verify.
func TestDurableRecordWrongCRCRejected(t *testing.T) {
	key := []byte("k")
	value := []byte("v")
	buf := make([]byte, durableRecordLen(key, value))
	encodeDurableRecord(buf, 1, key, value, 0)
	binary.LittleEndian.PutUint32(buf[len(buf)-recordCRCSize:], 0xDEADBEEF)
	if _, _, _, _, _, err := decodeDurableRecord(buf); err == nil {
		t.Fatal("a record with a wrong CRC was accepted")
	}
}
