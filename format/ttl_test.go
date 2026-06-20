package format

import (
	"bytes"
	"testing"
)

// TestTTLValueRoundTrip checks the expiry frame survives encode/decode for a range of
// values, including an empty one.
func TestTTLValueRoundTrip(t *testing.T) {
	cases := []struct {
		expiry uint64
		value  []byte
	}{
		{0, nil},
		{1, []byte("x")},
		{1 << 40, []byte("hello world")},
		{^uint64(0), bytes.Repeat([]byte("a"), 300)},
	}
	for _, c := range cases {
		raw := EncodeTTLValue(c.expiry, c.value)
		gotExp, gotVal := DecodeTTLValue(raw)
		if gotExp != c.expiry {
			t.Fatalf("expiry round-trip = %d, want %d", gotExp, c.expiry)
		}
		if !bytes.Equal(gotVal, c.value) {
			t.Fatalf("value round-trip = %q, want %q", gotVal, c.value)
		}
	}
}

// TestDecodeTTLValueShort checks a frame too short to hold the 8-byte prefix decodes as
// a non-expiring empty value rather than panicking.
func TestDecodeTTLValueShort(t *testing.T) {
	exp, val := DecodeTTLValue([]byte{1, 2, 3})
	if exp != 0 || val != nil {
		t.Fatalf("short decode = (%d,%q), want (0,nil)", exp, val)
	}
}

// TestOpFromPartsExpiry checks the TTL expansion: a live TTL set folds as a set, an
// expired one as a synthetic delete at the same version, and now == 0 disables expiry.
func TestOpFromPartsExpiry(t *testing.T) {
	framed := EncodeTTLValue(100, []byte("v"))

	// Before the deadline: a plain set carrying the unframed value.
	op, ok := OpFromParts(7, KindSetWithTTL, framed, 50)
	if !ok || op.Kind != KindSet || string(op.Value) != "v" || op.Version != 7 {
		t.Fatalf("live ttl op = %+v ok=%v, want set v at v7", op, ok)
	}

	// At the deadline: a synthetic delete at the same version.
	op, ok = OpFromParts(7, KindSetWithTTL, framed, 100)
	if !ok || op.Kind != KindDelete || op.Version != 7 {
		t.Fatalf("expired ttl op = %+v ok=%v, want delete at v7", op, ok)
	}

	// now == 0 disables expiry: the value folds live regardless of the deadline.
	op, ok = OpFromParts(7, KindSetWithTTL, framed, 0)
	if !ok || op.Kind != KindSet || string(op.Value) != "v" {
		t.Fatalf("now=0 ttl op = %+v ok=%v, want live set v", op, ok)
	}

	// A zero expiry never expires, even at a large now.
	never := EncodeTTLValue(0, []byte("v"))
	op, ok = OpFromParts(7, KindSetWithTTL, never, ^uint64(0))
	if !ok || op.Kind != KindSet || string(op.Value) != "v" {
		t.Fatalf("zero-expiry ttl op = %+v ok=%v, want live set v", op, ok)
	}

	// Range markers still resolve out of band.
	if _, ok := OpFromParts(7, KindRangeBegin, []byte("hi"), 50); ok {
		t.Fatalf("range-begin returned an op, want ok=false")
	}
}
