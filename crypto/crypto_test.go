package crypto

import (
	"bytes"
	"errors"
	"testing"
)

// testKey is a fixed 32-byte master key for the round-trip tests.
var testKey = bytes.Repeat([]byte{0xA5}, keySize)

// TestSealOpenRoundTrip seals a page and opens it back, confirming the envelope carries
// the plaintext intact and costs exactly Overhead bytes over it.
func TestSealOpenRoundTrip(t *testing.T) {
	s, err := NewScheme(testKey, CipherAESGCM, 0)
	if err != nil {
		t.Fatalf("new scheme: %v", err)
	}
	plain := []byte("the quick brown fox jumps over the lazy dog")
	env, err := s.SealPage(nil, plain, 7)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if got, want := len(env), len(plain)+Overhead; got != want {
		t.Fatalf("envelope length = %d, want %d", got, want)
	}
	if bytes.Contains(env, plain) {
		t.Errorf("plaintext leaked into the envelope")
	}
	out, err := s.OpenPage(nil, env, 7)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(out, plain) {
		t.Errorf("round trip = %q, want %q", out, plain)
	}
}

// TestSealUsesFreshNonce confirms two seals of the same plaintext under the same key and
// page produce different envelopes, the property a random per-write nonce guarantees.
func TestSealUsesFreshNonce(t *testing.T) {
	s, _ := NewScheme(testKey, CipherAESGCM, 0)
	plain := []byte("same plaintext, twice")
	a, _ := s.SealPage(nil, plain, 1)
	b, _ := s.SealPage(nil, plain, 1)
	if bytes.Equal(a, b) {
		t.Errorf("two seals produced identical envelopes; nonce is not fresh")
	}
}

// TestOpenWrongKey confirms a different key fails to open an envelope and reports
// ErrWrongKey rather than returning garbage.
func TestOpenWrongKey(t *testing.T) {
	good, _ := NewScheme(testKey, CipherAESGCM, 0)
	env, _ := good.SealPage(nil, []byte("secret"), 3)

	otherKey := bytes.Repeat([]byte{0x5A}, keySize)
	bad, _ := NewScheme(otherKey, CipherAESGCM, 0)
	if _, err := bad.OpenPage(nil, env, 3); !errors.Is(err, ErrWrongKey) {
		t.Errorf("wrong-key open error = %v, want ErrWrongKey", err)
	}
}

// TestOpenWrongPageNo confirms the page number is bound as AAD: an envelope sealed for one
// page does not authenticate when opened as another, defeating page relocation.
func TestOpenWrongPageNo(t *testing.T) {
	s, _ := NewScheme(testKey, CipherAESGCM, 0)
	env, _ := s.SealPage(nil, []byte("page seven"), 7)
	if _, err := s.OpenPage(nil, env, 8); !errors.Is(err, ErrWrongKey) {
		t.Errorf("relocated-page open error = %v, want ErrWrongKey", err)
	}
}

// TestOpenWrongEpoch confirms the epoch is bound as AAD: an envelope sealed under one
// epoch does not authenticate under another, even with the same master key.
func TestOpenWrongEpoch(t *testing.T) {
	e0, _ := NewScheme(testKey, CipherAESGCM, 0)
	env, _ := e0.SealPage(nil, []byte("epoch zero"), 2)
	e1, _ := NewScheme(testKey, CipherAESGCM, 1)
	if _, err := e1.OpenPage(nil, env, 2); !errors.Is(err, ErrWrongKey) {
		t.Errorf("cross-epoch open error = %v, want ErrWrongKey", err)
	}
}

// TestOpenTamperedEnvelope confirms a single flipped bit anywhere in the envelope fails
// authentication.
func TestOpenTamperedEnvelope(t *testing.T) {
	s, _ := NewScheme(testKey, CipherAESGCM, 0)
	env, _ := s.SealPage(nil, []byte("integrity matters"), 5)
	for i := range env {
		tampered := append([]byte(nil), env...)
		tampered[i] ^= 0x01
		if _, err := s.OpenPage(nil, tampered, 5); !errors.Is(err, ErrWrongKey) {
			t.Fatalf("flip at byte %d opened cleanly, want ErrWrongKey", i)
		}
	}
}

// TestNewSchemeRejectsBadKeySize confirms a key that is not 32 bytes is refused.
func TestNewSchemeRejectsBadKeySize(t *testing.T) {
	if _, err := NewScheme([]byte("too short"), CipherAESGCM, 0); !errors.Is(err, ErrKeySize) {
		t.Errorf("short key error = %v, want ErrKeySize", err)
	}
}

// TestNewSchemeRejectsUnsupportedCipher confirms a cipher this build cannot provide is
// refused rather than silently downgraded.
func TestNewSchemeRejectsUnsupportedCipher(t *testing.T) {
	if _, err := NewScheme(testKey, CipherChaCha20, 0); !errors.Is(err, ErrUnsupportedCipher) {
		t.Errorf("chacha20 error = %v, want ErrUnsupportedCipher", err)
	}
}

// TestDescriptorRoundTrip encodes and decodes a descriptor and confirms every field
// survives the trip.
func TestDescriptorRoundTrip(t *testing.T) {
	s, _ := NewScheme(testKey, CipherAESGCM, 4)
	salt := bytes.Repeat([]byte{0x11}, 16)
	d, err := NewDescriptor(s, KDFArgon2id, salt, 64*1024, 3, 2)
	if err != nil {
		t.Fatalf("new descriptor: %v", err)
	}
	got, err := DecodeDescriptor(d.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Cipher != d.Cipher || got.KDF != d.KDF || got.Epoch != d.Epoch ||
		got.Parallelism != d.Parallelism || got.MemoryKiB != d.MemoryKiB || got.Time != d.Time {
		t.Errorf("scalar field mismatch: %+v vs %+v", got, d)
	}
	if !bytes.Equal(got.Salt, d.Salt) || !bytes.Equal(got.VerifyTag, d.VerifyTag) {
		t.Errorf("byte field mismatch after round trip")
	}
}

// TestDescriptorVerifyAcceptsRightKey confirms the descriptor's verification tag
// authenticates against the scheme that built it.
func TestDescriptorVerifyAcceptsRightKey(t *testing.T) {
	s, _ := NewScheme(testKey, CipherAESGCM, 0)
	d, _ := NewDescriptor(s, KDFRaw, nil, 0, 0, 0)
	if err := d.Verify(s); err != nil {
		t.Errorf("verify with the right key = %v, want nil", err)
	}
}

// TestDescriptorVerifyRejectsWrongKey confirms the verification tag rejects a different
// master key with ErrWrongKey, the clean wrong-passphrase signal.
func TestDescriptorVerifyRejectsWrongKey(t *testing.T) {
	good, _ := NewScheme(testKey, CipherAESGCM, 0)
	d, _ := NewDescriptor(good, KDFRaw, nil, 0, 0, 0)

	bad, _ := NewScheme(bytes.Repeat([]byte{0x5A}, keySize), CipherAESGCM, 0)
	if err := d.Verify(bad); !errors.Is(err, ErrWrongKey) {
		t.Errorf("verify with the wrong key = %v, want ErrWrongKey", err)
	}
}

// TestDecodeDescriptorRejectsGarbage confirms non-descriptor bytes are refused with
// ErrBadDescriptor rather than mis-parsed.
func TestDecodeDescriptorRejectsGarbage(t *testing.T) {
	if _, err := DecodeDescriptor([]byte("not a descriptor at all")); !errors.Is(err, ErrBadDescriptor) {
		t.Errorf("garbage decode error = %v, want ErrBadDescriptor", err)
	}
	if _, err := DecodeDescriptor(nil); !errors.Is(err, ErrBadDescriptor) {
		t.Errorf("nil decode error = %v, want ErrBadDescriptor", err)
	}
}

// TestHKDFDeterministic confirms the HKDF helper is a pure function of its inputs and
// that changing info changes the output, the property the key hierarchy relies on.
func TestHKDFDeterministic(t *testing.T) {
	a := hkdf(nil, testKey, []byte("info-1"), 32)
	b := hkdf(nil, testKey, []byte("info-1"), 32)
	c := hkdf(nil, testKey, []byte("info-2"), 32)
	if !bytes.Equal(a, b) {
		t.Errorf("hkdf not deterministic for equal inputs")
	}
	if bytes.Equal(a, c) {
		t.Errorf("hkdf collided across distinct info")
	}
	if len(a) != 32 {
		t.Errorf("hkdf length = %d, want 32", len(a))
	}
}
