package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
)

// Cipher identifies the AEAD a file is encrypted with, recorded in the cleartext
// descriptor so a reader knows how to open a page (spec 14 §3).
type Cipher uint8

const (
	// CipherNone is the zero value: the file is not encrypted.
	CipherNone Cipher = 0
	// CipherAESGCM is AES-256-GCM, the spec default, hardware-accelerated through
	// AES-NI on amd64 and arm64 via the standard library.
	CipherAESGCM Cipher = 1
	// CipherChaCha20 (ChaCha20-Poly1305) is named by the spec for platforms without
	// AES acceleration. It lives in golang.org/x/crypto, which this project does not
	// vendor, so it is reserved here and not yet selectable.
	CipherChaCha20 Cipher = 2
)

// String renders a Cipher for logs and errors.
func (c Cipher) String() string {
	switch c {
	case CipherNone:
		return "none"
	case CipherAESGCM:
		return "aes-256-gcm"
	case CipherChaCha20:
		return "chacha20-poly1305"
	default:
		return "cipher?"
	}
}

// KDF identifies how the master key was produced, recorded in the descriptor so a
// reader knows whether to run a passphrase KDF or take a supplied raw key (spec 14 §4).
type KDF uint8

const (
	// KDFRaw means a 32-byte key was supplied directly, the KMS-managed path: no
	// passphrase stretching, the key is used as the master key as-is.
	KDFRaw KDF = 0
	// KDFArgon2id is the memory-hard passphrase KDF the spec defaults to. Argon2id
	// needs BLAKE2b, which is not in the standard library, so it is reserved here and
	// arrives in a later slice; today encryption is reached through the raw-key path.
	KDFArgon2id KDF = 1
)

const (
	// nonceSize is AES-GCM's standard 96-bit nonce.
	nonceSize = 12
	// tagSize is AES-GCM's 128-bit authentication tag.
	tagSize = 16
	// keySize is the 256-bit key length for AES-256 and every derived key.
	keySize = 32
	// epochSize is the width of the key epoch stored in cleartext at the tail of every
	// envelope, so a reader derives the key for the epoch that actually sealed the page
	// rather than the file's current one. This is what lets a rotation be lazy: pages of
	// different epochs coexist and each decrypts under its own key (spec 14 §5).
	epochSize = 4
	// Overhead is the per-page envelope cost in bytes: the AEAD tag, the stored nonce, and
	// the cleartext epoch tag. A page's usable area shrinks by this much when a file is
	// encrypted, which the header's reserved-per-page byte accounts for (spec 02 §2,
	// spec 14 §3, §5).
	Overhead = nonceSize + tagSize + epochSize
)

// ErrWrongKey is returned when a key fails to authenticate against the file: a wrong
// passphrase or key, or a corrupt or tampered descriptor. It is the clean error the
// spec promises in place of garbage data (spec 14 §2, §4).
var ErrWrongKey = errors.New("kv/crypto: wrong encryption key or corrupt descriptor")

// ErrKeySize is returned when a supplied raw key is not 32 bytes.
var ErrKeySize = errors.New("kv/crypto: encryption key must be 32 bytes")

// ErrUnsupportedCipher is returned when a file names a cipher this build cannot
// provide (today, anything but AES-256-GCM).
var ErrUnsupportedCipher = errors.New("kv/crypto: unsupported cipher")

// Scheme encrypts and decrypts pages under a file's master key (spec 14 §4). It holds
// the master key, the current key epoch, and the epoch's derived data-encryption key
// (DEK); per-page keys are derived from the DEK on each call so a single AEAD key never
// spans many pages. It is safe for concurrent use: every method derives its own cipher
// and touches no shared mutable state.
type Scheme struct {
	cipher Cipher
	epoch  uint32
	master []byte // the key the descriptor's verification tag is bound to
	dek    []byte // data-encryption key for the current epoch
}

// NewScheme builds a Scheme from a 32-byte master key, the cipher, and the current key
// epoch. The master key is the output of the KDF (or the raw supplied key); this
// derives the epoch's DEK and is ready to seal and open pages.
func NewScheme(master []byte, c Cipher, epoch uint32) (*Scheme, error) {
	if len(master) != keySize {
		return nil, ErrKeySize
	}
	if c != CipherAESGCM {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedCipher, c)
	}
	s := &Scheme{
		cipher: c,
		epoch:  epoch,
		master: append([]byte(nil), master...),
		dek:    deriveDEK(master, epoch),
	}
	return s, nil
}

// Epoch reports the key epoch the scheme seals new pages under.
func (s *Scheme) Epoch() uint32 { return s.epoch }

// Rotate returns a new scheme that seals under newEpoch, derived from the same master key
// (spec 14 §5). The returned scheme still opens pages of any earlier epoch, because its DEK
// derivation is keyed by the epoch the page records, so a rotation does not invalidate the
// pages already on disk: it only changes the epoch new and rewritten pages are sealed under.
// The master key is unchanged, which is the spec's lazy DEK-rotation model, distinct from a
// passphrase change that would re-wrap the master.
func (s *Scheme) Rotate(newEpoch uint32) *Scheme {
	return &Scheme{
		cipher: s.cipher,
		epoch:  newEpoch,
		master: s.master,
		dek:    deriveDEK(s.master, newEpoch),
	}
}

// deriveDEK derives the data-encryption key for an epoch from the master key, so a key
// rotation is a matter of bumping the epoch and re-deriving rather than re-deriving from
// the passphrase (spec 14 §4, §5).
func deriveDEK(master []byte, epoch uint32) []byte {
	info := make([]byte, len("kv/dek")+4)
	copy(info, "kv/dek")
	binary.BigEndian.PutUint32(info[len("kv/dek"):], epoch)
	return hkdf(nil, master, info, keySize)
}

// dekFor returns the data-encryption key for an arbitrary epoch. The scheme's current epoch
// hits the precomputed DEK with no work, the common case for reads of recently written pages;
// any other epoch is derived on the spot from the master key. This is what makes a lazy
// rotation cheap on the hot path while still opening pages sealed under an older epoch
// (spec 14 §5). The derivation touches only immutable fields, so it is safe to call
// concurrently.
func (s *Scheme) dekFor(epoch uint32) []byte {
	if epoch == s.epoch {
		return s.dek
	}
	return deriveDEK(s.master, epoch)
}

// pageGCM derives the per-page key for pageNo under a given epoch and returns an
// AES-256-GCM AEAD over it. Deriving a key per page number keeps any one AEAD key from
// spanning many pages, so a random per-write nonce stays well within its safe budget for
// the writes a single page sees (spec 14 §3, §4). The epoch selects the DEK the page key
// descends from, so a page sealed under one epoch opens under that epoch's key after a
// rotation (spec 14 §5).
func (s *Scheme) pageGCM(pageNo, epoch uint32) (cipher.AEAD, error) {
	info := make([]byte, len("kv/page")+4)
	copy(info, "kv/page")
	binary.BigEndian.PutUint32(info[len("kv/page"):], pageNo)
	pageKey := hkdf(nil, s.dekFor(epoch), info, keySize)
	block, err := aes.NewCipher(pageKey)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// walGCM derives the per-frame key for a WAL frame at lsn from the epoch DEK and returns
// an AES-256-GCM AEAD over it. WAL frames live in a key namespace distinct from main-file
// pages (the info label differs), so a frame key and a page key never coincide even at the
// same numeric index, and the full 64-bit LSN keys the derivation so every frame in a
// generation has its own key (spec 14 §3).
func (s *Scheme) walGCM(lsn uint64, epoch uint32) (cipher.AEAD, error) {
	info := make([]byte, len("kv/wal")+8)
	copy(info, "kv/wal")
	binary.BigEndian.PutUint64(info[len("kv/wal"):], lsn)
	frameKey := hkdf(nil, s.dekFor(epoch), info, keySize)
	block, err := aes.NewCipher(frameKey)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// walAAD binds a WAL frame envelope to its LSN and key epoch as additional authenticated
// data, so a frame cannot be reordered to a different LSN or replayed from another epoch
// without failing authentication (spec 14 §3).
func walAAD(lsn uint64, epoch uint32) []byte {
	aad := make([]byte, 12)
	binary.BigEndian.PutUint64(aad[0:8], lsn)
	binary.BigEndian.PutUint32(aad[8:12], epoch)
	return aad
}

// SealWAL encrypts a WAL frame payload for lsn and returns the on-disk envelope
// (ciphertext, tag, nonce, epoch), len(plaintext)+Overhead bytes, with the same layout
// SealPage uses. The frame is sealed under the scheme's current epoch, which is stored in
// the cleartext trailer so recovery decrypts it under the right key after a rotation. The
// nonce is fresh per call. dst backs the result if it has the capacity.
func (s *Scheme) SealWAL(dst, plaintext []byte, lsn uint64) ([]byte, error) {
	aead, err := s.walGCM(lsn, s.epoch)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, nonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	out := aead.Seal(dst[:0], nonce, plaintext, walAAD(lsn, s.epoch))
	out = append(out, nonce...)
	out = appendEpoch(out, s.epoch)
	return out, nil
}

// OpenWAL decrypts a WAL frame envelope for lsn, returning the plaintext payload. The epoch
// stored in the envelope selects the key, so a frame survives a rotation. A failed
// authentication (a wrong key or a tampered frame) is reported as ErrWrongKey. dst backs
// the plaintext if it has the capacity.
func (s *Scheme) OpenWAL(dst, env []byte, lsn uint64) ([]byte, error) {
	if len(env) < Overhead {
		return nil, ErrWrongKey
	}
	body, epoch := splitEpoch(env)
	aead, err := s.walGCM(lsn, epoch)
	if err != nil {
		return nil, err
	}
	ctTag := body[:len(body)-nonceSize]
	nonce := body[len(body)-nonceSize:]
	pt, err := aead.Open(dst[:0], nonce, ctTag, walAAD(lsn, epoch))
	if err != nil {
		return nil, ErrWrongKey
	}
	return pt, nil
}

// appendEpoch writes the 4-byte big-endian key epoch onto the tail of an envelope.
func appendEpoch(dst []byte, epoch uint32) []byte {
	var b [epochSize]byte
	binary.BigEndian.PutUint32(b[:], epoch)
	return append(dst, b[:]...)
}

// splitEpoch peels the cleartext epoch trailer off an envelope, returning the AEAD body
// (ciphertext, tag, nonce) and the epoch that sealed it. The caller has already checked the
// envelope is at least Overhead bytes.
func splitEpoch(env []byte) (body []byte, epoch uint32) {
	n := len(env)
	epoch = binary.BigEndian.Uint32(env[n-epochSize:])
	return env[:n-epochSize], epoch
}

// pageAAD binds a page envelope to its page number and key epoch as additional
// authenticated data, so a ciphertext page cannot be silently relocated to another page
// number or replayed from another epoch without failing authentication (spec 14 §3).
func pageAAD(pageNo, epoch uint32) []byte {
	aad := make([]byte, 8)
	binary.BigEndian.PutUint32(aad[0:4], pageNo)
	binary.BigEndian.PutUint32(aad[4:8], epoch)
	return aad
}

// SealPage encrypts plaintext for pageNo and returns the on-disk envelope, laid out as
// ciphertext, then the 16-byte tag, then the 12-byte nonce, then the 4-byte cleartext key
// epoch: len(plaintext)+Overhead bytes. The page is sealed under the scheme's current epoch,
// stored in the trailer so a reader decrypts it under that epoch's key even after the file
// has rotated to a newer one (spec 14 §5). The nonce is freshly random per call and stored
// in the envelope, so a page rewritten in place never reuses a (key, nonce) pair. dst is used
// for the result if it has the capacity, else a new slice is allocated.
func (s *Scheme) SealPage(dst, plaintext []byte, pageNo uint32) ([]byte, error) {
	aead, err := s.pageGCM(pageNo, s.epoch)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, nonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	// Seal writes ciphertext||tag into the front; the nonce and epoch trailer follow.
	// Reusing dst's backing array when it is large enough keeps the write path allocation-free.
	out := dst[:0]
	out = aead.Seal(out, nonce, plaintext, pageAAD(pageNo, s.epoch))
	out = append(out, nonce...)
	out = appendEpoch(out, s.epoch)
	return out, nil
}

// OpenPage decrypts an on-disk envelope (ciphertext||tag||nonce||epoch, as SealPage produced)
// for pageNo, returning the plaintext. The epoch stored in the trailer selects the key, so a
// page sealed under an older epoch still opens after a rotation. A failed authentication, the
// signature of a wrong key or a tampered page, is reported as ErrWrongKey. dst is used for the
// plaintext if it has the capacity, else a new slice is allocated.
func (s *Scheme) OpenPage(dst, env []byte, pageNo uint32) ([]byte, error) {
	if len(env) < Overhead {
		return nil, ErrWrongKey
	}
	body, epoch := splitEpoch(env)
	aead, err := s.pageGCM(pageNo, epoch)
	if err != nil {
		return nil, err
	}
	ctTag := body[:len(body)-nonceSize]
	nonce := body[len(body)-nonceSize:]
	pt, err := aead.Open(dst[:0], nonce, ctTag, pageAAD(pageNo, epoch))
	if err != nil {
		return nil, ErrWrongKey
	}
	return pt, nil
}
