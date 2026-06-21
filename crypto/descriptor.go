package crypto

import (
	"crypto/subtle"
	"encoding/binary"
	"errors"
)

// descMagic marks the start of an encryption descriptor on disk.
var descMagic = [4]byte{'K', 'V', 'E', 'N'}

// descVersion is the descriptor layout version, bumped if the encoding below changes.
const descVersion = 1

// verifyConst is the fixed plaintext sealed into a descriptor's verification tag. Opening
// the tag with the right key recovers exactly these bytes; the wrong key fails to
// authenticate. The value is arbitrary, only its constancy matters.
var verifyConst = []byte("kv/crypto verify")

// verifyPageNo is the page number the verification tag is bound to as AAD. It is a
// sentinel outside the file's real page space so the tag can never be confused with a
// data page envelope.
const verifyPageNo = 0xFFFFFFFF

// ErrBadDescriptor is returned when the descriptor bytes are missing, truncated, or do
// not carry the expected magic and version: the file is not a kv encrypted file, or its
// header is damaged.
var ErrBadDescriptor = errors.New("kv/crypto: missing or malformed encryption descriptor")

// Descriptor is the cleartext, self-describing record stored on the file's first page
// (spec 14 §3, §4). It names the cipher and key-derivation function, carries the KDF salt
// and cost parameters, records the current key epoch, and holds a verification tag the
// master key must authenticate. A reader decodes it, re-derives the master key, and
// checks the tag before touching any data page, so a wrong passphrase or key is a clean
// error rather than a wall of garbage.
type Descriptor struct {
	Cipher      Cipher
	KDF         KDF
	Epoch       uint32
	Parallelism uint8  // Argon2id lanes; 0 on the raw-key path
	MemoryKiB   uint32 // Argon2id memory cost; 0 on the raw-key path
	Time        uint32 // Argon2id iterations; 0 on the raw-key path
	Salt        []byte // Argon2id salt; empty on the raw-key path
	VerifyTag   []byte // AEAD envelope of verifyConst under the master key
}

// descFixed is the size of the descriptor's fixed-layout prefix, before the variable salt
// and verification tag.
const descFixed = 4 + 1 + 1 + 1 + 1 + 4 + 4 + 4 + 1 + 2

//                magic ver cip kdf par epoch mem time saltLen verifyLen

// NewDescriptor builds a descriptor for a scheme, sealing the verification constant under
// the scheme's keys so the tag authenticates only to the right master key. salt and the
// KDF cost parameters describe how the master key was derived; they are zero and empty on
// the raw-key path, where kdf is KDFRaw.
func NewDescriptor(s *Scheme, kdf KDF, salt []byte, memKiB, time uint32, parallelism uint8) (*Descriptor, error) {
	verify, err := s.SealPage(nil, verifyConst, verifyPageNo)
	if err != nil {
		return nil, err
	}
	return &Descriptor{
		Cipher:      s.cipher,
		KDF:         kdf,
		Epoch:       s.epoch,
		Parallelism: parallelism,
		MemoryKiB:   memKiB,
		Time:        time,
		Salt:        append([]byte(nil), salt...),
		VerifyTag:   verify,
	}, nil
}

// Verify confirms a scheme's master key authenticates against the descriptor's tag,
// returning ErrWrongKey if it does not. It is the gate openExisting runs before trusting
// any page: a wrong key, or a tampered descriptor, fails here.
func (d *Descriptor) Verify(s *Scheme) error {
	pt, err := s.OpenPage(nil, d.VerifyTag, verifyPageNo)
	if err != nil {
		return err
	}
	if subtle.ConstantTimeCompare(pt, verifyConst) != 1 {
		return ErrWrongKey
	}
	return nil
}

// Encode serializes the descriptor to its on-disk byte form. The layout is fixed-prefix
// then variable salt then variable verification tag, each length-prefixed, so a reader
// can decode without knowing the KDF in advance.
func (d *Descriptor) Encode() []byte {
	if len(d.Salt) > 255 {
		panic("kv/crypto: salt too long")
	}
	out := make([]byte, descFixed+len(d.Salt)+len(d.VerifyTag))
	copy(out[0:4], descMagic[:])
	out[4] = descVersion
	out[5] = byte(d.Cipher)
	out[6] = byte(d.KDF)
	out[7] = d.Parallelism
	binary.BigEndian.PutUint32(out[8:12], d.Epoch)
	binary.BigEndian.PutUint32(out[12:16], d.MemoryKiB)
	binary.BigEndian.PutUint32(out[16:20], d.Time)
	out[20] = byte(len(d.Salt))
	binary.BigEndian.PutUint16(out[21:23], uint16(len(d.VerifyTag)))
	n := descFixed
	n += copy(out[n:], d.Salt)
	copy(out[n:], d.VerifyTag)
	return out
}

// DecodeDescriptor parses an on-disk descriptor. It returns ErrBadDescriptor if the bytes
// are too short or do not carry the expected magic and version, the signal that the file
// is not a kv encrypted file or its header is damaged.
func DecodeDescriptor(b []byte) (*Descriptor, error) {
	if len(b) < descFixed {
		return nil, ErrBadDescriptor
	}
	if [4]byte(b[0:4]) != descMagic || b[4] != descVersion {
		return nil, ErrBadDescriptor
	}
	saltLen := int(b[20])
	verifyLen := int(binary.BigEndian.Uint16(b[21:23]))
	if len(b) < descFixed+saltLen+verifyLen {
		return nil, ErrBadDescriptor
	}
	d := &Descriptor{
		Cipher:      Cipher(b[5]),
		KDF:         KDF(b[6]),
		Parallelism: b[7],
		Epoch:       binary.BigEndian.Uint32(b[8:12]),
		MemoryKiB:   binary.BigEndian.Uint32(b[12:16]),
		Time:        binary.BigEndian.Uint32(b[16:20]),
	}
	n := descFixed
	d.Salt = append([]byte(nil), b[n:n+saltLen]...)
	n += saltLen
	d.VerifyTag = append([]byte(nil), b[n:n+verifyLen]...)
	return d, nil
}
