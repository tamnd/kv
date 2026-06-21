// Package crypto is kv's encryption-at-rest core (spec 14): an AEAD page envelope, a
// two-level key hierarchy, and the cleartext on-disk descriptor that lets a file
// re-derive its key and reject a wrong one with a clean error. It is pure standard
// library, holding kv to its zero-dependency rule (ADR-8): AES-256-GCM comes from
// crypto/aes and crypto/cipher, and the key-derivation function below is HKDF (RFC
// 5869) built on crypto/hmac and crypto/sha256. ChaCha20-Poly1305 and the Argon2id
// passphrase KDF the spec also names live in golang.org/x/crypto, which this project
// does not vendor, so they are deferred to a later slice that implements them in pure
// Go or are reached only through the raw-key path; AES-256-GCM is the spec default and
// is fully native.
package crypto

import (
	"crypto/hmac"
	"crypto/sha256"
)

// hkdfExtract is the extract step of HKDF (RFC 5869 §2.2): it folds the input keying
// material into a fixed-length pseudorandom key with an HMAC keyed by the salt. A nil
// salt is replaced by a string of zeros the length of the hash, per the RFC.
func hkdfExtract(salt, ikm []byte) []byte {
	if len(salt) == 0 {
		salt = make([]byte, sha256.Size)
	}
	mac := hmac.New(sha256.New, salt)
	mac.Write(ikm)
	return mac.Sum(nil)
}

// hkdfExpand is the expand step of HKDF (RFC 5869 §2.3): it stretches a pseudorandom
// key into length bytes of output bound to info, by chaining HMAC blocks. length must
// not exceed 255 hash lengths, which every call here satisfies (all derive a single
// 32-byte key).
func hkdfExpand(prk, info []byte, length int) []byte {
	var out []byte
	var prev []byte
	mac := hmac.New(sha256.New, prk)
	for counter := byte(1); len(out) < length; counter++ {
		mac.Reset()
		mac.Write(prev)
		mac.Write(info)
		mac.Write([]byte{counter})
		prev = mac.Sum(nil)
		out = append(out, prev...)
	}
	return out[:length]
}

// hkdf is the full extract-then-expand derivation, the only HKDF form this package
// calls: a one-shot key derivation from ikm to length bytes bound to info under salt.
func hkdf(salt, ikm, info []byte, length int) []byte {
	return hkdfExpand(hkdfExtract(salt, ikm), info, length)
}
