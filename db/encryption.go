package db

import (
	"errors"
	"fmt"

	"github.com/tamnd/kv/crypto"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// ErrEncryptedNoKey is returned when an encrypted database is opened without a key.
var ErrEncryptedNoKey = errors.New("kv: database is encrypted, an encryption key is required")

// ErrKeyOnPlaintext is returned when an encryption key is supplied to open a database
// that was not created encrypted: the key does not belong to this file.
var ErrKeyOnPlaintext = errors.New("kv: encryption key supplied for an unencrypted database")

// ErrNotEncrypted is returned when a key-rotation is requested on a database that is not
// encrypted: there is no scheme to rotate.
var ErrNotEncrypted = errors.New("kv: database is not encrypted")

// newEncryptionForCreate builds the encryption scheme and the cleartext descriptor for a
// fresh database when Options.EncryptionKey is set (spec 14). It returns nil, nil, nil
// when encryption is off, the default, so the create path stays byte-for-byte unchanged
// for an unencrypted file. The key is taken as the raw master key (KDFRaw); the
// passphrase KDF lands in a later slice.
func newEncryptionForCreate(opts Options) (*crypto.Scheme, []byte, error) {
	if len(opts.EncryptionKey) == 0 {
		return nil, nil, nil
	}
	s, err := crypto.NewScheme(opts.EncryptionKey, crypto.CipherAESGCM, 0)
	if err != nil {
		return nil, nil, err
	}
	desc, err := crypto.NewDescriptor(s, crypto.KDFRaw, nil, 0, 0, 0)
	if err != nil {
		return nil, nil, err
	}
	return s, desc.Encode(), nil
}

// openEncryptionForExisting reconstructs the encryption scheme for an existing database
// and verifies the supplied key against the on-disk descriptor before any data page is
// read (spec 14 §4). It returns nil, nil when no key is supplied and the file is not
// encrypted; if the file is encrypted, the pager itself enforces that a key was given, so
// a missing key with an encrypted file is reported there. A wrong key surfaces as a clean
// crypto.ErrWrongKey from the descriptor's verification tag, not as garbage pages.
func openEncryptionForExisting(fs vfs.FS, path string, opts Options) (*crypto.Scheme, error) {
	if len(opts.EncryptionKey) == 0 {
		return nil, nil
	}
	desc, err := pager.ReadDescriptor(fs, path)
	if err != nil {
		if errors.Is(err, pager.ErrNotEncrypted) {
			return nil, ErrKeyOnPlaintext
		}
		return nil, err
	}
	s, err := crypto.NewScheme(opts.EncryptionKey, desc.Cipher, desc.Epoch)
	if err != nil {
		return nil, err
	}
	if err := desc.Verify(s); err != nil {
		return nil, fmt.Errorf("kv: open encrypted database: %w", err)
	}
	return s, nil
}
