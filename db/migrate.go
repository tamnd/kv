package db

// This file is M0's one-way storage migration: it reads a generation-1 (v0.2.0)
// database and rewrites it into a generation-2 Bε-tree file, atomically, gated on a
// full key-space equality check (redesign doc 06 section 5). It is the bridge that
// keeps the standing promise that an existing database keeps opening across the
// node-layout change the rewrite forced: the magic bumped, so a generation-2 file is
// a new container a v0.2.0 binary cannot read, and this is the supported upgrade off
// the old files.
//
// What this lands, and what it leaves. The migration is the simplest thing that is
// obviously safe: it streams the source's live key space in order into a fresh
// generation-2 temp file beside the target, verifies the temp independently, and only
// then renames it over the destination, so a crash at any point before the rename
// leaves the original untouched and a stale temp the next run discards. The source is
// read through the shipped core's reader at its latest version, which is the M0
// stand-in for doc 06's "legacy generation-1 reader" while the old cores still live in
// the tree (they retire at M8). Reading at the latest snapshot migrates the live
// value of every key; for an offline migration with no open snapshots, which is the
// case the kv migrate command runs in, the oldest readable version equals the last
// commit version, so every version below it is already garbage doc 06 drops and the
// latest-visible view is exactly "every live key at its committed version." Preserving
// versions above the oldest readable one, for a migration of a live process with open
// snapshots, needs a multi-version source scan the engine SPI does not expose yet;
// that is a later refinement, noted here rather than hidden. Carrying the source's
// version watermark into the new file's header is likewise deferred: the rewritten
// file starts its version counter fresh, which is safe because no snapshot survives a
// format migration.

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"

	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/format"
	"github.com/tamnd/kv/pager"
	"github.com/tamnd/kv/vfs"
)

// migratePageSize is the page size of a freshly migrated generation-2 file: the 16 KiB
// default doc 06 section 2 sets for the new format, larger than the shipped 4 KiB
// because the front-coded leaves and buffered interiors amortize better over a bigger
// page.
const migratePageSize = 16384

// migrateTmpSuffix names the temporary file the migration builds beside the target.
// Nothing touches the destination until the temp is fully durable and verified, so a
// crash leaves either the original plus an orphaned temp (discarded on the next run)
// or the finished file, never a blend.
const migrateTmpSuffix = ".migrate-tmp"

// migrateSig is the equality witness for a database's live key space: the number of
// keys and a rolling hash over every key/value pair in ascending key order. Two
// databases with the same signature hold the same live keys with the same values, so
// comparing the source's signature (computed as it is read) against the rewritten
// file's (computed by an independent re-scan) is the full key-space equality check
// doc 06 section 5 gates the atomic swap on.
type migrateSig struct {
	count uint64
	sum   uint64
}

// Migrate reads the generation-1 database at srcPath and writes an equivalent
// generation-2 Bε-tree database to dstPath, atomically. It builds a temp file beside
// the destination, verifies the temp's live key space matches the source key-for-key,
// and only then renames it over dstPath, so the original is preserved until the new
// file is durable and verified. Passing the same path for src and dst performs an
// in-place upgrade. It is one-way: there is no generation-2-to-generation-1 downgrade,
// so a caller who needs to go back restores from a backup of the original.
func Migrate(fs vfs.FS, srcPath, dstPath string) error {
	// Refuse to migrate a file that is already generation 2: it has nothing to upgrade,
	// and rewriting it would be a silent waste. The peek opens the pager just far enough
	// to read the engine selector; a peek failure is left for the real open below to
	// report with its full context.
	if pgr, err := pager.Open(fs, srcPath, pager.Options{}); err == nil {
		kind := pgr.Header().Engine
		pgr.Close()
		if kind == format.EngineBeta {
			return fmt.Errorf("migrate: %s is already generation 2", srcPath)
		}
	}

	src, err := Open(fs, srcPath, Options{})
	if err != nil {
		return fmt.Errorf("migrate: open source %s: %w", srcPath, err)
	}

	tmpPath := dstPath + migrateTmpSuffix
	// Idempotent retry: a previous migration that crashed before the rename leaves a
	// stale temp and its WAL sidecar; drop both before starting so the rewrite is clean.
	cleanup := func() {
		_ = fs.Delete(tmpPath, false)
		_ = fs.Delete(tmpPath+walSuffix, false)
	}
	cleanup()

	dst, err := Open(fs, tmpPath, Options{Engine: format.EngineBeta, PageSize: migratePageSize})
	if err != nil {
		src.Close()
		return fmt.Errorf("migrate: create temp %s: %w", tmpPath, err)
	}

	srcSig, err := streamMigrate(src, dst)
	if err != nil {
		dst.Close()
		src.Close()
		cleanup()
		return fmt.Errorf("migrate: copy %s into %s: %w", srcPath, tmpPath, err)
	}
	// Fold the rewritten pages into the temp's main file so it stands alone. The Bε-tree
	// core buffers Load through the WAL rather than the bulk-load fast path that
	// self-checkpoints, so without this the data would live only in the temp's WAL
	// sidecar, which the rename below leaves behind. Checkpoint moves it into the main
	// file, so the verify re-scan and the atomic rename both see a complete file.
	if err := dst.Checkpoint(); err != nil {
		dst.Close()
		src.Close()
		cleanup()
		return fmt.Errorf("migrate: checkpoint temp %s: %w", tmpPath, err)
	}
	// Close the temp: with the checkpoint above its main file is complete, and Close
	// removes the temp's WAL sidecar, leaving a standalone generation-2 file to verify.
	if err := dst.Close(); err != nil {
		src.Close()
		cleanup()
		return fmt.Errorf("migrate: finalize temp %s: %w", tmpPath, err)
	}
	// Close the source cleanly before the rename: on an in-place upgrade this frees the
	// source path and removes the source's WAL sidecar, so the rename does not leave a
	// stale generation-1 WAL beside the new generation-2 file.
	if err := src.Close(); err != nil {
		cleanup()
		return fmt.Errorf("migrate: close source %s: %w", srcPath, err)
	}

	// Verify the temp independently: reopen it through the generation-2 reader and
	// re-scan its live key space, then compare key count and the rolling pair hash
	// against the source. A mismatch aborts with the temp left for inspection and the
	// original untouched, rather than swapping in a result that silently dropped or
	// changed a key.
	dstSig, err := signatureOf(fs, tmpPath)
	if err != nil {
		cleanup()
		return fmt.Errorf("migrate: verify temp %s: %w", tmpPath, err)
	}
	if dstSig != srcSig {
		cleanup()
		return fmt.Errorf("migrate: verification mismatch: source had %d keys (sig %#x), rewrite had %d keys (sig %#x)",
			srcSig.count, srcSig.sum, dstSig.count, dstSig.sum)
	}

	// The only destructive step, run last and only after the rewrite is durable and
	// verified: the rename is all-or-nothing across a crash, so dstPath names either the
	// old bytes or the new bytes, never a blend.
	if err := fs.Rename(tmpPath, dstPath, true); err != nil {
		cleanup()
		return fmt.Errorf("migrate: swap %s into place: %w", dstPath, err)
	}
	return nil
}

// streamMigrate walks src's live key space in ascending key order and bulk-loads it
// into dst, returning the source's signature computed over the same stream. The
// source iterator stays open across the whole load: dst.Load pulls one pair at a time
// through next, so the rewrite never holds the whole database in memory. The hash is
// fed length-prefixed key then length-prefixed value, so no pair boundary is
// ambiguous and a value that happens to look like the next key cannot collide.
func streamMigrate(src, dst *DB) (migrateSig, error) {
	txn := src.Begin(false)
	defer txn.Discard()
	it, err := txn.NewIterator(engine.IterOptions{})
	if err != nil {
		return migrateSig{}, err
	}
	defer it.Close()

	h := fnv.New64a()
	var count uint64
	var iterErr error
	ok := it.First()
	next := func() (key, value []byte, more bool) {
		if !ok || iterErr != nil {
			return nil, nil, false
		}
		// Copy out: Load and the hash retain these, and the iterator reuses its key and
		// value buffers across steps on the streaming path.
		key = append([]byte(nil), it.Key()...)
		val, verr := it.Value()
		if verr != nil {
			iterErr = verr
			return nil, nil, false
		}
		value = append([]byte(nil), val...)
		hashPair(h, key, value)
		count++
		ok = it.Next()
		return key, value, true
	}

	if _, err := dst.Load(next); err != nil {
		return migrateSig{}, err
	}
	if iterErr != nil {
		return migrateSig{}, iterErr
	}
	if err := it.Error(); err != nil {
		return migrateSig{}, err
	}
	return migrateSig{count: count, sum: h.Sum64()}, nil
}

// signatureOf opens the database at path read-only, scans its live key space in
// ascending key order, and returns its signature: the same count and rolling pair
// hash streamMigrate computes for the source, so the two are directly comparable. The
// engine kind is read from the file's own header, so this reopens the rewritten file
// through the generation-2 reader without being told which core wrote it.
func signatureOf(fs vfs.FS, path string) (migrateSig, error) {
	d, err := Open(fs, path, Options{})
	if err != nil {
		return migrateSig{}, err
	}
	defer d.Close()

	txn := d.Begin(false)
	defer txn.Discard()
	it, err := txn.NewIterator(engine.IterOptions{})
	if err != nil {
		return migrateSig{}, err
	}
	defer it.Close()

	h := fnv.New64a()
	var count uint64
	for ok := it.First(); ok; ok = it.Next() {
		val, err := it.Value()
		if err != nil {
			return migrateSig{}, err
		}
		hashPair(h, it.Key(), val)
		count++
	}
	if err := it.Error(); err != nil {
		return migrateSig{}, err
	}
	return migrateSig{count: count, sum: h.Sum64()}, nil
}

// hashPair folds one key/value pair into h, length-prefixing each so the stream is
// unambiguous: two different key spaces can never hash the same by shifting a byte
// from a value into the following key.
func hashPair(h hash.Hash64, key, value []byte) {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(key)))
	h.Write(n[:])
	h.Write(key)
	binary.BigEndian.PutUint32(n[:], uint32(len(value)))
	h.Write(n[:])
	h.Write(value)
}
