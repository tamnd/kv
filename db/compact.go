package db

import (
	"github.com/tamnd/kv/engine"
	"github.com/tamnd/kv/vfs"
)

// compactSuffix names the scratch file a full vacuum builds before swapping it into place.
const compactSuffix = ".compact"

// Compact performs a full vacuum (spec 09 §3.2): it rebuilds the database at path into a
// fresh, maximally compact file holding only the live key/value pairs visible at the
// current snapshot -- no obsolete versions, no tombstones, no TTL-expired entries, no
// freelist holes, no fragmentation -- then atomically swaps it in. It is the deepest
// reclamation, equivalent to dump + load, and unlike the incremental Vacuum it returns
// essentially all reclaimable space in one pass.
//
// It is an offline/maintenance operation, not a steady-state one: it opens path itself and
// must be the only opener for its duration, and it needs room for a second copy of the live
// data while it runs. The caller passes no open handle; the CLI closes its handle first.
//
// Crash safety rests on three self-complete files and one atomic rename. The source is
// checkpointed so its main file stands alone without its WAL; the rebuilt scratch file is
// made durable the same way by the bulk-load fast path; the old WAL is deleted before the
// rename, so the rename is the single commit point. A crash before it leaves the original
// intact, a crash after it leaves the compact file in place, and neither window leaves a
// main file paired with a stale WAL that would replay onto the wrong contents.
func Compact(fs vfs.FS, path string, opts Options) error {
	tmp := path + compactSuffix
	// Clear any scratch left behind by an interrupted earlier run so the rebuild starts on
	// a clean slate rather than reopening a half-built file.
	if err := fs.Delete(tmp, true); err != nil {
		return err
	}
	if err := fs.Delete(tmp+walSuffix, true); err != nil {
		return err
	}

	src, err := Open(fs, path, opts)
	if err != nil {
		return err
	}
	// Fold the source WAL into its main file so the original stands alone: the swap deletes
	// that WAL, and a crash mid-swap must leave the original recoverable without it.
	if err := src.Checkpoint(); err != nil {
		src.Close()
		return err
	}

	// Mirror the source's physical format so compaction preserves the page size and engine.
	// The header on an existing file wins over opts, so read the live values back from the
	// pager rather than trusting what the caller passed.
	dstOpts := opts
	dstOpts.PageSize = src.pgr.PageSize()
	dstOpts.Engine = src.pgr.Header().Engine

	dst, err := Open(fs, tmp, dstOpts)
	if err != nil {
		src.Close()
		return err
	}

	if err := compactCopy(src, dst); err != nil {
		dst.Close()
		src.Close()
		fs.Delete(tmp, true)
		fs.Delete(tmp+walSuffix, true)
		return err
	}

	// The bulk-load fast path already checkpointed the scratch file, so its main file stands
	// alone. Close both handles so no open file blocks the rename.
	if err := dst.Close(); err != nil {
		src.Close()
		fs.Delete(tmp, true)
		fs.Delete(tmp+walSuffix, true)
		return err
	}
	if err := src.Close(); err != nil {
		fs.Delete(tmp, true)
		fs.Delete(tmp+walSuffix, true)
		return err
	}

	// Both main files now stand alone. Remove the original WAL first so the about-to-be
	// installed main is never paired with a stale log, then atomically swap the compact main
	// into place, then drop the now-redundant scratch WAL.
	if err := fs.Delete(path+walSuffix, true); err != nil {
		return err
	}
	if err := fs.Rename(tmp, path, true); err != nil {
		return err
	}
	return fs.Delete(tmp+walSuffix, true)
}

// compactCopy streams every live pair visible at the source's current snapshot into the
// destination through its bulk-load fast path. The engine reader resolves MVCC, skips
// tombstones, drops TTL-expired entries, and yields keys in ascending order, so the
// destination -- a fresh, empty database -- takes the bottom-up build that writes each page
// exactly once. A read fault in the source surfaces through readErr, since the pull function
// the loader drives cannot return an error directly.
func compactCopy(src, dst *DB) error {
	snap := engine.Snapshot{Version: src.Version(), Now: src.now()}
	rd, err := src.eng.NewReader(snap)
	if err != nil {
		return err
	}
	defer rd.Close()
	cur, err := rd.NewIter(engine.IterOptions{})
	if err != nil {
		return err
	}
	defer cur.Close()

	cur.First()
	var readErr error
	next := func() (key, value []byte, ok bool) {
		if readErr != nil || !cur.Valid() {
			return nil, nil, false
		}
		k := cur.Key()
		lv, err := cur.Value()
		if err != nil {
			readErr = err
			return nil, nil, false
		}
		v, err := lv.Value()
		if err != nil {
			readErr = err
			return nil, nil, false
		}
		// Clone both: the loader holds them past the next cursor step, which reuses the
		// underlying page buffers.
		k = append([]byte(nil), k...)
		v = append([]byte(nil), v...)
		cur.Next()
		return k, v, true
	}

	if _, err := dst.Load(next); err != nil {
		return err
	}
	if readErr != nil {
		return readErr
	}
	return cur.Error()
}
