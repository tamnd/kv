package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/tamnd/kv/vfs"
)

// The physical backup container (spec 18 §2). A backup is a consistent, self-describing
// image of a database: a fixed header, then every live page of the main file, then the
// durable prefix of the WAL. Restoring it writes the two files back and opens them, so the
// restored database is byte-faithful to the source at the backup version, same engine, same
// format, same encryption. The format is its own little file so a restore can validate it
// before touching the destination rather than discovering a truncated stream halfway through.
var backupMagic = [8]byte{'K', 'V', 'B', 'A', 'C', 'K', 'U', 'P'}

// backupFormatVersion is the container layout version, bumped if the header or section
// order ever changes so an old reader rejects a new container instead of misparsing it.
const backupFormatVersion uint32 = 1

// backupHeaderSize is the fixed prefix: magic(8) + formatVersion(4) + pageSize(4) +
// pageCount(4) + dbVersion(8) + walLen(8).
const backupHeaderSize = 8 + 4 + 4 + 4 + 8 + 8

// ErrBackupFormat means a stream handed to RestoreBackup is not a kv backup container, or
// is a version this build does not understand, or is truncated. It is the loud refusal that
// keeps a restore from writing a half-formed database from a corrupt or foreign stream.
var ErrBackupFormat = errors.New("kv: not a valid backup stream")

// backupHeader is the decoded container prefix.
type backupHeader struct {
	pageSize  uint32
	pageCount uint32
	dbVersion uint64
	walLen    uint64
}

func (h backupHeader) encode() []byte {
	b := make([]byte, backupHeaderSize)
	copy(b[0:8], backupMagic[:])
	binary.BigEndian.PutUint32(b[8:12], backupFormatVersion)
	binary.BigEndian.PutUint32(b[12:16], h.pageSize)
	binary.BigEndian.PutUint32(b[16:20], h.pageCount)
	binary.BigEndian.PutUint64(b[20:28], h.dbVersion)
	binary.BigEndian.PutUint64(b[28:36], h.walLen)
	return b
}

func decodeBackupHeader(b []byte) (backupHeader, error) {
	if len(b) < backupHeaderSize {
		return backupHeader{}, ErrBackupFormat
	}
	if [8]byte(b[0:8]) != backupMagic {
		return backupHeader{}, ErrBackupFormat
	}
	if binary.BigEndian.Uint32(b[8:12]) != backupFormatVersion {
		return backupHeader{}, fmt.Errorf("%w: unsupported format version %d", ErrBackupFormat, binary.BigEndian.Uint32(b[8:12]))
	}
	return backupHeader{
		pageSize:  binary.BigEndian.Uint32(b[12:16]),
		pageCount: binary.BigEndian.Uint32(b[16:20]),
		dbVersion: binary.BigEndian.Uint64(b[20:28]),
		walLen:    binary.BigEndian.Uint64(b[28:36]),
	}, nil
}

// Backup streams a consistent physical image of the database to w and returns the commit
// version it captured (spec 18 §2). It folds the WAL into the main file with a checkpoint
// first, so the image is self-contained: the main pages hold every committed change and the
// WAL section is whatever frames the engine still needs past its durable point, an empty log
// for the B-tree core and the kept tail for the LSM core. The result restores with
// RestoreBackup into a database that opens directly and passes Check.
//
// Backup runs under the database's write lock for its duration, so it serializes with the
// single writer: writers wait while the image is copied. This is the simple, always-correct
// physical backup; it copies the file at bulk speed but pauses commits, which is the right
// tradeoff for the periodic-snapshot use and the honest one to document. Reads on other
// goroutines that already hold the shared lock proceed; new reads and writes wait. An
// incremental, writer-online delta is the WAL-shipping path of a later slice.
//
// If the database was opened with an encryption key the image is ciphertext, page for page
// and frame for frame, so the backup is encrypted at rest and a restore needs the same key
// (spec 18 §7). Backing the key up separately is mandatory: without it the backup is
// unrecoverable.
func (d *DB) Backup(w io.Writer) (uint64, error) {
	d.rl.Lock()
	defer d.rl.Unlock()
	if d.fatal != nil {
		return 0, d.fatal
	}
	// Fold everything durable into the main file so the page image is a complete database.
	if err := d.checkpointLocked(); err != nil {
		return 0, err
	}

	pageSize := d.pgr.PageSize()
	pageCount := d.pgr.DBSize()
	dbVersion := d.orc.lastCommitted()
	walImg, err := d.wal.DurableImage()
	if err != nil {
		return 0, err
	}

	hdr := backupHeader{
		pageSize:  uint32(pageSize),
		pageCount: pageCount,
		dbVersion: dbVersion,
		walLen:    uint64(len(walImg)),
	}
	if _, err := w.Write(hdr.encode()); err != nil {
		return 0, err
	}
	// Pages are 1-based; page 1 is the header. ReadRaw returns the on-disk bytes verbatim,
	// including the checksum trailer and, when encrypted, the sealed envelope, so the copy
	// is byte-faithful. No eviction can change a page mid-stream: the write lock is held, so
	// no writer runs to dirty or flush one.
	for pgno := uint32(1); pgno <= pageCount; pgno++ {
		page, err := d.pgr.ReadRaw(pgno)
		if err != nil {
			return 0, err
		}
		if _, err := w.Write(page); err != nil {
			return 0, err
		}
	}
	if _, err := w.Write(walImg); err != nil {
		return 0, err
	}
	return dbVersion, nil
}

// RestoreBackup reconstructs a database from a stream produced by Backup, writing the main
// file at path and its -wal sidecar, then leaving them for Open to read (spec 18 §2). It
// refuses to overwrite an existing file at either path: a restore creates, it never
// clobbers, so a mistaken target fails loudly instead of destroying a live database. The
// restored pair is byte-identical to the source at the backup version, so opening it yields
// the same engine, format, and contents; an encrypted backup restores to an encrypted
// database that needs the original key.
func RestoreBackup(fs vfs.FS, path string, r io.Reader) error {
	walPath := path + walSuffix
	for _, p := range []string{path, walPath} {
		exists, err := fs.Exists(p)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("kv: refusing to restore over existing file %s", p)
		}
	}

	hb := make([]byte, backupHeaderSize)
	if _, err := io.ReadFull(r, hb); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return ErrBackupFormat
		}
		return err
	}
	hdr, err := decodeBackupHeader(hb)
	if err != nil {
		return err
	}

	main, err := fs.Open(path, vfs.OpenReadWrite|vfs.OpenCreate|vfs.OpenExclusive)
	if err != nil {
		return err
	}
	mainLen := int64(hdr.pageCount) * int64(hdr.pageSize)
	if err := streamInto(main, r, mainLen); err != nil {
		main.Close()
		return err
	}
	if err := main.Sync(vfs.SyncFull); err != nil {
		main.Close()
		return err
	}
	if err := main.Close(); err != nil {
		return err
	}

	walFile, err := fs.Open(walPath, vfs.OpenReadWrite|vfs.OpenCreate|vfs.OpenExclusive)
	if err != nil {
		return err
	}
	if err := streamInto(walFile, r, int64(hdr.walLen)); err != nil {
		walFile.Close()
		return err
	}
	if err := walFile.Sync(vfs.SyncFull); err != nil {
		walFile.Close()
		return err
	}
	return walFile.Close()
}

// streamInto copies exactly n bytes from r into f at increasing offsets, in page-sized
// chunks so a large image moves in flat memory. A short read is a truncated container.
func streamInto(f vfs.File, r io.Reader, n int64) error {
	const chunk = 1 << 16
	buf := make([]byte, chunk)
	var off int64
	for off < n {
		want := int64(chunk)
		if rem := n - off; rem < want {
			want = rem
		}
		if _, err := io.ReadFull(r, buf[:want]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return ErrBackupFormat
			}
			return err
		}
		if _, err := f.WriteAt(buf[:want], off); err != nil {
			return err
		}
		off += want
	}
	return nil
}
