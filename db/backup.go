package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/tamnd/kv/format"
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
// Version 2 added the f2 sidecar section after the WAL.
const backupFormatVersion uint32 = 2

// backupHeaderSize is the fixed prefix: magic(8) + formatVersion(4) + pageSize(4) +
// pageCount(4) + dbVersion(8) + walLen(8) + sidecarLen(8).
const backupHeaderSize = 8 + 4 + 4 + 4 + 8 + 8 + 8

// ErrBackupFormat means a stream handed to RestoreBackup is not a kv backup container, or
// is a version this build does not understand, or is truncated. It is the loud refusal that
// keeps a restore from writing a half-formed database from a corrupt or foreign stream.
var ErrBackupFormat = errors.New("kv: not a valid backup stream")

// backupHeader is the decoded container prefix. sidecarLen is the length of the f2 core's
// self-durable file copied after the WAL; it is zero for the pager-backed cores, which keep
// all their state in the main pages.
type backupHeader struct {
	pageSize   uint32
	pageCount  uint32
	dbVersion  uint64
	walLen     uint64
	sidecarLen uint64
}

func (h backupHeader) encode() []byte {
	b := make([]byte, backupHeaderSize)
	copy(b[0:8], backupMagic[:])
	binary.BigEndian.PutUint32(b[8:12], backupFormatVersion)
	binary.BigEndian.PutUint32(b[12:16], h.pageSize)
	binary.BigEndian.PutUint32(b[16:20], h.pageCount)
	binary.BigEndian.PutUint64(b[20:28], h.dbVersion)
	binary.BigEndian.PutUint64(b[28:36], h.walLen)
	binary.BigEndian.PutUint64(b[36:44], h.sidecarLen)
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
		pageSize:   binary.BigEndian.Uint32(b[12:16]),
		pageCount:  binary.BigEndian.Uint32(b[16:20]),
		dbVersion:  binary.BigEndian.Uint64(b[20:28]),
		walLen:     binary.BigEndian.Uint64(b[28:36]),
		sidecarLen: binary.BigEndian.Uint64(b[36:44]),
	}, nil
}

// Backup streams a consistent physical image of the database to w and returns the commit
// version it captured (spec 18 §2). It folds the WAL into the main file with a checkpoint
// first, so the image is self-contained: the main pages hold every committed change and the
// WAL section is whatever frames the engine still needs past its durable point, an empty log
// for the B-tree core and the kept tail for the LSM core. The result restores with
// RestoreBackup into a database that opens directly and passes Check.
//
// The f2 core is self-durable: its state lives in a sidecar file beside the main one, not in
// the main pages. For an f2 database the image carries that sidecar verbatim after the WAL,
// so the restore is the sidecar plus the kept WAL tail, exactly the pair f2 recovers from on
// a normal reopen. The bytes are copied as they sit on disk, so a sealed sidecar stays
// sealed in the backup and the same key restores it.
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
	// The f2 core keeps its state in its own file, so copy it whole after the checkpoint.
	// The write lock is held, so no writer is dirtying it and the on-disk bytes are the
	// complete image at this version.
	var sidecar []byte
	if d.pgr.Header().Engine == format.EngineF2 {
		sidecar, err = d.readF2Sidecar()
		if err != nil {
			return 0, err
		}
	}

	hdr := backupHeader{
		pageSize:   uint32(pageSize),
		pageCount:  pageCount,
		dbVersion:  dbVersion,
		walLen:     uint64(len(walImg)),
		sidecarLen: uint64(len(sidecar)),
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
	if _, err := w.Write(sidecar); err != nil {
		return 0, err
	}
	return dbVersion, nil
}

// readF2Sidecar reads the f2 core's self-durable file whole, for the backup image. The
// caller holds the write lock and has just checkpointed, so the file is stable and complete
// at the backup version. The bytes are copied verbatim, so a sealed sidecar stays sealed.
func (d *DB) readF2Sidecar() ([]byte, error) {
	f, err := d.fs.Open(d.path+f2Suffix, vfs.OpenReadWrite)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	if size > 0 {
		if _, err := f.ReadAt(buf, 0); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// RestoreBackup reconstructs a database from a stream produced by Backup, writing the main
// file at path and its -wal sidecar, then leaving them for Open to read (spec 18 §2). It
// refuses to overwrite an existing file at either path: a restore creates, it never
// clobbers, so a mistaken target fails loudly instead of destroying a live database. The
// restored pair is byte-identical to the source at the backup version, so opening it yields
// the same engine, format, and contents; an encrypted backup restores to an encrypted
// database that needs the original key. An f2 backup also restores its sidecar file, so the
// self-durable core opens from the same pair it was backed up from.
func RestoreBackup(fs vfs.FS, path string, r io.Reader) error {
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

	// A restore creates, it never clobbers, so refuse if any target file already exists. The
	// sidecar is only in play when the container carries one.
	walPath := path + walSuffix
	targets := []string{path, walPath}
	if hdr.sidecarLen > 0 {
		targets = append(targets, path+f2Suffix)
	}
	for _, p := range targets {
		exists, err := fs.Exists(p)
		if err != nil {
			return err
		}
		if exists {
			return fmt.Errorf("kv: refusing to restore over existing file %s", p)
		}
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
	if err := walFile.Close(); err != nil {
		return err
	}

	// The f2 sidecar, when the container carries one. Written last so a torn stream fails
	// before the self-durable file exists, leaving nothing half-formed for Open to find.
	if hdr.sidecarLen == 0 {
		return nil
	}
	sidecar, err := fs.Open(path+f2Suffix, vfs.OpenReadWrite|vfs.OpenCreate|vfs.OpenExclusive)
	if err != nil {
		return err
	}
	if err := streamInto(sidecar, r, int64(hdr.sidecarLen)); err != nil {
		sidecar.Close()
		return err
	}
	if err := sidecar.Sync(vfs.SyncFull); err != nil {
		sidecar.Close()
		return err
	}
	return sidecar.Close()
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
