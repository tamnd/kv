package format

import (
	"encoding/binary"
	"errors"
)

// HeaderSize is the fixed size of the database header at the start of page 1
// (spec 02 §2). SQLite uses 100 bytes; kv reserves 128 for headroom.
const HeaderSize = 128

// Magic is the 16-byte file signature: "kvdb format 1" NUL-padded. It identifies
// the file and the major format generation; a generation bump changes the
// trailing number and older code refuses a newer generation outright.
var Magic = [16]byte{'k', 'v', 'd', 'b', ' ', 'f', 'o', 'r', 'm', 'a', 't', ' ', '1', 0, 0, 0}

// EngineKind selects the storage core recorded at header offset 21. It is fixed
// at creation and cannot change in place (spec 04 §5).
type EngineKind byte

const (
	EngineBTree EngineKind = 1
	EngineLSM   EngineKind = 2
)

// String renders an EngineKind.
func (e EngineKind) String() string {
	switch e {
	case EngineBTree:
		return "btree"
	case EngineLSM:
		return "lsm"
	default:
		return "engine?"
	}
}

// Header flag bits (offset 22).
const (
	FlagWAL         = 1 << 0
	FlagEncryption  = 1 << 1
	FlagCompression = 1 << 2
)

// FormatLevel is the current read/write feature level a generation-1 writer
// stamps into a fresh file. A reader may open read-only when its supported level
// is >= the file's read level, and read-write when >= the file's write level
// (spec 02 §7).
const FormatLevel = 1

// WriterVersion is the informational KV_VERSION_NUMBER recorded at offset 96.
const WriterVersion = 1

// Header is the decoded 128-byte database header. Field names and order follow
// the offset table in spec 02 §2 exactly.
type Header struct {
	PageSize         int          // offset 16 (encoded form handles 65536)
	FormatWrite      byte         // offset 18
	FormatRead       byte         // offset 19
	ReservedPerPage  byte         // offset 20
	Engine           EngineKind   // offset 21
	Flags            byte         // offset 22
	Checksum         ChecksumAlgo // offset 23
	ChangeCounter    uint32       // offset 24
	DBSize           uint32       // offset 28 (pages; valid only if VersionValid==ChangeCounter)
	FreelistTrunk    PageNo       // offset 32
	FreelistPages    uint32       // offset 36
	MetaCookie       uint32       // offset 40
	EngineRoot       PageNo       // offset 44
	CacheSizeHint    uint32       // offset 48
	HighWaterMark    uint32       // offset 52
	Collation        uint32       // offset 56
	UserVersion      uint32       // offset 60
	ApplicationID    uint32       // offset 64
	CheckpointLSN    uint64       // offset 68
	OldestVersion    uint64       // offset 76
	_reserved84      uint32       // offset 84
	PageCountAtVac   uint32       // offset 88
	VersionValidFor  uint32       // offset 92
	WriterVersionNum uint32       // offset 96
	// LastCommitVersion is the highest MVCC commit version durably folded into the
	// file (offset 100). It persists the version counter across restart so a
	// reopened database never reissues a version <= one already on disk, which
	// would make a fresh write sort as older than an existing one (spec 10 §1). It
	// lives in the header's reserved headroom; an older reader that does not know
	// the field simply ignores it.
	LastCommitVersion uint64 // offset 100

	// AutoVacuumMode controls automatic space reclamation after checkpoints
	// (spec 22 §2, spec 09 §3.3). 0=NONE (default), 1=INCREMENTAL, 2=FULL.
	// INCREMENTAL and FULL both run TruncateTail after each checkpoint; the
	// distinction only matters once pointer-map pages are added in a future
	// milestone.
	AutoVacuumMode uint8 // offset 108

	// FullPageWritesOff disables full-page image logging during checkpoint
	// (spec 22 §2, spec 07 §5). 0=full-page writes ON (default, safe for all
	// storage); 1=OFF (only safe on storage that guarantees atomic page writes).
	// Inverted so the zero value is the safe default.
	FullPageWritesOff uint8 // offset 109

	// CommitLingerUs is the maximum microseconds the group-commit leader waits
	// for additional committers to join the group before flushing (spec 22 §2,
	// spec 07 §4). 0=adaptive (default; no explicit delay, batch whatever is
	// already queued). Persistent so the DBA can tune it without a code change.
	CommitLingerUs uint32 // offset 110
}

// Encode writes the header into the first 128 bytes of page, zeroing reserved
// ranges. page must be at least HeaderSize bytes.
func (h *Header) Encode(page []byte) {
	for i := 0; i < HeaderSize; i++ {
		page[i] = 0
	}
	copy(page[0:16], Magic[:])
	binary.BigEndian.PutUint16(page[16:18], EncodePageSize(h.PageSize))
	page[18] = h.FormatWrite
	page[19] = h.FormatRead
	page[20] = h.ReservedPerPage
	page[21] = byte(h.Engine)
	page[22] = h.Flags
	page[23] = byte(h.Checksum)
	binary.BigEndian.PutUint32(page[24:28], h.ChangeCounter)
	binary.BigEndian.PutUint32(page[28:32], h.DBSize)
	binary.BigEndian.PutUint32(page[32:36], h.FreelistTrunk)
	binary.BigEndian.PutUint32(page[36:40], h.FreelistPages)
	binary.BigEndian.PutUint32(page[40:44], h.MetaCookie)
	binary.BigEndian.PutUint32(page[44:48], h.EngineRoot)
	binary.BigEndian.PutUint32(page[48:52], h.CacheSizeHint)
	binary.BigEndian.PutUint32(page[52:56], h.HighWaterMark)
	binary.BigEndian.PutUint32(page[56:60], h.Collation)
	binary.BigEndian.PutUint32(page[60:64], h.UserVersion)
	binary.BigEndian.PutUint32(page[64:68], h.ApplicationID)
	binary.BigEndian.PutUint64(page[68:76], h.CheckpointLSN)
	binary.BigEndian.PutUint64(page[76:84], h.OldestVersion)
	binary.BigEndian.PutUint32(page[88:92], h.PageCountAtVac)
	binary.BigEndian.PutUint32(page[92:96], h.VersionValidFor)
	binary.BigEndian.PutUint32(page[96:100], h.WriterVersionNum)
	binary.BigEndian.PutUint64(page[100:108], h.LastCommitVersion)
	page[108] = h.AutoVacuumMode
	page[109] = h.FullPageWritesOff
	binary.BigEndian.PutUint32(page[110:114], h.CommitLingerUs)
}

// ErrBadMagic means the file does not begin with the kv magic string.
var ErrBadMagic = errors.New("kv/format: bad magic, not a kv database file")

// ErrBadPageSize means the header's page-size field is not a legal power of two.
var ErrBadPageSize = errors.New("kv/format: invalid page size in header")

// DecodeHeader parses the header from the start of page. It validates the magic
// and page size; higher layers validate the format levels against what they
// support (spec 02 §7).
func DecodeHeader(page []byte) (*Header, error) {
	if len(page) < HeaderSize {
		return nil, ErrBadMagic
	}
	var magic [16]byte
	copy(magic[:], page[0:16])
	if magic != Magic {
		return nil, ErrBadMagic
	}
	ps := DecodePageSize(binary.BigEndian.Uint16(page[16:18]))
	if !ValidPageSize(ps) {
		return nil, ErrBadPageSize
	}
	h := &Header{
		PageSize:          ps,
		FormatWrite:       page[18],
		FormatRead:        page[19],
		ReservedPerPage:   page[20],
		Engine:            EngineKind(page[21]),
		Flags:             page[22],
		Checksum:          ChecksumAlgo(page[23]),
		ChangeCounter:     binary.BigEndian.Uint32(page[24:28]),
		DBSize:            binary.BigEndian.Uint32(page[28:32]),
		FreelistTrunk:     binary.BigEndian.Uint32(page[32:36]),
		FreelistPages:     binary.BigEndian.Uint32(page[36:40]),
		MetaCookie:        binary.BigEndian.Uint32(page[40:44]),
		EngineRoot:        binary.BigEndian.Uint32(page[44:48]),
		CacheSizeHint:     binary.BigEndian.Uint32(page[48:52]),
		HighWaterMark:     binary.BigEndian.Uint32(page[52:56]),
		Collation:         binary.BigEndian.Uint32(page[56:60]),
		UserVersion:       binary.BigEndian.Uint32(page[60:64]),
		ApplicationID:     binary.BigEndian.Uint32(page[64:68]),
		CheckpointLSN:     binary.BigEndian.Uint64(page[68:76]),
		OldestVersion:     binary.BigEndian.Uint64(page[76:84]),
		PageCountAtVac:    binary.BigEndian.Uint32(page[88:92]),
		VersionValidFor:   binary.BigEndian.Uint32(page[92:96]),
		WriterVersionNum:  binary.BigEndian.Uint32(page[96:100]),
		LastCommitVersion: binary.BigEndian.Uint64(page[100:108]),
		AutoVacuumMode:    page[108],
		FullPageWritesOff: page[109],
		CommitLingerUs:    binary.BigEndian.Uint32(page[110:114]),
	}
	return h, nil
}

// UsablePageSize is the page size minus the reserved trailing bytes; this is the
// space cell structures and checksums actually use (spec 02 §2 offset 20).
func (h *Header) UsablePageSize() int {
	return h.PageSize - int(h.ReservedPerPage)
}

// SizeValid reports whether the DBSize and EngineRoot fields are trustworthy:
// they are valid only when VersionValidFor equals ChangeCounter (spec 02 §2 the
// validity trick). When invalid, the caller derives the size from the file length.
func (h *Header) SizeValid() bool {
	return h.VersionValidFor == h.ChangeCounter
}

// NewHeader builds a header for a freshly created database.
func NewHeader(pageSize int, engine EngineKind, flags byte, checksum ChecksumAlgo) *Header {
	return &Header{
		PageSize:         pageSize,
		FormatWrite:      FormatLevel,
		FormatRead:       FormatLevel,
		ReservedPerPage:  byte(checksum.ChecksumSize()),
		Engine:           engine,
		Flags:            flags,
		Checksum:         checksum,
		ChangeCounter:    1,
		DBSize:           1,
		EngineRoot:       NoPage,
		HighWaterMark:    1,
		VersionValidFor:  1,
		WriterVersionNum: WriterVersion,
	}
}
