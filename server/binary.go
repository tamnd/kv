package server

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/tamnd/kv"
)

// This file defines the wire format of the pure-Go binary protocol, the efficient alternative
// to HTTP/JSON (spec 17). The spec calls for a second, streaming-friendly protocol in the role
// gRPC plays elsewhere; the zero-dependency rule rules out gRPC (it would pull in protobuf and
// the grpc runtime), so the protocol is hand-rolled length-prefixed framing over a raw TCP
// stream. It carries the same operation set as the HTTP surface, decoded by the same Service,
// so the two protocols cannot diverge in what an operation means, only in how the bytes are
// laid out. Bytes move raw here, with no base64 and no JSON, which is the efficiency the binary
// protocol exists to provide: a key or value is a length-prefixed run of bytes, not a
// quoted-and-escaped string.
//
// A message is a 4-byte big-endian length prefix followed by that many body bytes. A request
// body is a one-byte opcode then the operation's fields; a response body is a one-byte status
// then, on success, the result fields, or on failure a length-prefixed error message. Framing
// every message with its length is what lets the reader own exactly one allocation per message
// and what makes the streaming opcodes (scan, watch, in a later slice) able to interleave many
// result frames between two request frames on the same connection.

// opcode names a request operation on the binary wire. The values are explicit and frozen:
// the wire is a compatibility surface, so an opcode's number never changes.
type opcode byte

const (
	opGet         opcode = 1
	opExists      opcode = 2
	opSet         opcode = 3
	opDelete      opcode = 4
	opDeleteRange opcode = 5
	opMerge       opcode = 6
	opTxn         opcode = 7
	opBatch       opcode = 8
	opStats       opcode = 9
	opCheckpoint  opcode = 10
	opCompact     opcode = 11
	opScan        opcode = 12
	opWatch       opcode = 13

	opBeginTxn       opcode = 14
	opTxnGet         opcode = 15
	opTxnExists      opcode = 16
	opTxnSet         opcode = 17
	opTxnDelete      opcode = 18
	opTxnDeleteRange opcode = 19
	opTxnMerge       opcode = 20
	opTxnCommit      opcode = 21
	opTxnDiscard     opcode = 22

	opAuth opcode = 23 // authenticate the connection, binding an identity for later ops
)

// status is the first byte of a response body. statusOK means the result fields follow; any
// other value means a length-prefixed error message follows, and the value classifies the
// error the same way the HTTP adapter's status codes do, so a binary client maps a failure to
// the same typed error a library caller would see.
type status byte

const (
	statusOK         status = 0
	statusNotFound   status = 1 // a key was absent where the op required it
	statusConflict   status = 2 // write-write conflict or a failed transaction assertion
	statusReadOnly   status = 3 // a write on a read-only database
	statusUnavail    status = 4 // fenced-for-recovery, corrupt, or closed database
	statusBadRequest status = 5 // a malformed frame or argument
	statusInternal   status = 6 // any other failure
	statusNoTxn      status = 7 // an interactive transaction id the server does not hold
	statusTooLarge   status = 8 // a request past a configured size limit

	statusUnauthenticated status = 9  // a missing or unrecognized credential
	statusForbidden       status = 10 // a recognized identity without a grant for the key
)

// maxFrameSize caps a single message body so a corrupt or hostile length prefix cannot make
// the reader allocate an unbounded buffer. It is generous enough for a large value or a big
// batch while still bounding a single read. Larger payloads belong in a streaming op, not one
// frame.
const maxFrameSize = 64 << 20 // 64 MiB

// errFrameTooLarge is returned when a length prefix exceeds maxFrameSize.
var errFrameTooLarge = errors.New("kv: binary frame exceeds maximum size")

// writeFrame writes a length-prefixed message body to w. It is the one place a frame's length
// is computed, so a reader and a writer can never disagree about where a message ends.
func writeFrame(w io.Writer, body []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}

// readFrame reads one length-prefixed message body from r, returning it as a freshly allocated
// slice the caller owns. A length past maxFrameSize is refused before any allocation, so a bad
// prefix costs nothing.
func readFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > maxFrameSize {
		return nil, errFrameTooLarge
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return body, nil
}

// encoder builds a message body field by field into a growing buffer. The field encoding is
// deliberately minimal: a byte, a big-endian uint64, or a length-prefixed byte run, which is
// all the operation set needs.
type encoder struct {
	buf []byte
}

func (e *encoder) byte(b byte) {
	e.buf = append(e.buf, b)
}

func (e *encoder) uint64(v uint64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], v)
	e.buf = append(e.buf, tmp[:]...)
}

// bytes writes a 4-byte length then the bytes. A nil and an empty slice both encode as a zero
// length, so the decoder cannot tell them apart; callers that must distinguish absent from
// empty carry a separate presence flag, as the transaction asserts do.
func (e *encoder) bytes(p []byte) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(len(p)))
	e.buf = append(e.buf, tmp[:]...)
	e.buf = append(e.buf, p...)
}

// decoder reads a message body field by field, tracking a position and a sticky error so a
// caller can decode a whole message and check once at the end rather than after every field.
// A truncated message trips err on the first short read and every later read is a no-op.
type decoder struct {
	buf []byte
	pos int
	err error
}

func newDecoder(body []byte) *decoder { return &decoder{buf: body} }

func (d *decoder) byte() byte {
	if d.err != nil {
		return 0
	}
	if d.pos+1 > len(d.buf) {
		d.err = io.ErrUnexpectedEOF
		return 0
	}
	b := d.buf[d.pos]
	d.pos++
	return b
}

func (d *decoder) uint64() uint64 {
	if d.err != nil {
		return 0
	}
	if d.pos+8 > len(d.buf) {
		d.err = io.ErrUnexpectedEOF
		return 0
	}
	v := binary.BigEndian.Uint64(d.buf[d.pos : d.pos+8])
	d.pos += 8
	return v
}

// bytes reads a length-prefixed field and returns a copy, so the returned slice does not alias
// the frame buffer and is safe to retain past the frame's life.
func (d *decoder) bytes() []byte {
	if d.err != nil {
		return nil
	}
	if d.pos+4 > len(d.buf) {
		d.err = io.ErrUnexpectedEOF
		return nil
	}
	n := int(binary.BigEndian.Uint32(d.buf[d.pos : d.pos+4]))
	d.pos += 4
	if n < 0 || d.pos+n > len(d.buf) {
		d.err = io.ErrUnexpectedEOF
		return nil
	}
	out := make([]byte, n)
	copy(out, d.buf[d.pos:d.pos+n])
	d.pos += n
	return out
}

// statusForError classifies a Service or library error into a wire status, the binary analog
// of the HTTP adapter's writeServiceErr. The two share intent: the same typed error becomes
// the same class on either protocol, so a client gets a consistent signal regardless of which
// it speaks.
func statusForError(err error) status {
	switch {
	case err == nil:
		return statusOK
	case errors.Is(err, kv.ErrNotFound):
		return statusNotFound
	case errors.Is(err, kv.ErrConflict), errors.Is(err, ErrAssertFailed):
		return statusConflict
	case errors.Is(err, kv.ErrReadOnly):
		return statusReadOnly
	case errors.Is(err, ErrNoSuchTxn):
		return statusNoTxn
	case errors.Is(err, ErrLimitExceeded):
		return statusTooLarge
	case errors.Is(err, ErrUnauthenticated):
		return statusUnauthenticated
	case errors.Is(err, ErrForbidden):
		return statusForbidden
	case errors.Is(err, ErrTooManyTxns), errors.Is(err, kv.ErrNeedsRecovery), errors.Is(err, kv.ErrCorrupt), errors.Is(err, kv.ErrClosed):
		return statusUnavail
	default:
		return statusInternal
	}
}
