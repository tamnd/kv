package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"time"

	"github.com/tamnd/kv"
)

// This file is the server side of the binary protocol: it accepts connections and serves a
// request/response loop on each. One goroutine per connection reads a request frame, dispatches
// it to the shared Service, and writes a response frame, then loops, so a client reuses one
// connection for many operations. The dispatch is the binary mirror of the HTTP handlers and
// calls the same Service methods, which is what keeps the two protocols semantically identical.

// ServeBinary serves the binary protocol on ln until the listener is closed or the server's
// base context is cancelled, accepting connections and handling each on its own goroutine. It
// is the binary analog of the HTTP Serve: a host that wants the efficient protocol runs this on
// a second listener alongside the HTTP one. It returns when accepting stops.
func (srv *Server) ServeBinary(ln net.Listener) error {
	// Close the listener when the server shuts down so a blocked Accept returns and this loop
	// exits, the same way http.Server.Shutdown unblocks its own Accept.
	stop := context.AfterFunc(srv.baseCtx, func() { ln.Close() })
	defer stop()

	for {
		conn, err := ln.Accept()
		if err != nil {
			// A closed listener (from shutdown) is the normal end, not a failure to report.
			if srv.baseCtx.Err() != nil {
				return nil
			}
			return err
		}
		go srv.serveBinaryConn(conn)
	}
}

// serveBinaryConn runs the request/response loop on one connection. It reads frames until the
// peer hangs up (io.EOF), a frame is malformed past recovery, or the server shuts down, then
// closes the connection. A per-request decode error is reported as a bad-request response and
// the loop continues, so one bad frame does not drop an otherwise healthy connection; a framing
// error (a length that cannot be read) is unrecoverable and ends the connection, since the
// stream position is then unknown.
func (srv *Server) serveBinaryConn(conn net.Conn) {
	defer conn.Close()
	// Cancelling the base context (shutdown) unblocks a connection parked in a blocking read
	// by closing it, so the per-connection goroutine does not pin a drain open.
	stop := context.AfterFunc(srv.baseCtx, func() { conn.Close() })
	defer stop()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	// One session per connection holds the identity an opAuth handshake binds, so a token is
	// presented once and every later operation on the connection is authorized against it. It
	// stays nil on an open server, where authorizeBinary allows everything.
	sess := &binarySession{}
	for {
		body, err := readFrame(r)
		if err != nil {
			return // EOF or a framing error: the connection is done.
		}
		// The streaming opcodes write many frames for one request, so they are handled before
		// the unary dispatch, which writes exactly one. A scan returns to the loop after its end
		// frame; a watch takes over the connection for its life, so the loop ends after it.
		if len(body) > 0 {
			switch opcode(body[0]) {
			case opScan:
				if err := srv.serveBinaryScan(w, body, sess); err != nil {
					return
				}
				continue
			case opWatch:
				srv.serveBinaryWatch(conn, w, body, sess)
				return
			}
		}
		resp := srv.dispatchBinary(body, sess)
		if err := writeFrame(w, resp); err != nil {
			return
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
}

// binarySession is the per-connection state of the binary protocol that outlives one request
// frame. ident is the caller an opAuth handshake authenticated; it is nil until the connection
// authenticates and stays nil for the life of an open server, where every operation is allowed.
type binarySession struct {
	ident *Identity
}

// authorizeBinary is the binary analog of the HTTP authorize gate. With auth disabled it allows
// everything. With auth on it runs the predicate against the identity bound to the connection by
// opAuth, returning ErrForbidden when the predicate denies and ErrUnauthenticated when the
// connection never authenticated. It calls the same Identity grant methods the HTTP path does, so
// a token authorizes the same operations on either wire.
func (srv *Server) authorizeBinary(sess *binarySession, allowed func(*Identity) bool) error {
	if srv.auth == nil {
		return nil
	}
	if sess.ident == nil {
		return ErrUnauthenticated
	}
	if !allowed(sess.ident) {
		return ErrForbidden
	}
	return nil
}

// binaryAuthed is the authorize predicate for an operation that any authenticated identity may
// perform regardless of its grants: opening, committing, or discarding an interactive transaction,
// none of which touch a key by itself (the per-key checks ride the transaction's own reads and
// writes). It exists so those cases read the same as the keyed ones.
func binaryAuthed(*Identity) bool { return true }

// dispatchBinary decodes one request body, performs the operation against the Service, and
// returns the encoded response body. A decode failure becomes a bad-request response rather
// than a dropped connection. The streaming opcodes (scan, watch) are not served here; they get
// their own framing in a later slice and a unary dispatch rejects them as bad requests.
func (srv *Server) dispatchBinary(body []byte, sess *binarySession) []byte {
	d := newDecoder(body)
	op := opcode(d.byte())
	if d.err != nil {
		return encodeError(statusBadRequest, "kv: empty request frame")
	}
	s := srv.svc

	switch op {
	case opAuth:
		// The authentication handshake. It is the one opcode that needs no prior authorization,
		// since it is what establishes the identity every later op is authorized against. On an
		// open server it is a no-op success with an empty identity name, so a client that always
		// authenticates works against an open server too.
		cred := d.bytes()
		if d.err != nil {
			return decodeErrResponse()
		}
		if srv.auth == nil {
			return encodeAuthResponse("")
		}
		id, ok := srv.auth.Authenticate(string(cred))
		if !ok {
			return encodeError(statusUnauthenticated, ErrUnauthenticated.Error())
		}
		sess.ident = id
		return encodeAuthResponse(id.Name)

	case opGet:
		key := d.bytes()
		if d.err != nil {
			return decodeErrResponse()
		}
		if err := srv.authorizeBinary(sess, func(id *Identity) bool { return id.canRead(key) }); err != nil {
			return errResponse(err)
		}
		v, found, err := s.Get(key)
		if err != nil {
			return errResponse(err)
		}
		return encodeReadResponse(found, v)

	case opExists:
		key := d.bytes()
		if d.err != nil {
			return decodeErrResponse()
		}
		if err := srv.authorizeBinary(sess, func(id *Identity) bool { return id.canRead(key) }); err != nil {
			return errResponse(err)
		}
		found, err := s.Exists(key)
		if err != nil {
			return errResponse(err)
		}
		return encodeReadResponse(found, nil)

	case opSet:
		key := d.bytes()
		value := d.bytes()
		ttl := time.Duration(d.uint64()) * time.Millisecond
		if d.err != nil {
			return decodeErrResponse()
		}
		if err := srv.authorizeBinary(sess, func(id *Identity) bool { return id.canWrite(key) }); err != nil {
			return errResponse(err)
		}
		version, err := s.Set(key, value, ttl)
		if err != nil {
			return errResponse(err)
		}
		return encodeVersionResponse(version)

	case opDelete:
		key := d.bytes()
		if d.err != nil {
			return decodeErrResponse()
		}
		if err := srv.authorizeBinary(sess, func(id *Identity) bool { return id.canWrite(key) }); err != nil {
			return errResponse(err)
		}
		version, err := s.Delete(key)
		if err != nil {
			return errResponse(err)
		}
		return encodeVersionResponse(version)

	case opDeleteRange:
		lo := d.bytes()
		hi := d.bytes()
		if d.err != nil {
			return decodeErrResponse()
		}
		if err := srv.authorizeBinary(sess, func(id *Identity) bool { return id.canWriteRange(lo, hi) }); err != nil {
			return errResponse(err)
		}
		version, err := s.DeleteRange(lo, hi)
		if err != nil {
			return errResponse(err)
		}
		return encodeVersionResponse(version)

	case opMerge:
		key := d.bytes()
		operand := d.bytes()
		if d.err != nil {
			return decodeErrResponse()
		}
		if err := srv.authorizeBinary(sess, func(id *Identity) bool { return id.canWrite(key) }); err != nil {
			return errResponse(err)
		}
		version, err := s.Merge(key, operand)
		if err != nil {
			return errResponse(err)
		}
		return encodeVersionResponse(version)

	case opTxn:
		req, ok := decodeTxnRequest(d)
		if !ok {
			return decodeErrResponse()
		}
		// The whole transaction is authorized before any of it applies, so a mixed-grant
		// transaction is refused as a unit and never commits the part it was allowed.
		if err := srv.authorizeBinary(sess, func(id *Identity) bool { return id.canDoTxn(req.Asserts, req.Ops) }); err != nil {
			return errResponse(err)
		}
		res, err := s.Txn(req)
		if err != nil {
			return errResponse(err)
		}
		return encodeTxnResponse(res)

	case opBatch:
		ops, ok := decodeOpList(d)
		if !ok {
			return decodeErrResponse()
		}
		if err := srv.authorizeBinary(sess, func(id *Identity) bool { return id.canDoTxn(nil, ops) }); err != nil {
			return errResponse(err)
		}
		version, err := s.Batch(ops)
		if err != nil {
			return errResponse(err)
		}
		return encodeVersionResponse(version)

	case opStats:
		if err := srv.authorizeBinary(sess, isAdmin); err != nil {
			return errResponse(err)
		}
		return encodeStatsResponse(s.Stats())

	case opCheckpoint:
		if err := srv.authorizeBinary(sess, isAdmin); err != nil {
			return errResponse(err)
		}
		if err := s.Checkpoint(); err != nil {
			return errResponse(err)
		}
		return []byte{byte(statusOK)}

	case opCompact:
		budget := int(d.uint64())
		if d.err != nil {
			return decodeErrResponse()
		}
		if err := srv.authorizeBinary(sess, isAdmin); err != nil {
			return errResponse(err)
		}
		reclaimed, err := s.Compact(budget)
		if err != nil {
			return errResponse(err)
		}
		e := encoder{buf: []byte{byte(statusOK)}}
		e.uint64(uint64(reclaimed))
		return e.buf

	case opBeginTxn:
		writable := d.byte() == 1
		if d.err != nil {
			return decodeErrResponse()
		}
		// Opening a transaction touches no key, so any authenticated identity may; its reads and
		// writes are authorized op by op as they arrive.
		if err := srv.authorizeBinary(sess, binaryAuthed); err != nil {
			return errResponse(err)
		}
		id, err := s.BeginTxn(writable)
		if err != nil {
			return errResponse(err)
		}
		return encodeVersionResponse(id) // the id rides the uint64 response shape

	case opTxnGet:
		id := d.uint64()
		key := d.bytes()
		if d.err != nil {
			return decodeErrResponse()
		}
		if err := srv.authorizeBinary(sess, func(id *Identity) bool { return id.canRead(key) }); err != nil {
			return errResponse(err)
		}
		v, found, err := s.TxnGet(id, key)
		if err != nil {
			return errResponse(err)
		}
		return encodeReadResponse(found, v)

	case opTxnExists:
		id := d.uint64()
		key := d.bytes()
		if d.err != nil {
			return decodeErrResponse()
		}
		if err := srv.authorizeBinary(sess, func(id *Identity) bool { return id.canRead(key) }); err != nil {
			return errResponse(err)
		}
		found, err := s.TxnExists(id, key)
		if err != nil {
			return errResponse(err)
		}
		return encodeReadResponse(found, nil)

	case opTxnSet:
		id := d.uint64()
		key := d.bytes()
		value := d.bytes()
		ttl := time.Duration(d.uint64()) * time.Millisecond
		if d.err != nil {
			return decodeErrResponse()
		}
		if err := srv.authorizeBinary(sess, func(id *Identity) bool { return id.canWrite(key) }); err != nil {
			return errResponse(err)
		}
		if err := s.TxnSet(id, key, value, ttl); err != nil {
			return errResponse(err)
		}
		return []byte{byte(statusOK)}

	case opTxnDelete:
		id := d.uint64()
		key := d.bytes()
		if d.err != nil {
			return decodeErrResponse()
		}
		if err := srv.authorizeBinary(sess, func(id *Identity) bool { return id.canWrite(key) }); err != nil {
			return errResponse(err)
		}
		if err := s.TxnDelete(id, key); err != nil {
			return errResponse(err)
		}
		return []byte{byte(statusOK)}

	case opTxnDeleteRange:
		id := d.uint64()
		lo := d.bytes()
		hi := d.bytes()
		if d.err != nil {
			return decodeErrResponse()
		}
		if err := srv.authorizeBinary(sess, func(id *Identity) bool { return id.canWriteRange(lo, hi) }); err != nil {
			return errResponse(err)
		}
		if err := s.TxnDeleteRange(id, lo, hi); err != nil {
			return errResponse(err)
		}
		return []byte{byte(statusOK)}

	case opTxnMerge:
		id := d.uint64()
		key := d.bytes()
		operand := d.bytes()
		if d.err != nil {
			return decodeErrResponse()
		}
		if err := srv.authorizeBinary(sess, func(id *Identity) bool { return id.canWrite(key) }); err != nil {
			return errResponse(err)
		}
		if err := s.TxnMerge(id, key, operand); err != nil {
			return errResponse(err)
		}
		return []byte{byte(statusOK)}

	case opTxnCommit:
		id := d.uint64()
		if d.err != nil {
			return decodeErrResponse()
		}
		if err := srv.authorizeBinary(sess, binaryAuthed); err != nil {
			return errResponse(err)
		}
		version, err := s.CommitTxn(id)
		if err != nil {
			return errResponse(err)
		}
		return encodeVersionResponse(version)

	case opTxnDiscard:
		id := d.uint64()
		if d.err != nil {
			return decodeErrResponse()
		}
		if err := srv.authorizeBinary(sess, binaryAuthed); err != nil {
			return errResponse(err)
		}
		if err := s.DiscardTxn(id); err != nil {
			return errResponse(err)
		}
		return []byte{byte(statusOK)}

	default:
		return encodeError(statusBadRequest, "kv: unknown opcode")
	}
}

// encodeAuthResponse encodes a successful authentication: the OK status then the identity's name,
// which the client returns so a caller can confirm which identity its token resolved to. The name
// is empty when the server runs open and there is no identity to bind.
func encodeAuthResponse(name string) []byte {
	e := encoder{buf: []byte{byte(statusOK)}}
	e.bytes([]byte(name))
	return e.buf
}

// decodeTxnRequest decodes the asserts and ops of a transaction request. An assert carries an
// explicit presence flag (expect-absent) separate from its expected value, since the byte
// encoding cannot distinguish an empty expected value from an absent one.
func decodeTxnRequest(d *decoder) (TxnRequest, bool) {
	var req TxnRequest
	nAsserts := d.uint64()
	for i := uint64(0); i < nAsserts; i++ {
		key := d.bytes()
		expectAbsent := d.byte() == 1
		expectValue := d.bytes()
		req.Asserts = append(req.Asserts, Assert{Key: key, ExpectValue: expectValue, ExpectAbsent: expectAbsent})
		if d.err != nil {
			return TxnRequest{}, false
		}
	}
	ops, ok := decodeOpList(d)
	if !ok {
		return TxnRequest{}, false
	}
	req.Ops = ops
	return req, true
}

// decodeOpList decodes a count-prefixed list of ops, the shared payload of a batch and a
// transaction's write set.
func decodeOpList(d *decoder) ([]Op, bool) {
	n := d.uint64()
	if d.err != nil {
		return nil, false
	}
	ops := make([]Op, 0, n)
	for i := uint64(0); i < n; i++ {
		kind := d.byte()
		key := d.bytes()
		value := d.bytes()
		lo := d.bytes()
		hi := d.bytes()
		ttl := time.Duration(d.uint64()) * time.Millisecond
		if d.err != nil {
			return nil, false
		}
		ops = append(ops, Op{Kind: opKindFromByte(kind), Key: key, Value: value, Lo: lo, Hi: hi, TTL: ttl})
	}
	return ops, true
}

// encodeReadResponse encodes a get or exists result: a found flag and, when found and a value
// was requested, the value bytes.
func encodeReadResponse(found bool, value []byte) []byte {
	e := encoder{buf: []byte{byte(statusOK)}}
	if found {
		e.byte(1)
		e.bytes(value)
	} else {
		e.byte(0)
	}
	return e.buf
}

// encodeVersionResponse encodes a write result: the commit version.
func encodeVersionResponse(version uint64) []byte {
	e := encoder{buf: []byte{byte(statusOK)}}
	e.uint64(version)
	return e.buf
}

// encodeTxnResponse encodes a transaction result: the commit version and the ordered reads.
func encodeTxnResponse(res TxnResult) []byte {
	e := encoder{buf: []byte{byte(statusOK)}}
	e.uint64(res.Version)
	e.uint64(uint64(len(res.Reads)))
	for _, r := range res.Reads {
		if r.Found {
			e.byte(1)
			e.bytes(r.Value)
		} else {
			e.byte(0)
		}
	}
	return e.buf
}

// encodeStatsResponse encodes the stats as a JSON blob, reusing the one rendering of Stats
// rather than re-laying it out in binary: stats is an occasional ops call, not a hot path, so
// the convenience of one shared shape outweighs the bytes.
func encodeStatsResponse(stats kv.Stats) []byte {
	e := encoder{buf: []byte{byte(statusOK)}}
	e.bytes(mustJSON(stats))
	return e.buf
}

// errResponse encodes a typed error into a classified status and its message.
func errResponse(err error) []byte {
	return encodeError(statusForError(err), err.Error())
}

// decodeErrResponse is the response for a request frame that did not decode.
func decodeErrResponse() []byte {
	return encodeError(statusBadRequest, "kv: malformed request frame")
}

// encodeError encodes a non-OK response: the status byte then a length-prefixed message.
func encodeError(s status, msg string) []byte {
	e := encoder{buf: []byte{byte(s)}}
	e.bytes([]byte(msg))
	return e.buf
}

// opKindFromByte maps a wire op-kind byte to an OpKind. An unknown byte yields an empty kind,
// which the Service's applyOp rejects as an invalid op, so a bad kind fails the request rather
// than silently doing nothing.
func opKindFromByte(b byte) OpKind {
	switch b {
	case 1:
		return OpGet
	case 2:
		return OpExists
	case 3:
		return OpSet
	case 4:
		return OpDelete
	case 5:
		return OpDeleteRange
	case 6:
		return OpMerge
	default:
		return OpKind("")
	}
}

// opKindToByte is the inverse, used by the client encoder.
func opKindToByte(k OpKind) byte {
	switch k {
	case OpGet:
		return 1
	case OpExists:
		return 2
	case OpSet:
		return 3
	case OpDelete:
		return 4
	case OpDeleteRange:
		return 5
	case OpMerge:
		return 6
	default:
		return 0
	}
}

// errBinaryStatus reconstructs an error from a response status and message, the client-side
// inverse of statusForError: it maps a class back onto the exported sentinel so a binary client
// can errors.Is the result against kv.ErrNotFound and friends exactly as a library caller does.
func errBinaryStatus(s status, msg string) error {
	switch s {
	case statusNotFound:
		return wrapBinary(kv.ErrNotFound, msg)
	case statusConflict:
		return wrapBinary(kv.ErrConflict, msg)
	case statusReadOnly:
		return wrapBinary(kv.ErrReadOnly, msg)
	case statusUnavail:
		return wrapBinary(kv.ErrNeedsRecovery, msg)
	case statusNoTxn:
		return wrapBinary(ErrNoSuchTxn, msg)
	case statusTooLarge:
		return wrapBinary(ErrLimitExceeded, msg)
	case statusUnauthenticated:
		return wrapBinary(ErrUnauthenticated, msg)
	case statusForbidden:
		return wrapBinary(ErrForbidden, msg)
	default:
		return errors.New(msg)
	}
}

// wrapBinary pairs a sentinel with the server's message so the client error both matches the
// sentinel under errors.Is and carries the original text.
func wrapBinary(sentinel error, msg string) error {
	return &binaryError{sentinel: sentinel, msg: msg}
}

type binaryError struct {
	sentinel error
	msg      string
}

func (e *binaryError) Error() string { return e.msg }
func (e *binaryError) Unwrap() error { return e.sentinel }

// mustJSON marshals a value that is known not to fail (a Stats struct of plain scalars). A
// marshal error here would be a programming fault, not a runtime condition, so it yields an
// empty object rather than propagating an impossible error up the wire path.
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}

// unmarshalJSON decodes the stats blob on the client side, the inverse of mustJSON. It lives
// here next to its partner so the one JSON dependency of the binary protocol stays in one file.
func unmarshalJSON(b []byte, v any) error {
	return json.Unmarshal(b, v)
}
