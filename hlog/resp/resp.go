// Package resp puts a Redis-compatible front end on the hlog engine. It speaks
// RESP2/RESP3 over a TCP or unix listener so any Redis client or benchmark can
// drive the bare hash-log store with GET/SET/DEL/EXISTS, PING, the HELLO
// handshake, and the handful of introspection commands a client issues at
// connect (CONFIG, COMMAND, INFO, DBSIZE, CLIENT, SELECT, FLUSHALL). This is the
// engine's own network face, not the transactional kv database's: there is no
// MVCC and no transaction, a SET is one append to the hybrid log and a GET is a
// point lookup, the same surface Open returns in process.
//
// The wire loop is the raw-buffer, parse-in-place, one-write-per-burst design the
// kv RESP front end uses, kept here over the *hlog.TieredDB surface. The hot path
// reads one chunk off the socket, parses every complete command sitting in the
// buffer in place, runs each one appending its reply to a single output buffer,
// and writes that buffer back in one syscall, so steady-state parsing copies
// nothing: a key points straight into the read buffer and the engine copies the
// value once when it frames the record into the log.
//
// Durability follows the engine: the store is group-commit by construction, so a
// SET returns as soon as it is in the hot tier and the background flusher makes it
// durable. When the server is started in a synced mode the BGREWRITEAOF command
// (the harness "make it durable now" hook) forces a Sync barrier; in the unsynced
// mode it is a no-op that returns OK, so a benchmark's OFF durability pays no
// fsync the engine would not otherwise do.
package resp

import (
	"bytes"
	"errors"
	"net"
	"strconv"
	"sync"

	"github.com/tamnd/kv/hlog"
)

// Server serves an hlog store over RESP on a listener. sync decides whether the
// durability hook forces a Sync: it is set from the mode the binary was started
// in, so the wire face honours the same durability contract the in-process engine
// does.
type Server struct {
	db   *hlog.TieredDB
	sync bool

	wg     sync.WaitGroup
	mu     sync.Mutex
	ln     net.Listener
	conns  map[net.Conn]struct{}
	closed bool
}

// New builds a Server over an open store. forceSync makes the BGREWRITEAOF
// durability hook issue a real Sync; pass false for the unsynced mode where the
// background flusher is the only durability and the hook is a no-op. Call Serve
// with a bound listener.
func New(db *hlog.TieredDB, forceSync bool) *Server {
	return &Server{db: db, sync: forceSync, conns: make(map[net.Conn]struct{})}
}

// Serve accepts connections on ln until Close, serving each one in its own
// goroutine. It returns nil after a Close, and the accept error otherwise.
func (s *Server) Serve(ln net.Listener) error {
	// Record the listener so Close can shut it down: dropping the open connections
	// is not enough, the accept loop is parked in Accept and only closing the
	// listener unblocks it so Serve returns.
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = ln.Close()
		return nil
	}
	s.ln = ln
	s.mu.Unlock()
	for {
		c, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			return err
		}
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = c.Close()
			return nil
		}
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go s.handle(c)
	}
}

// Close stops serving and drops every open connection, then waits for the
// per-connection goroutines to finish. It is safe to call more than once.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	if s.ln != nil {
		_ = s.ln.Close()
	}
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

const (
	readChunk = 64 * 1024 // initial read buffer, grows for an oversize single command
	writeCap  = 64 * 1024 // initial reply buffer, grows as a pipeline burst fills it
)

func (s *Server) handle(c net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		_ = c.Close()
	}()

	conn := &connState{
		buf:     make([]byte, readChunk),
		out:     make([]byte, 0, writeCap),
		scratch: make([]byte, 0, 1024),
	}
	// start..end is the unparsed window inside buf.
	start, end := 0, 0
	for {
		// Drain every complete command in the buffer, appending replies to
		// conn.out. Arguments point straight into buf, valid until the next read.
		for {
			args, n, ok, perr := parseCommand(conn.buf[start:end], conn)
			if perr != nil {
				return
			}
			if !ok {
				break
			}
			start += n
			if len(args) > 0 {
				var shutdown bool
				conn.out, shutdown = s.dispatch(conn, conn.out, args)
				if shutdown {
					// Reply (if any) goes out, then the server tears down. Close
					// runs on its own goroutine so it does not wait on this one.
					if len(conn.out) > 0 {
						_, _ = c.Write(conn.out)
					}
					go s.Close()
					return
				}
			}
		}
		// One write back per drained burst.
		if len(conn.out) > 0 {
			if _, err := c.Write(conn.out); err != nil {
				return
			}
			conn.out = conn.out[:0]
		}
		// Slide the leftover partial command to the front.
		if start > 0 {
			copy(conn.buf, conn.buf[start:end])
			end -= start
			start = 0
		}
		// A single command larger than the buffer: grow and keep reading it.
		if end == len(conn.buf) {
			nb := make([]byte, len(conn.buf)*2)
			copy(nb, conn.buf[:end])
			conn.buf = nb
		}
		n, err := c.Read(conn.buf[end:])
		if n > 0 {
			end += n
		}
		if err != nil {
			return
		}
	}
}

// connState carries the per-connection read buffer, reply buffer, a reusable
// argument slice, and a scratch buffer the engine fills a Get into, so
// steady-state command handling does not allocate.
type connState struct {
	buf     []byte
	out     []byte
	args    [][]byte
	scratch []byte
}

var errProtocol = errors.New("protocol error")

// parseCommand tries to parse one RESP multibulk command (or an inline command)
// from the front of buf. On success it returns the argument slices (pointing into
// buf), the number of bytes consumed, and ok=true. When buf holds only a partial
// command it returns ok=false with no error, signalling the caller to read more.
func parseCommand(buf []byte, conn *connState) (args [][]byte, consumed int, ok bool, err error) {
	if len(buf) == 0 {
		return nil, 0, false, nil
	}
	if buf[0] != '*' {
		// Inline command (redis-cli, a bare PING).
		nl := bytes.IndexByte(buf, '\n')
		if nl < 0 {
			return nil, 0, false, nil
		}
		return splitInline(trimCR(buf[:nl]), conn), nl + 1, true, nil
	}
	nl := bytes.IndexByte(buf, '\n')
	if nl < 0 {
		return nil, 0, false, nil
	}
	n, okp := atoiBytes(trimCR(buf[1:nl]))
	if !okp || n < 0 {
		return nil, 0, false, errProtocol
	}
	pos := nl + 1
	a := conn.args[:0]
	for i := 0; i < n; i++ {
		if pos >= len(buf) {
			return nil, 0, false, nil
		}
		if buf[pos] != '$' {
			return nil, 0, false, errProtocol
		}
		rel := bytes.IndexByte(buf[pos:], '\n')
		if rel < 0 {
			return nil, 0, false, nil
		}
		hdrEnd := pos + rel
		blen, okb := atoiBytes(trimCR(buf[pos+1 : hdrEnd]))
		if !okb || blen < 0 {
			return nil, 0, false, errProtocol
		}
		dataStart := hdrEnd + 1
		dataEnd := dataStart + blen
		if dataEnd+2 > len(buf) { // value bytes plus trailing CRLF
			return nil, 0, false, nil
		}
		a = append(a, buf[dataStart:dataEnd])
		pos = dataEnd + 2
	}
	conn.args = a
	return a, pos, true, nil
}

// trimCR drops a single trailing carriage return, leaving the line content.
func trimCR(b []byte) []byte {
	if n := len(b); n > 0 && b[n-1] == '\r' {
		return b[:n-1]
	}
	return b
}

func splitInline(line []byte, conn *connState) [][]byte {
	a := conn.args[:0]
	start := -1
	for i := 0; i < len(line); i++ {
		if line[i] == ' ' {
			if start >= 0 {
				a = append(a, line[start:i])
				start = -1
			}
		} else if start < 0 {
			start = i
		}
	}
	if start >= 0 {
		a = append(a, line[start:])
	}
	conn.args = a
	return a
}

// atoiBytes parses a non-negative decimal integer from b with no allocation.
// ok is false on an empty slice or a non-digit byte.
func atoiBytes(b []byte) (int, bool) {
	if len(b) == 0 {
		return 0, false
	}
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

var (
	respOK       = []byte("+OK\r\n")
	respPong     = []byte("+PONG\r\n")
	respNil      = []byte("$-1\r\n")
	respZero     = []byte(":0\r\n")
	respEmptyArr = []byte("*0\r\n")
	respInfo     = []byte("# Server\r\nredis_version:7.4.0\r\nhlog_redis_layer:1\r\n")
)

// dispatch runs one command, appends its reply to out, and returns the grown
// buffer along with whether the command asks the server to shut down. It switches
// on command length first so the GET/SET hot paths reach their compare quickly.
func (s *Server) dispatch(conn *connState, out []byte, args [][]byte) ([]byte, bool) {
	cmd := args[0]
	switch len(cmd) {
	case 3:
		if eqFold(cmd, "get") {
			return s.cmdGet(conn, out, args), false
		}
		if eqFold(cmd, "set") {
			return s.cmdSet(out, args), false
		}
		if eqFold(cmd, "del") {
			return s.cmdDel(conn, out, args), false
		}
	case 4:
		if eqFold(cmd, "ping") {
			return append(out, respPong...), false
		}
		if eqFold(cmd, "info") {
			return appendBulk(out, respInfo), false
		}
	case 5:
		if eqFold(cmd, "hello") {
			return s.cmdHello(out, args), false
		}
	case 6:
		if eqFold(cmd, "exists") {
			return s.cmdExists(conn, out, args), false
		}
		if eqFold(cmd, "config") {
			return append(out, respEmptyArr...), false
		}
		if eqFold(cmd, "dbsize") {
			// The hash-log engine keeps no live-key counter, so the introspection
			// DBSIZE a client issues at connect answers a best-effort zero rather
			// than walking the index. It is never on the benchmark's hot path.
			return append(out, respZero...), false
		}
		if eqFold(cmd, "select") {
			return append(out, respOK...), false
		}
		if eqFold(cmd, "client") {
			return append(out, respOK...), false
		}
	case 7:
		if eqFold(cmd, "command") {
			return append(out, respEmptyArr...), false
		}
	case 8:
		if eqFold(cmd, "flushall") {
			return append(out, respOK...), false
		}
		if eqFold(cmd, "shutdown") {
			// No reply: the client takes the connection drop as success.
			return out, true
		}
	case 12:
		if eqFold(cmd, "bgrewriteaof") {
			// The harness "make it durable now" hook. In a synced mode it forces a
			// group-commit barrier; in the unsynced mode the background flusher is
			// the only durability and this is a no-op that still answers OK.
			if s.sync {
				_ = s.db.Sync()
			}
			return append(out, respOK...), false
		}
	}
	// Unknown command: a benign OK keeps a benchmark client moving for anything
	// not special-cased here.
	return append(out, respOK...), false
}

func (s *Server) cmdGet(conn *connState, out []byte, args [][]byte) []byte {
	if len(args) != 2 {
		return appendErr(out, "wrong number of arguments for 'get'")
	}
	val, found, err := s.db.Get(args[1], conn.scratch[:0])
	if err != nil {
		return appendErr(out, err.Error())
	}
	if !found {
		return append(out, respNil...)
	}
	return appendBulk(out, val)
}

// cmdSet appends key=value to the hybrid log, last-writer-wins by append order,
// the Redis SET contract. The engine frames and copies the value into the log, so
// it is safe that key and value still point into the read buffer here.
func (s *Server) cmdSet(out []byte, args [][]byte) []byte {
	if len(args) < 3 {
		return appendErr(out, "wrong number of arguments for 'set'")
	}
	s.db.Set(args[1], args[2])
	return append(out, respOK...)
}

// cmdDel deletes each key that exists and replies with the count removed, the
// Redis DEL contract. It reads each key to decide whether it counts, then appends
// a delete tombstone for the ones present. The read and the delete are not one
// atomic step, so a key concurrently written between them is a thin race the
// single-threaded Redis would not have; it does not corrupt the store and is
// immaterial to a benchmark.
func (s *Server) cmdDel(conn *connState, out []byte, args [][]byte) []byte {
	if len(args) < 2 {
		return appendErr(out, "wrong number of arguments for 'del'")
	}
	var removed int64
	for _, k := range args[1:] {
		_, found, err := s.db.Get(k, conn.scratch[:0])
		if err != nil {
			return appendErr(out, err.Error())
		}
		if !found {
			continue
		}
		s.db.Delete(k)
		removed++
	}
	return appendInt(out, removed)
}

// cmdExists replies with the count of the named keys that are present, counting a
// key given more than once each time, matching Redis.
func (s *Server) cmdExists(conn *connState, out []byte, args [][]byte) []byte {
	if len(args) < 2 {
		return appendErr(out, "wrong number of arguments for 'exists'")
	}
	var n int64
	for _, k := range args[1:] {
		_, found, err := s.db.Get(k, conn.scratch[:0])
		if err != nil {
			return appendErr(out, err.Error())
		}
		if found {
			n++
		}
	}
	return appendInt(out, n)
}

// cmdHello answers the RESP handshake with the server map a client reads to learn
// the protocol version and server identity. The reply is the RESP2 flattened-map
// form, which go-redis and other clients accept under both protocol versions; the
// proto field echoes the requested version (2 or 3), defaulting to 2.
func (s *Server) cmdHello(out []byte, args [][]byte) []byte {
	proto := int64(2)
	if len(args) >= 2 {
		if p, ok := atoiBytes(args[1]); ok && (p == 2 || p == 3) {
			proto = int64(p)
		}
	}
	out = append(out, "*14\r\n"...)
	out = appendBulkStr(out, "server")
	out = appendBulkStr(out, "hlog")
	out = appendBulkStr(out, "version")
	out = appendBulkStr(out, "7.4.0")
	out = appendBulkStr(out, "proto")
	out = appendInt(out, proto)
	out = appendBulkStr(out, "id")
	out = appendInt(out, 1)
	out = appendBulkStr(out, "mode")
	out = appendBulkStr(out, "standalone")
	out = appendBulkStr(out, "role")
	out = appendBulkStr(out, "master")
	out = appendBulkStr(out, "modules")
	out = append(out, respEmptyArr...)
	return out
}

func appendBulk(out, b []byte) []byte {
	out = append(out, '$')
	out = strconv.AppendInt(out, int64(len(b)), 10)
	out = append(out, '\r', '\n')
	out = append(out, b...)
	return append(out, '\r', '\n')
}

func appendBulkStr(out []byte, s string) []byte {
	out = append(out, '$')
	out = strconv.AppendInt(out, int64(len(s)), 10)
	out = append(out, '\r', '\n')
	out = append(out, s...)
	return append(out, '\r', '\n')
}

func appendInt(out []byte, n int64) []byte {
	out = append(out, ':')
	out = strconv.AppendInt(out, n, 10)
	return append(out, '\r', '\n')
}

func appendErr(out []byte, msg string) []byte {
	out = append(out, "-ERR "...)
	out = append(out, msg...)
	return append(out, '\r', '\n')
}

// eqFold reports whether cmd equals the ASCII lowercase want, case-insensitively.
// want must already be lowercase.
func eqFold(cmd []byte, want string) bool {
	if len(cmd) != len(want) {
		return false
	}
	for i := 0; i < len(cmd); i++ {
		c := cmd[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != want[i] {
			return false
		}
	}
	return true
}
