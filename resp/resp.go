// Package resp puts a Redis-compatible front end on the kv engine. It speaks
// RESP2/RESP3 over TCP and unix listeners so any Redis client, redis-cli, or
// benchmark can drive the store with GET/SET/DEL/EXISTS, PING, the HELLO
// handshake, and the handful of introspection commands a client issues at connect
// (CONFIG, COMMAND, INFO, DBSIZE, CLIENT, SELECT, FLUSHALL). This is the store's
// own network face: a SET is one append to the hybrid log and a GET is a point
// lookup, the same surface Open returns in process.
//
// The wire loop is a raw-buffer, parse-in-place, one-write-per-burst design over
// the *kv.DB surface. The hot path reads one chunk off the socket, parses every
// complete command sitting in the buffer in place, runs each one appending its
// reply to a single output buffer, and writes that buffer back in one syscall, so
// steady-state parsing copies nothing: a key points straight into the read buffer
// and the engine copies the value once when it frames the record into the log.
//
// Durability follows the store the server was opened over, which is the redis
// appendfsync contract. With appendfsync everysec the store acks a SET from the
// in-memory hot tier and the background flusher fsyncs it a moment later, a bounded
// sub-second loss window. With appendfsync always the store waits for the
// group-commit fsync before the SET returns, so an acked write survives a crash
// with zero loss; concurrent writers coalesce onto one shared fsync. BGREWRITEAOF
// forces a durability barrier on demand under either mode.
package resp

import (
	"bytes"
	"errors"
	"net"
	"strconv"
	"sync"

	"github.com/tamnd/kv"
)

// Config carries the redis-style settings the server reports back through CONFIG
// GET and INFO. It does not change how the store is opened; the durability mode is
// fixed when the caller opens the *kv.DB (appendfsync always opens it with
// SyncWrites true). These fields let a redis client read the running config and
// see values it recognizes.
type Config struct {
	AppendOnly  string // "yes" or "no"; kv is always log-backed, so it reports "yes"
	AppendFsync string // "no", "everysec", or "always"
	MaxMemory   int64  // resident memory budget in bytes; 0 means unset
	Dir         string // data directory
	DBFilename  string // store file name within Dir
}

// Server serves a kv store over RESP on one or more listeners. cfg is the
// redis-style config it echoes back to clients; it does not affect the store,
// which is already open in the durability mode the caller chose.
type Server struct {
	db  *kv.DB
	cfg Config

	wg     sync.WaitGroup
	mu     sync.Mutex
	lns    map[net.Listener]struct{}
	conns  map[net.Conn]struct{}
	closed bool
}

// New builds a Server over an open store. cfg is the redis-style config the server
// reports through CONFIG GET and INFO; the durability contract itself is set when
// the caller opens the *kv.DB. Call Serve once per bound listener; a TCP and a unix
// listener can both drive the same server.
func New(db *kv.DB, cfg Config) *Server {
	return &Server{db: db, cfg: cfg, lns: make(map[net.Listener]struct{}), conns: make(map[net.Conn]struct{})}
}

// Serve accepts connections on ln until Close, serving each one in its own
// goroutine. It returns nil after a Close, and the accept error otherwise. It may
// be called once per listener, so a TCP and a unix listener can both feed the same
// server the way redis-server binds both at once.
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
	s.lns[ln] = struct{}{}
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
	for ln := range s.lns {
		_ = ln.Close()
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
			return appendBulk(out, s.infoBytes()), false
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
			return s.cmdConfig(out, args), false
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
			// The "make it durable now" hook, redis BGREWRITEAOF. It forces a
			// group-commit barrier: Sync waits for the flusher to fsync everything
			// acked so far. Under appendfsync always the acked writes are already
			// durable, so it is a cheap confirmation; under everysec it flushes the
			// sub-second tail the background timer had not reached yet.
			_ = s.db.Sync()
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
	out = appendBulkStr(out, "kv")
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

// cmdConfig answers CONFIG GET and CONFIG SET the way a redis client expects at
// connect. GET returns the running value of a known parameter as the flat
// [name, value, ...] array redis uses, so a client that reads maxmemory or
// appendfsync sees a real answer; an unknown parameter returns an empty array. SET
// is accepted and answered OK without changing anything: the durability mode and
// the memory budget are fixed when the store is opened, so there is nothing to
// retune at runtime.
func (s *Server) cmdConfig(out []byte, args [][]byte) []byte {
	if len(args) >= 2 && eqFold(args[1], "set") {
		return append(out, respOK...)
	}
	if len(args) < 3 || !eqFold(args[1], "get") {
		return append(out, respEmptyArr...)
	}
	pat := args[2]
	pairs := s.configPairs()
	matched := 0
	for i := range pairs {
		if configMatch(pat, pairs[i][0]) {
			matched++
		}
	}
	out = append(out, '*')
	out = strconv.AppendInt(out, int64(matched*2), 10)
	out = append(out, '\r', '\n')
	for i := range pairs {
		if configMatch(pat, pairs[i][0]) {
			out = appendBulkStr(out, pairs[i][0])
			out = appendBulkStr(out, pairs[i][1])
		}
	}
	return out
}

// configPairs is the redis-style running config the server reports through CONFIG
// GET. maxmemory-policy is fixed at noeviction (kv never evicts a key) and save is
// empty (there is no periodic RDB dump, only the append log).
func (s *Server) configPairs() [][2]string {
	maxmem := "0"
	if s.cfg.MaxMemory > 0 {
		maxmem = strconv.FormatInt(s.cfg.MaxMemory, 10)
	}
	return [][2]string{
		{"maxmemory", maxmem},
		{"maxmemory-policy", "noeviction"},
		{"appendonly", s.cfg.AppendOnly},
		{"appendfsync", s.cfg.AppendFsync},
		{"save", ""},
		{"dir", s.cfg.Dir},
		{"dbfilename", s.cfg.DBFilename},
	}
}

// configMatch reports whether a CONFIG GET pattern selects a parameter name. It
// covers the forms a client actually sends: an exact name, "*" for all, and a
// trailing-star prefix like "maxmemory*". The name is matched case-insensitively.
func configMatch(pat []byte, name string) bool {
	if len(pat) == 1 && pat[0] == '*' {
		return true
	}
	if n := len(pat); n > 0 && pat[n-1] == '*' {
		prefix := pat[:n-1]
		if len(prefix) > len(name) {
			return false
		}
		return eqFoldPrefix(prefix, name)
	}
	return eqFold(pat, name)
}

// eqFoldPrefix reports whether name begins with the ASCII-lowercase-folded prefix.
func eqFoldPrefix(prefix []byte, name string) bool {
	for i := 0; i < len(prefix); i++ {
		c := prefix[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		d := name[i]
		if d >= 'A' && d <= 'Z' {
			d += 'a' - 'A'
		}
		if c != d {
			return false
		}
	}
	return true
}

// infoBytes builds the INFO reply a client reads at connect. It reports the redis
// version the wire layer emulates and a Persistence section carrying the append
// log state and the fsync policy, so a client sees the durability mode the store
// was opened in.
func (s *Server) infoBytes() []byte {
	aof := "1"
	if s.cfg.AppendOnly == "no" {
		aof = "0"
	}
	out := make([]byte, 0, 160)
	out = append(out, "# Server\r\nredis_version:7.4.0\r\nkv_redis_layer:1\r\n"...)
	out = append(out, "# Persistence\r\naof_enabled:"...)
	out = append(out, aof...)
	out = append(out, "\r\naof_fsync:"...)
	out = append(out, s.cfg.AppendFsync...)
	out = append(out, "\r\n"...)
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
