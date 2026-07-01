package resp

import (
	"bufio"
	"net"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/tamnd/kv"
)

// The kv module carries no third-party dependencies, so these tests drive the
// server with a tiny hand-rolled RESP client over the real socket rather than
// pulling in go-redis. The client speaks the same multibulk requests a benchmark
// client sends, so the parse-in-place loop and the reply encoders are both on the
// path under test.

// client is a minimal RESP connection: it writes a command as a multibulk array
// and reads back one reply, enough to assert GET/SET/DEL/EXISTS behaviour.
type client struct {
	conn net.Conn
	r    *bufio.Reader
}

func dial(t *testing.T, sock string) *client {
	t.Helper()
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return &client{conn: c, r: bufio.NewReader(c)}
}

// cmd writes one command and returns the raw first line of the reply plus, for a
// bulk string, its decoded payload. A nil payload with a "$-1" line is the Redis
// missing-key reply.
func (c *client) cmd(t *testing.T, args ...string) (line string, bulk string, isNil bool) {
	t.Helper()
	var b []byte
	b = append(b, '*')
	b = strconv.AppendInt(b, int64(len(args)), 10)
	b = append(b, '\r', '\n')
	for _, a := range args {
		b = append(b, '$')
		b = strconv.AppendInt(b, int64(len(a)), 10)
		b = append(b, '\r', '\n')
		b = append(b, a...)
		b = append(b, '\r', '\n')
	}
	if _, err := c.conn.Write(b); err != nil {
		t.Fatalf("write: %v", err)
	}
	first, err := c.r.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	first = trimLine(first)
	if len(first) == 0 {
		t.Fatal("empty reply line")
	}
	switch first[0] {
	case '$':
		n, _ := strconv.Atoi(first[1:])
		if n < 0 {
			return first, "", true
		}
		payload := make([]byte, n+2) // value plus trailing CRLF
		if _, err := readFull(c.r, payload); err != nil {
			t.Fatalf("read bulk: %v", err)
		}
		return first, string(payload[:n]), false
	default:
		return first, "", false
	}
}

func (c *client) close() { _ = c.conn.Close() }

func trimLine(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}

func readFull(r *bufio.Reader, p []byte) (int, error) {
	got := 0
	for got < len(p) {
		n, err := r.Read(p[got:])
		got += n
		if err != nil {
			return got, err
		}
	}
	return got, nil
}

// startServer opens a temp store, serves it on a unix socket, and returns the
// socket path plus a cleanup that tears both down.
func startServer(t *testing.T, forceSync bool) (string, func()) {
	t.Helper()
	dir := t.TempDir()
	db, err := kv.Open(filepath.Join(dir, "hlog.db"), kv.Options{KeyCapacity: 1000})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	sock := filepath.Join(dir, "s.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := New(db, forceSync)
	go func() { _ = srv.Serve(ln) }()
	cleanup := func() {
		_ = srv.Close()
		_ = db.Sync()
		_ = db.Close()
	}
	return sock, cleanup
}

func TestPing(t *testing.T) {
	sock, cleanup := startServer(t, false)
	defer cleanup()
	c := dial(t, sock)
	defer c.close()
	if line, _, _ := c.cmd(t, "PING"); line != "+PONG" {
		t.Fatalf("ping = %q, want +PONG", line)
	}
}

func TestSetGet(t *testing.T) {
	sock, cleanup := startServer(t, false)
	defer cleanup()
	c := dial(t, sock)
	defer c.close()

	if line, _, _ := c.cmd(t, "SET", "k", "value-one"); line != "+OK" {
		t.Fatalf("set = %q, want +OK", line)
	}
	if _, bulk, isNil := c.cmd(t, "GET", "k"); isNil || bulk != "value-one" {
		t.Fatalf("get = %q (nil=%v), want value-one", bulk, isNil)
	}
	// An overwrite reads back as the last write, the Redis last-writer-wins
	// contract the append log honours by append order.
	c.cmd(t, "SET", "k", "value-two")
	if _, bulk, _ := c.cmd(t, "GET", "k"); bulk != "value-two" {
		t.Fatalf("after overwrite get = %q, want value-two", bulk)
	}
}

func TestGetMissingIsNil(t *testing.T) {
	sock, cleanup := startServer(t, false)
	defer cleanup()
	c := dial(t, sock)
	defer c.close()
	if _, _, isNil := c.cmd(t, "GET", "absent"); !isNil {
		t.Fatal("get absent should reply nil")
	}
}

func TestExistsAndDel(t *testing.T) {
	sock, cleanup := startServer(t, false)
	defer cleanup()
	c := dial(t, sock)
	defer c.close()

	c.cmd(t, "SET", "a", "1")
	c.cmd(t, "SET", "b", "2")
	if line, _, _ := c.cmd(t, "EXISTS", "a", "b", "c"); line != ":2" {
		t.Fatalf("exists = %q, want :2", line)
	}
	// DEL replies with the count actually removed: a is present, c is not.
	if line, _, _ := c.cmd(t, "DEL", "a", "c"); line != ":1" {
		t.Fatalf("del = %q, want :1", line)
	}
	if line, _, _ := c.cmd(t, "EXISTS", "a"); line != ":0" {
		t.Fatalf("exists after del = %q, want :0", line)
	}
}

func TestManyKeys(t *testing.T) {
	sock, cleanup := startServer(t, false)
	defer cleanup()
	c := dial(t, sock)
	defer c.close()

	for i := range 200 {
		c.cmd(t, "SET", "key:"+strconv.Itoa(i), "val:"+strconv.Itoa(i))
	}
	for i := range 200 {
		_, bulk, isNil := c.cmd(t, "GET", "key:"+strconv.Itoa(i))
		if isNil || bulk != "val:"+strconv.Itoa(i) {
			t.Fatalf("get %d = %q (nil=%v)", i, bulk, isNil)
		}
	}
}

func TestBgRewriteAOFSyncs(t *testing.T) {
	// In a synced server the durability hook returns OK after forcing the barrier;
	// the test asserts the command round-trips and the value survives.
	sock, cleanup := startServer(t, true)
	defer cleanup()
	c := dial(t, sock)
	defer c.close()

	c.cmd(t, "SET", "durable", "yes")
	if line, _, _ := c.cmd(t, "BGREWRITEAOF"); line != "+OK" {
		t.Fatalf("bgrewriteaof = %q, want +OK", line)
	}
	if _, bulk, _ := c.cmd(t, "GET", "durable"); bulk != "yes" {
		t.Fatalf("get after sync = %q, want yes", bulk)
	}
}
