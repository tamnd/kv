package resp

import (
	"bufio"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tamnd/kv"
)

// startServer opens a fresh database in a temp dir, serves it over RESP on a
// loopback port, and returns a connected client plus a cleanup the test defers.
func startServer(t *testing.T) (*client, func()) {
	t.Helper()
	db, err := kv.Open(filepath.Join(t.TempDir(), "kv.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := New(db)
	done := make(chan struct{})
	go func() { _ = srv.Serve(ln); close(done) }()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	cli := &client{conn: conn, r: bufio.NewReader(conn)}
	cleanup := func() {
		_ = conn.Close()
		_ = srv.Close()
		// Close owns the connections but not the listener (the serve command does),
		// so the test closes the listener to unblock Accept and let Serve return.
		_ = ln.Close()
		<-done
		_ = db.Close()
	}
	return cli, cleanup
}

// client is a minimal RESP client: it sends a multibulk command and reads one
// reply, enough to exercise the server without a third-party dependency.
type client struct {
	conn net.Conn
	r    *bufio.Reader
}

func (c *client) send(t *testing.T, args ...string) {
	t.Helper()
	var b strings.Builder
	b.WriteByte('*')
	b.WriteString(strconv.Itoa(len(args)))
	b.WriteString("\r\n")
	for _, a := range args {
		b.WriteByte('$')
		b.WriteString(strconv.Itoa(len(a)))
		b.WriteString("\r\n")
		b.WriteString(a)
		b.WriteString("\r\n")
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.conn.Write([]byte(b.String())); err != nil {
		t.Fatalf("write %v: %v", args, err)
	}
}

// readReply reads one RESP reply and returns it as a string: a simple string or
// error keeps its leading byte, an integer is its decimal text, a bulk string is
// its payload, and a nil bulk is "<nil>".
func (c *client) readReply(t *testing.T) string {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := c.r.ReadString('\n')
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		t.Fatalf("empty reply line")
	}
	switch line[0] {
	case '+', '-', ':':
		return line
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "<nil>"
		}
		buf := make([]byte, n+2)
		if _, err := readFull(c.r, buf); err != nil {
			t.Fatalf("read bulk: %v", err)
		}
		return string(buf[:n])
	case '*':
		// Skip an array reply (HELLO), reading its flattened elements.
		n, _ := strconv.Atoi(line[1:])
		for i := 0; i < n; i++ {
			c.readReply(t)
		}
		return "*" + strconv.Itoa(n)
	}
	t.Fatalf("unknown reply type %q", line)
	return ""
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	got := 0
	for got < len(buf) {
		n, err := r.Read(buf[got:])
		got += n
		if err != nil {
			return got, err
		}
	}
	return got, nil
}

func (c *client) cmd(t *testing.T, args ...string) string {
	t.Helper()
	c.send(t, args...)
	return c.readReply(t)
}

func TestPingAndSetGet(t *testing.T) {
	cli, cleanup := startServer(t)
	defer cleanup()

	if got := cli.cmd(t, "PING"); got != "+PONG" {
		t.Fatalf("PING = %q, want +PONG", got)
	}
	if got := cli.cmd(t, "SET", "alpha", "one"); got != "+OK" {
		t.Fatalf("SET = %q, want +OK", got)
	}
	if got := cli.cmd(t, "GET", "alpha"); got != "one" {
		t.Fatalf("GET alpha = %q, want one", got)
	}
	if got := cli.cmd(t, "GET", "missing"); got != "<nil>" {
		t.Fatalf("GET missing = %q, want <nil>", got)
	}
}

func TestExistsDelDbsize(t *testing.T) {
	cli, cleanup := startServer(t)
	defer cleanup()

	cli.cmd(t, "SET", "a", "1")
	cli.cmd(t, "SET", "b", "2")
	if got := cli.cmd(t, "EXISTS", "a", "b", "c"); got != ":2" {
		t.Fatalf("EXISTS = %q, want :2", got)
	}
	// DBSIZE is the engine's live-key estimate; after two fresh inserts it is exact.
	if got := cli.cmd(t, "DBSIZE"); got != ":2" {
		t.Fatalf("DBSIZE = %q, want :2", got)
	}
	if got := cli.cmd(t, "DEL", "a", "c"); got != ":1" {
		t.Fatalf("DEL = %q, want :1 (only a existed)", got)
	}
	// The delete is visible to reads immediately, even though DBSIZE only reconciles
	// the dropped key at the next checkpoint.
	if got := cli.cmd(t, "GET", "a"); got != "<nil>" {
		t.Fatalf("GET a after DEL = %q, want <nil>", got)
	}
	if got := cli.cmd(t, "EXISTS", "a"); got != ":0" {
		t.Fatalf("EXISTS a after DEL = %q, want :0", got)
	}
}

func TestHelloHandshake(t *testing.T) {
	cli, cleanup := startServer(t)
	defer cleanup()

	// HELLO 3 returns a flattened map of 14 elements; readReply drains it and
	// reports the element count, which is enough to confirm the handshake replied.
	if got := cli.cmd(t, "HELLO", "3"); got != "*14" {
		t.Fatalf("HELLO 3 = %q, want a 14-element array", got)
	}
	// The connection is still usable after the handshake.
	if got := cli.cmd(t, "PING"); got != "+PONG" {
		t.Fatalf("PING after HELLO = %q, want +PONG", got)
	}
}

// TestConcurrentSetNoConflict drives a small, overlapping keyspace from several
// connections at once. An optimistic kv transaction would lose a write-write race
// here and surface "transaction conflict", and a retrying one would livelock on a
// hot key; the Redis face writes blind commits instead, which are last-writer-wins
// and never conflict, so every SET must return +OK and the run must not stall.
func TestConcurrentSetNoConflict(t *testing.T) {
	db, err := kv.Open(filepath.Join(t.TempDir(), "kv.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := New(db)
	done := make(chan struct{})
	go func() { _ = srv.Serve(ln); close(done) }()
	defer func() {
		_ = srv.Close()
		_ = ln.Close()
		<-done
		_ = db.Close()
	}()

	const writers = 8
	const iters = 200
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			cli := &client{conn: conn, r: bufio.NewReader(conn)}
			for i := 0; i < iters; i++ {
				// Ten shared keys, so writers race on the same keys constantly.
				key := "k" + strconv.Itoa(i%10)
				cli.send(t, "SET", key, strconv.Itoa(w))
				if got := cli.readReply(t); got != "+OK" {
					errs <- fmtErr("writer %d SET %s = %q, want +OK", w, key, got)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
}

func fmtErr(format string, a ...any) error { return fmt.Errorf(format, a...) }

func TestInlinePing(t *testing.T) {
	cli, cleanup := startServer(t)
	defer cleanup()

	// An inline command (no multibulk framing), the form redis-cli sends for a
	// bare PING.
	_ = cli.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := cli.conn.Write([]byte("PING\r\n")); err != nil {
		t.Fatalf("write inline: %v", err)
	}
	if got := cli.readReply(t); got != "+PONG" {
		t.Fatalf("inline PING = %q, want +PONG", got)
	}
}
