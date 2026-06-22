package server

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/tamnd/kv"
)

// newBinaryServer opens a fresh temp database, binds a listener on a free port, and serves the
// binary protocol on it, returning a connected Client. The cleanup closes the client, shuts the
// server down, and closes the database, in that order, so an idle serve loop is released before
// the file folds.
func newBinaryServer(t *testing.T) *Client {
	t.Helper()
	addr := newBinaryServerAddr(t)
	return dialClient(t, addr)
}

// newBinaryServerAddr opens a fresh temp database, serves the binary protocol on a free port,
// and returns its address. The cleanup shuts the server down and closes the database. A test
// that needs several connections to one server dials each itself.
func newBinaryServerAddr(t *testing.T) string {
	t.Helper()
	path := t.TempDir() + "/test.kv"
	db, err := kv.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := New(db, Options{})
	go srv.ServeBinary(ln)
	t.Cleanup(func() {
		srv.Shutdown(context.Background())
		db.Close()
	})
	return ln.Addr().String()
}

// dialClient dials a client to addr and closes it on cleanup.
func dialClient(t *testing.T, addr string) *Client {
	t.Helper()
	cl, err := Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { cl.Close() })
	return cl
}

func TestBinaryPutGetDelete(t *testing.T) {
	cl := newBinaryServer(t)

	version, err := cl.Set([]byte("alpha"), []byte("hello"), 0)
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	if version == 0 {
		t.Fatalf("set returned zero version")
	}

	value, found, err := cl.Get([]byte("alpha"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !found || string(value) != "hello" {
		t.Fatalf("get = %q found=%v, want hello", value, found)
	}

	if _, err := cl.Delete([]byte("alpha")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, found, err = cl.Get([]byte("alpha"))
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if found {
		t.Fatalf("alpha still present after delete")
	}
}

func TestBinaryExists(t *testing.T) {
	cl := newBinaryServer(t)
	if _, err := cl.Set([]byte("k"), []byte("v"), 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	ok, err := cl.Exists([]byte("k"))
	if err != nil || !ok {
		t.Fatalf("exists k = %v, %v; want true", ok, err)
	}
	ok, err = cl.Exists([]byte("missing"))
	if err != nil || ok {
		t.Fatalf("exists missing = %v, %v; want false", ok, err)
	}
}

func TestBinaryRangeDelete(t *testing.T) {
	cl := newBinaryServer(t)
	for _, k := range []string{"a", "b", "c", "d"} {
		if _, err := cl.Set([]byte(k), []byte("v"), 0); err != nil {
			t.Fatalf("set %s: %v", k, err)
		}
	}
	if _, err := cl.DeleteRange([]byte("b"), []byte("d")); err != nil {
		t.Fatalf("delete range: %v", err)
	}
	for k, wantFound := range map[string]bool{"a": true, "b": false, "c": false, "d": true} {
		_, found, err := cl.Get([]byte(k))
		if err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
		if found != wantFound {
			t.Fatalf("get %s found=%v, want %v", k, found, wantFound)
		}
	}
}

func TestBinaryBatch(t *testing.T) {
	cl := newBinaryServer(t)
	_, err := cl.Batch([]Op{
		{Kind: OpSet, Key: []byte("x"), Value: []byte("1")},
		{Kind: OpSet, Key: []byte("y"), Value: []byte("2")},
		{Kind: OpDelete, Key: []byte("x")},
	})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if _, found, _ := cl.Get([]byte("x")); found {
		t.Fatalf("x should be deleted")
	}
	v, found, _ := cl.Get([]byte("y"))
	if !found || string(v) != "2" {
		t.Fatalf("y = %q found=%v", v, found)
	}
}

func TestBinaryTxnCompareAndSet(t *testing.T) {
	cl := newBinaryServer(t)
	if _, err := cl.Set([]byte("k"), []byte("old"), 0); err != nil {
		t.Fatalf("set: %v", err)
	}

	// Assert k == "old" then set it to "new": succeeds.
	res, err := cl.Txn(TxnRequest{
		Asserts: []Assert{{Key: []byte("k"), ExpectValue: []byte("old")}},
		Ops:     []Op{{Kind: OpSet, Key: []byte("k"), Value: []byte("new")}},
	})
	if err != nil {
		t.Fatalf("txn: %v", err)
	}
	if res.Version == 0 {
		t.Fatalf("txn returned zero version")
	}
	v, _, _ := cl.Get([]byte("k"))
	if string(v) != "new" {
		t.Fatalf("k = %q, want new", v)
	}

	// Assert the stale value again: now fails with a conflict and changes nothing.
	_, err = cl.Txn(TxnRequest{
		Asserts: []Assert{{Key: []byte("k"), ExpectValue: []byte("old")}},
		Ops:     []Op{{Kind: OpSet, Key: []byte("k"), Value: []byte("never")}},
	})
	if !errors.Is(err, ErrAssertFailed) && !errors.Is(err, kv.ErrConflict) {
		t.Fatalf("stale assert err = %v, want assert/conflict", err)
	}
	v, _, _ = cl.Get([]byte("k"))
	if string(v) != "new" {
		t.Fatalf("k changed after failed assert: %q", v)
	}
}

func TestBinaryTxnReads(t *testing.T) {
	cl := newBinaryServer(t)
	if _, err := cl.Set([]byte("present"), []byte("yes"), 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	res, err := cl.Txn(TxnRequest{Ops: []Op{
		{Kind: OpGet, Key: []byte("present")},
		{Kind: OpGet, Key: []byte("absent")},
		{Kind: OpExists, Key: []byte("present")},
	}})
	if err != nil {
		t.Fatalf("txn: %v", err)
	}
	if len(res.Reads) != 3 {
		t.Fatalf("reads = %d, want 3", len(res.Reads))
	}
	if !res.Reads[0].Found || string(res.Reads[0].Value) != "yes" {
		t.Fatalf("read 0 = %+v", res.Reads[0])
	}
	if res.Reads[1].Found {
		t.Fatalf("read 1 should be a miss")
	}
	if !res.Reads[2].Found {
		t.Fatalf("read 2 exists should be true")
	}
}

func TestBinaryMerge(t *testing.T) {
	cl := newBinaryServer(t)
	// Merge on a fresh key behaves as the engine's default operator defines; the point here is
	// that the op round-trips and reports a version, not the operator's semantics.
	if _, err := cl.Merge([]byte("counter"), []byte("1")); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if _, found, err := cl.Get([]byte("counter")); err != nil || !found {
		t.Fatalf("counter after merge: found=%v err=%v", found, err)
	}
}

func TestBinaryNotFoundError(t *testing.T) {
	cl := newBinaryServer(t)
	value, found, err := cl.Get([]byte("nope"))
	if err != nil {
		t.Fatalf("get miss should not error, got %v", err)
	}
	if found || value != nil {
		t.Fatalf("miss = %q found=%v, want absent", value, found)
	}
}

func TestBinaryStats(t *testing.T) {
	cl := newBinaryServer(t)
	if _, err := cl.Set([]byte("k"), []byte("v"), 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	stats, err := cl.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Version == 0 {
		t.Fatalf("stats version = 0 after a write")
	}
}

func TestBinaryCheckpointAndCompact(t *testing.T) {
	cl := newBinaryServer(t)
	if _, err := cl.Set([]byte("k"), []byte("v"), 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := cl.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if _, err := cl.Compact(0); err != nil {
		t.Fatalf("compact: %v", err)
	}
}

func TestBinaryTTL(t *testing.T) {
	cl := newBinaryServer(t)
	if _, err := cl.Set([]byte("eph"), []byte("v"), 10*time.Second); err != nil {
		t.Fatalf("set with ttl: %v", err)
	}
	_, found, err := cl.Get([]byte("eph"))
	if err != nil || !found {
		t.Fatalf("get within ttl: found=%v err=%v", found, err)
	}
}

func TestBinaryScan(t *testing.T) {
	cl := newBinaryServer(t)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if _, err := cl.Set([]byte(k), []byte("v-"+k), 0); err != nil {
			t.Fatalf("set %s: %v", k, err)
		}
	}

	// A full forward scan yields every pair in key order with its value.
	var got []string
	err := cl.Scan(ScanOptions{}, func(key, value []byte) error {
		got = append(got, string(key)+"="+string(value))
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	want := []string{"a=v-a", "b=v-b", "c=v-c", "d=v-d", "e=v-e"}
	if len(got) != len(want) {
		t.Fatalf("scan got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("scan[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestBinaryScanBoundsAndLimit(t *testing.T) {
	cl := newBinaryServer(t)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		cl.Set([]byte(k), []byte("v"), 0)
	}

	// A bounded scan [b, e) yields b, c, d.
	var keys []string
	cl.Scan(ScanOptions{Lower: []byte("b"), Upper: []byte("e")}, func(key, _ []byte) error {
		keys = append(keys, string(key))
		return nil
	})
	if len(keys) != 3 || keys[0] != "b" || keys[2] != "d" {
		t.Fatalf("bounded scan = %v, want [b c d]", keys)
	}

	// A limit caps the count.
	keys = nil
	cl.Scan(ScanOptions{Limit: 2}, func(key, _ []byte) error {
		keys = append(keys, string(key))
		return nil
	})
	if len(keys) != 2 {
		t.Fatalf("limited scan = %v, want 2 keys", keys)
	}
}

func TestBinaryScanKeysOnly(t *testing.T) {
	cl := newBinaryServer(t)
	cl.Set([]byte("k"), []byte("value"), 0)
	err := cl.Scan(ScanOptions{KeysOnly: true}, func(key, value []byte) error {
		if string(key) != "k" {
			t.Fatalf("key = %q, want k", key)
		}
		if value != nil {
			t.Fatalf("keys-only value = %q, want nil", value)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan keys-only: %v", err)
	}
}

func TestBinaryScanReverse(t *testing.T) {
	cl := newBinaryServer(t)
	for _, k := range []string{"a", "b", "c"} {
		cl.Set([]byte(k), []byte("v"), 0)
	}
	var keys []string
	cl.Scan(ScanOptions{Reverse: true}, func(key, _ []byte) error {
		keys = append(keys, string(key))
		return nil
	})
	if len(keys) != 3 || keys[0] != "c" || keys[2] != "a" {
		t.Fatalf("reverse scan = %v, want [c b a]", keys)
	}
}

func TestBinaryWatch(t *testing.T) {
	// A dedicated client drives the watch, since a watch takes over its connection; a second
	// client on the same server does the write.
	addr := newBinaryServerAddr(t)
	watcher := dialClient(t, addr)
	writer := dialClient(t, addr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got := make(chan kv.Change, 8)
	done := make(chan error, 1)
	go func() {
		done <- watcher.Watch(ctx, nil, 0, func(c kv.Change) error {
			got <- c
			return nil
		})
	}()

	// Give the watch a moment to subscribe before the write, so the change is not missed.
	time.Sleep(50 * time.Millisecond)
	if _, err := writer.Set([]byte("wk"), []byte("wv"), 0); err != nil {
		t.Fatalf("set: %v", err)
	}

	select {
	case c := <-got:
		if string(c.Key) != "wk" || string(c.Value) != "wv" {
			t.Fatalf("change = %+v, want wk=wv", c)
		}
		if c.Kind != kv.ChangeSet {
			t.Fatalf("kind = %v, want set", c.Kind)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("watch did not deliver the change")
	}

	// Cancelling ends the watch with the context error.
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("watch end err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("watch did not end after cancel")
	}
}

func TestBinaryReuseConnection(t *testing.T) {
	cl := newBinaryServer(t)
	// Many ops on one connection exercise the per-connection request/response loop reusing one
	// socket, the property the framing exists to support.
	for i := 0; i < 100; i++ {
		k := []byte{byte(i)}
		if _, err := cl.Set(k, []byte("v"), 0); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	for i := 0; i < 100; i++ {
		_, found, err := cl.Get([]byte{byte(i)})
		if err != nil || !found {
			t.Fatalf("get %d: found=%v err=%v", i, found, err)
		}
	}
}
