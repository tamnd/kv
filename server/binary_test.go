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

func TestBinaryInteractiveTxnReadModifyWrite(t *testing.T) {
	cl := newBinaryServer(t)
	if _, err := cl.Set([]byte("bal"), []byte("100"), 0); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Begin a writable transaction, read the balance, write a new one based on it, commit.
	txn, err := cl.BeginTxn(true)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	v, found, err := txn.Get([]byte("bal"))
	if err != nil || !found || string(v) != "100" {
		t.Fatalf("txn get = %q found=%v err=%v", v, found, err)
	}
	if err := txn.Set([]byte("bal"), []byte("150"), 0); err != nil {
		t.Fatalf("txn set: %v", err)
	}
	// The write is not visible outside the transaction until commit.
	if outside, _, _ := cl.Get([]byte("bal")); string(outside) != "100" {
		t.Fatalf("uncommitted write leaked: %q", outside)
	}
	version, err := txn.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if version == 0 {
		t.Fatalf("commit returned zero version")
	}
	if after, _, _ := cl.Get([]byte("bal")); string(after) != "150" {
		t.Fatalf("after commit = %q, want 150", after)
	}
}

func TestBinaryInteractiveTxnSeesOwnWrites(t *testing.T) {
	cl := newBinaryServer(t)
	txn, err := cl.BeginTxn(true)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer txn.Discard()
	if err := txn.Set([]byte("k"), []byte("v"), 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	v, found, err := txn.Get([]byte("k"))
	if err != nil || !found || string(v) != "v" {
		t.Fatalf("read own write = %q found=%v err=%v", v, found, err)
	}
	ok, err := txn.Exists([]byte("k"))
	if err != nil || !ok {
		t.Fatalf("exists own write = %v err=%v", ok, err)
	}
}

func TestBinaryInteractiveTxnDiscard(t *testing.T) {
	cl := newBinaryServer(t)
	txn, err := cl.BeginTxn(true)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := txn.Set([]byte("k"), []byte("v"), 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := txn.Discard(); err != nil {
		t.Fatalf("discard: %v", err)
	}
	// The discarded write never landed.
	if _, found, _ := cl.Get([]byte("k")); found {
		t.Fatalf("discarded write is visible")
	}
	// Operations on a discarded transaction report it is gone.
	if _, _, err := txn.Get([]byte("k")); !errors.Is(err, ErrNoSuchTxn) {
		t.Fatalf("get on discarded txn err = %v, want ErrNoSuchTxn", err)
	}
}

func TestBinaryInteractiveTxnUnknownID(t *testing.T) {
	cl := newBinaryServer(t)
	// A handle with an id the server never issued is unknown.
	bogus := &TxnHandle{c: cl, id: 999999}
	if _, _, err := bogus.Get([]byte("k")); !errors.Is(err, ErrNoSuchTxn) {
		t.Fatalf("get on bogus id err = %v, want ErrNoSuchTxn", err)
	}
}

func TestBinaryInteractiveTxnRangeAndMerge(t *testing.T) {
	cl := newBinaryServer(t)
	for _, k := range []string{"a", "b", "c"} {
		cl.Set([]byte(k), []byte("v"), 0)
	}
	txn, err := cl.BeginTxn(true)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := txn.DeleteRange([]byte("a"), []byte("c")); err != nil {
		t.Fatalf("delete range: %v", err)
	}
	if err := txn.Merge([]byte("counter"), []byte("1")); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if _, err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// a and b removed by the range delete, c survives.
	if _, found, _ := cl.Get([]byte("a")); found {
		t.Fatalf("a should be deleted")
	}
	if _, found, _ := cl.Get([]byte("c")); !found {
		t.Fatalf("c should survive")
	}
	if _, found, _ := cl.Get([]byte("counter")); !found {
		t.Fatalf("counter should exist after merge")
	}
}

// newTestDB opens a fresh temp database and closes it on cleanup, for Service-level tests that
// drive the registry directly without a socket in between.
func newTestDB(t *testing.T) *kv.DB {
	t.Helper()
	db, err := kv.Open(t.TempDir() + "/test.kv")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestBinaryInteractiveTxnConflict(t *testing.T) {
	cl := newBinaryServer(t)
	if _, err := cl.Set([]byte("k"), []byte("0"), 0); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Two writable transactions both read the key, then both try to write it. The first to
	// commit wins; the second conflicts because its snapshot is now stale.
	t1, err := cl.BeginTxn(true)
	if err != nil {
		t.Fatalf("begin t1: %v", err)
	}
	t2, err := cl.BeginTxn(true)
	if err != nil {
		t.Fatalf("begin t2: %v", err)
	}
	if _, _, err := t1.Get([]byte("k")); err != nil {
		t.Fatalf("t1 get: %v", err)
	}
	if _, _, err := t2.Get([]byte("k")); err != nil {
		t.Fatalf("t2 get: %v", err)
	}
	if err := t1.Set([]byte("k"), []byte("1"), 0); err != nil {
		t.Fatalf("t1 set: %v", err)
	}
	if err := t2.Set([]byte("k"), []byte("2"), 0); err != nil {
		t.Fatalf("t2 set: %v", err)
	}
	if _, err := t1.Commit(); err != nil {
		t.Fatalf("t1 commit: %v", err)
	}
	if _, err := t2.Commit(); !errors.Is(err, kv.ErrConflict) {
		t.Fatalf("t2 commit err = %v, want ErrConflict", err)
	}
}

func TestServiceInteractiveTxnCap(t *testing.T) {
	db := newTestDB(t)
	svc := newServiceWithLimits(db, 2, defaultTxnIdleTTL)
	defer svc.Close()

	if _, err := svc.BeginTxn(false); err != nil {
		t.Fatalf("begin 1: %v", err)
	}
	if _, err := svc.BeginTxn(false); err != nil {
		t.Fatalf("begin 2: %v", err)
	}
	if _, err := svc.BeginTxn(false); !errors.Is(err, ErrTooManyTxns) {
		t.Fatalf("begin 3 err = %v, want ErrTooManyTxns", err)
	}
}

func TestServiceInteractiveTxnReaper(t *testing.T) {
	db := newTestDB(t)
	// Drive the idle clock hard so the reaper fires within the test.
	svc := newServiceWithLimits(db, defaultMaxOpenTxns, 20*time.Millisecond)
	defer svc.Close()

	id, err := svc.BeginTxn(true)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Leave the session untouched so its lastUsed stays put: any use refreshes it and would
	// keep it alive. After several idle windows the reaper has force-discarded it, and the next
	// reference reports it gone.
	time.Sleep(300 * time.Millisecond)
	if _, err := svc.TxnExists(id, []byte("k")); !errors.Is(err, ErrNoSuchTxn) {
		t.Fatalf("reaper did not discard idle txn, err = %v", err)
	}
}

func TestServiceInteractiveTxnCloseDiscards(t *testing.T) {
	db := newTestDB(t)
	svc := NewService(db)
	id, err := svc.BeginTxn(true)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := svc.TxnSet(id, []byte("k"), []byte("v"), 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	svc.Close()
	// Every open session is gone after Close, and the uncommitted write never landed.
	if _, err := svc.TxnExists(id, []byte("k")); !errors.Is(err, ErrNoSuchTxn) {
		t.Fatalf("after close err = %v, want ErrNoSuchTxn", err)
	}
	var found bool
	db.View(func(txn *kv.Txn) error {
		found, _ = txn.Exists([]byte("k"))
		return nil
	})
	if found {
		t.Fatalf("uncommitted write survived Close")
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
