package server

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/tamnd/kv"
)

// newAuthBinaryServer opens a fresh temp database, serves the binary protocol on a free port with a
// fixed token table, and returns its address. The table mirrors the HTTP auth test: an admin token,
// a read-write token on "t1-", and a read-only token on "t1-". The cleanup shuts the server down and
// closes the database.
func newAuthBinaryServer(t *testing.T) string {
	t.Helper()
	db, err := kv.Open(t.TempDir() + "/test.kv")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	auth := NewStaticTokenAuthenticator(map[string]*Identity{
		"admin": {Name: "admin", Admin: true},
		"rw":    {Name: "rw", Grants: []Grant{{Prefix: []byte("t1-"), Write: true}}},
		"ro":    {Name: "ro", Grants: []Grant{{Prefix: []byte("t1-")}}},
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := New(db, Options{Auth: auth})
	go srv.ServeBinary(ln)
	t.Cleanup(func() {
		srv.Shutdown(context.Background())
		db.Close()
	})
	return ln.Addr().String()
}

func TestBinaryAuthRequiresHandshake(t *testing.T) {
	addr := newAuthBinaryServer(t)
	cl := dialClient(t, addr)
	// An operation before any handshake is unauthenticated, not merely forbidden.
	if _, _, err := cl.Get([]byte("t1-a")); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("pre-auth Get error = %v, want ErrUnauthenticated", err)
	}
}

func TestBinaryAuthBadToken(t *testing.T) {
	addr := newAuthBinaryServer(t)
	cl := dialClient(t, addr)
	if _, err := cl.Authenticate("bogus"); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("bad-token Authenticate error = %v, want ErrUnauthenticated", err)
	}
	// A failed handshake binds nothing, so a later op is still unauthenticated.
	if _, _, err := cl.Get([]byte("t1-a")); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("Get after failed auth error = %v, want ErrUnauthenticated", err)
	}
}

func TestBinaryAuthName(t *testing.T) {
	addr := newAuthBinaryServer(t)
	cl := dialClient(t, addr)
	name, err := cl.Authenticate("rw")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if name != "rw" {
		t.Fatalf("identity name = %q, want %q", name, "rw")
	}
}

func TestBinaryAuthPrefixEnforced(t *testing.T) {
	addr := newAuthBinaryServer(t)
	cl := dialClient(t, addr)
	if _, err := cl.Authenticate("rw"); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	// The rw token writes and reads its own prefix.
	if _, err := cl.Set([]byte("t1-a"), []byte("v"), 0); err != nil {
		t.Fatalf("rw Set in-prefix: %v", err)
	}
	if v, found, err := cl.Get([]byte("t1-a")); err != nil || !found || string(v) != "v" {
		t.Fatalf("rw Get in-prefix = %q found=%v err=%v", v, found, err)
	}
	// It cannot touch another prefix.
	if _, err := cl.Set([]byte("t2-a"), []byte("v"), 0); !errors.Is(err, ErrForbidden) {
		t.Fatalf("rw Set out-of-prefix error = %v, want ErrForbidden", err)
	}
	if _, _, err := cl.Get([]byte("t2-a")); !errors.Is(err, ErrForbidden) {
		t.Fatalf("rw Get out-of-prefix error = %v, want ErrForbidden", err)
	}
}

func TestBinaryAuthReadOnlyCannotWrite(t *testing.T) {
	addr := newAuthBinaryServer(t)
	cl := dialClient(t, addr)
	if _, err := cl.Authenticate("ro"); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if _, err := cl.Set([]byte("t1-a"), []byte("v"), 0); !errors.Is(err, ErrForbidden) {
		t.Fatalf("ro Set error = %v, want ErrForbidden", err)
	}
	if _, err := cl.Delete([]byte("t1-a")); !errors.Is(err, ErrForbidden) {
		t.Fatalf("ro Delete error = %v, want ErrForbidden", err)
	}
	// A read in its prefix is allowed (the key is simply absent).
	if _, found, err := cl.Get([]byte("t1-a")); err != nil || found {
		t.Fatalf("ro Get in-prefix found=%v err=%v, want absent, no error", found, err)
	}
}

func TestBinaryAuthOpsEndpointsAdminOnly(t *testing.T) {
	addr := newAuthBinaryServer(t)
	// A non-admin token cannot reach the operational opcodes.
	rw := dialClient(t, addr)
	if _, err := rw.Authenticate("rw"); err != nil {
		t.Fatalf("Authenticate rw: %v", err)
	}
	if _, err := rw.Stats(); !errors.Is(err, ErrForbidden) {
		t.Fatalf("rw Stats error = %v, want ErrForbidden", err)
	}
	if err := rw.Checkpoint(); !errors.Is(err, ErrForbidden) {
		t.Fatalf("rw Checkpoint error = %v, want ErrForbidden", err)
	}
	if _, err := rw.Compact(1); !errors.Is(err, ErrForbidden) {
		t.Fatalf("rw Compact error = %v, want ErrForbidden", err)
	}
	// The admin token reaches them.
	admin := dialClient(t, addr)
	if _, err := admin.Authenticate("admin"); err != nil {
		t.Fatalf("Authenticate admin: %v", err)
	}
	if _, err := admin.Stats(); err != nil {
		t.Fatalf("admin Stats: %v", err)
	}
	if err := admin.Checkpoint(); err != nil {
		t.Fatalf("admin Checkpoint: %v", err)
	}
	if _, err := admin.Compact(1); err != nil {
		t.Fatalf("admin Compact: %v", err)
	}
}

func TestBinaryAuthBatchAllOrNothing(t *testing.T) {
	addr := newAuthBinaryServer(t)
	cl := dialClient(t, addr)
	if _, err := cl.Authenticate("rw"); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	// A batch mixing an allowed and a forbidden key is rejected whole, before any write.
	ops := []Op{
		{Kind: OpSet, Key: []byte("t1-a"), Value: []byte("v")},
		{Kind: OpSet, Key: []byte("t2-a"), Value: []byte("v")},
	}
	if _, err := cl.Batch(ops); !errors.Is(err, ErrForbidden) {
		t.Fatalf("mixed-prefix Batch error = %v, want ErrForbidden", err)
	}
	// The allowed key must not have been written.
	if _, found, err := cl.Get([]byte("t1-a")); err != nil || found {
		t.Fatalf("allowed key from rejected batch was written: found=%v err=%v", found, err)
	}
}

func TestBinaryAuthRangeDeleteEnforced(t *testing.T) {
	addr := newAuthBinaryServer(t)
	cl := dialClient(t, addr)
	if _, err := cl.Authenticate("rw"); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	// A range delete inside the grant is allowed.
	if _, err := cl.DeleteRange([]byte("t1-a"), []byte("t1-z")); err != nil {
		t.Fatalf("rw DeleteRange in-prefix: %v", err)
	}
	// One that escapes the grant is forbidden.
	if _, err := cl.DeleteRange([]byte("t1-a"), []byte("u")); !errors.Is(err, ErrForbidden) {
		t.Fatalf("rw DeleteRange escaping prefix error = %v, want ErrForbidden", err)
	}
}

func TestBinaryAuthScanPrefixEnforced(t *testing.T) {
	addr := newAuthBinaryServer(t)
	cl := dialClient(t, addr)
	if _, err := cl.Authenticate("ro"); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	// A scan confined to the granted prefix is allowed.
	if err := cl.Scan(ScanOptions{Prefix: []byte("t1-")}, func(_, _ []byte) error { return nil }); err != nil {
		t.Fatalf("ro scan in-prefix: %v", err)
	}
	// A scan of another prefix is forbidden. The deny rides an error frame, so the Scan call
	// returns the forbidden error; the connection is spent after, so a fresh client follows.
	if err := cl.Scan(ScanOptions{Prefix: []byte("t2-")}, func(_, _ []byte) error { return nil }); !errors.Is(err, ErrForbidden) {
		t.Fatalf("ro scan out-of-prefix error = %v, want ErrForbidden", err)
	}
	// A whole-keyspace scan is forbidden for a prefix-scoped token.
	cl2 := dialClient(t, addr)
	if _, err := cl2.Authenticate("ro"); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if err := cl2.Scan(ScanOptions{}, func(_, _ []byte) error { return nil }); !errors.Is(err, ErrForbidden) {
		t.Fatalf("ro whole-keyspace scan error = %v, want ErrForbidden", err)
	}
}

func TestBinaryAuthWatchPrefixEnforced(t *testing.T) {
	addr := newAuthBinaryServer(t)
	cl := dialClient(t, addr)
	if _, err := cl.Authenticate("ro"); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	// A watch outside the grant is refused before the feed opens.
	err := cl.Watch(context.Background(), []byte("t2-"), 0, func(kv.Change) error { return nil })
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("ro watch out-of-prefix error = %v, want ErrForbidden", err)
	}
}

func TestBinaryAuthTxnEnforced(t *testing.T) {
	addr := newAuthBinaryServer(t)
	cl := dialClient(t, addr)
	if _, err := cl.Authenticate("rw"); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	// A single-shot transaction touching a forbidden key is refused whole.
	req := TxnRequest{Ops: []Op{
		{Kind: OpSet, Key: []byte("t1-a"), Value: []byte("v")},
		{Kind: OpSet, Key: []byte("t2-a"), Value: []byte("v")},
	}}
	if _, err := cl.Txn(req); !errors.Is(err, ErrForbidden) {
		t.Fatalf("mixed-prefix Txn error = %v, want ErrForbidden", err)
	}
	if _, found, err := cl.Get([]byte("t1-a")); err != nil || found {
		t.Fatalf("allowed key from rejected txn was written: found=%v err=%v", found, err)
	}
}

func TestBinaryAuthInteractiveTxnEnforced(t *testing.T) {
	addr := newAuthBinaryServer(t)
	cl := dialClient(t, addr)
	if _, err := cl.Authenticate("rw"); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	tx, err := cl.BeginTxn(true)
	if err != nil {
		t.Fatalf("BeginTxn: %v", err)
	}
	// A write to the granted prefix inside the transaction is allowed.
	if err := tx.Set([]byte("t1-a"), []byte("v"), 0); err != nil {
		t.Fatalf("txn Set in-prefix: %v", err)
	}
	// A write outside the grant is forbidden at the per-op gate.
	if err := tx.Set([]byte("t2-a"), []byte("v"), 0); !errors.Is(err, ErrForbidden) {
		t.Fatalf("txn Set out-of-prefix error = %v, want ErrForbidden", err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestBinaryAuthDisabledByDefault(t *testing.T) {
	// With no authenticator the server is open: an unauthenticated op succeeds, and an explicit
	// handshake is a no-op that returns an empty identity name.
	cl := newBinaryServer(t)
	if _, err := cl.Set([]byte("anything"), []byte("v"), 0); err != nil {
		t.Fatalf("open-server Set: %v", err)
	}
	if name, err := cl.Authenticate("ignored"); err != nil || name != "" {
		t.Fatalf("open-server Authenticate name=%q err=%v, want empty, no error", name, err)
	}
	if v, found, err := cl.Get([]byte("anything")); err != nil || !found || string(v) != "v" {
		t.Fatalf("open-server Get = %q found=%v err=%v", v, found, err)
	}
}
