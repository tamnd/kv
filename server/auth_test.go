package server

import "testing"

// readerOn and writerOn build single-grant identities for the ACL table tests.
func readerOn(prefix string) *Identity {
	return &Identity{Name: "r", Grants: []Grant{{Prefix: []byte(prefix)}}}
}
func writerOn(prefix string) *Identity {
	return &Identity{Name: "w", Grants: []Grant{{Prefix: []byte(prefix), Write: true}}}
}

func TestACLPointReadWrite(t *testing.T) {
	r := readerOn("tenant1/")
	w := writerOn("tenant1/")
	admin := &Identity{Name: "a", Admin: true}

	// A read grant authorizes reads under its prefix but no writes anywhere.
	if !r.canRead([]byte("tenant1/key")) {
		t.Fatalf("reader cannot read its own prefix")
	}
	if r.canRead([]byte("tenant2/key")) {
		t.Fatalf("reader can read another prefix")
	}
	if r.canWrite([]byte("tenant1/key")) {
		t.Fatalf("read-only grant authorized a write")
	}

	// A write grant authorizes both reads and writes under its prefix, and nothing outside it.
	if !w.canRead([]byte("tenant1/key")) || !w.canWrite([]byte("tenant1/key")) {
		t.Fatalf("writer cannot read or write its own prefix")
	}
	if w.canWrite([]byte("tenant2/key")) {
		t.Fatalf("writer can write outside its prefix")
	}

	// Admin reads and writes anything.
	if !admin.canRead([]byte("x")) || !admin.canWrite([]byte("y")) {
		t.Fatalf("admin denied")
	}
}

func TestACLMultipleGrants(t *testing.T) {
	// A token may carry read-only on one prefix and read-write on another.
	id := &Identity{Name: "m", Grants: []Grant{
		{Prefix: []byte("ro/")},
		{Prefix: []byte("rw/"), Write: true},
	}}
	if !id.canRead([]byte("ro/x")) || id.canWrite([]byte("ro/x")) {
		t.Fatalf("ro/ should be read-only")
	}
	if !id.canRead([]byte("rw/x")) || !id.canWrite([]byte("rw/x")) {
		t.Fatalf("rw/ should be read-write")
	}
	if id.canRead([]byte("other/x")) {
		t.Fatalf("ungranted prefix should be denied")
	}
}

func TestACLScan(t *testing.T) {
	r := readerOn("t/")
	global := &Identity{Name: "g", Grants: []Grant{{Prefix: nil}}}

	// A scan confined to a covered prefix is allowed; one that reaches outside is not.
	if !r.canReadScan([]byte("t/")) {
		t.Fatalf("scan of granted prefix denied")
	}
	if !r.canReadScan([]byte("t/sub")) {
		t.Fatalf("scan of a sub-prefix of the grant denied")
	}
	if r.canReadScan([]byte("u/")) {
		t.Fatalf("scan of ungranted prefix allowed")
	}
	// A whole-keyspace scan (empty prefix) needs a global read grant.
	if r.canReadScan(nil) {
		t.Fatalf("prefix-scoped reader allowed a whole-keyspace scan")
	}
	if !global.canReadScan(nil) {
		t.Fatalf("global reader denied a whole-keyspace scan")
	}
}

func TestACLDoOp(t *testing.T) {
	id := &Identity{Name: "m", Grants: []Grant{
		{Prefix: []byte("ro/")},
		{Prefix: []byte("rw/"), Write: true},
	}}
	cases := []struct {
		op   Op
		want bool
	}{
		{Op{Kind: OpGet, Key: []byte("ro/x")}, true},
		{Op{Kind: OpExists, Key: []byte("rw/x")}, true},
		{Op{Kind: OpSet, Key: []byte("ro/x")}, false}, // write to read-only prefix
		{Op{Kind: OpSet, Key: []byte("rw/x")}, true},
		{Op{Kind: OpDelete, Key: []byte("other/x")}, false},
		{Op{Kind: OpMerge, Key: []byte("rw/x")}, true},
		{Op{Kind: OpGet, Key: []byte("nope")}, false},
	}
	for _, c := range cases {
		if got := id.canDoOp(c.op); got != c.want {
			t.Fatalf("canDoOp(%s %q) = %v, want %v", c.op.Kind, c.op.Key, got, c.want)
		}
	}
}

func TestStaticTokenAuthenticator(t *testing.T) {
	alice := &Identity{Name: "alice", Grants: []Grant{{Prefix: []byte("a/"), Write: true}}}
	a := NewStaticTokenAuthenticator(map[string]*Identity{"tok-alice": alice})

	if id, ok := a.Authenticate("tok-alice"); !ok || id.Name != "alice" {
		t.Fatalf("known token did not resolve to its identity")
	}
	if _, ok := a.Authenticate("nope"); ok {
		t.Fatalf("unknown token authenticated")
	}
	if _, ok := a.Authenticate(""); ok {
		t.Fatalf("empty credential authenticated")
	}
}

func TestStaticTokenAuthenticatorCopiesTable(t *testing.T) {
	m := map[string]*Identity{"t": {Name: "x"}}
	a := NewStaticTokenAuthenticator(m)
	// Mutating the caller's map after construction must not change what the authenticator knows.
	delete(m, "t")
	if _, ok := a.Authenticate("t"); !ok {
		t.Fatalf("authenticator table changed when caller's map was mutated")
	}
}
