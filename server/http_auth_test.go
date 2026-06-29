package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tamnd/kv"
)

// newAuthHTTPServer mounts the HTTP handler with a fixed token table: an admin token, a token with
// read-write on "t1-", and a token with read-only on "t1-". It returns the base URL.
func newAuthHTTPServer(t *testing.T) string {
	t.Helper()
	db, err := kv.Open(t.TempDir() + "/test.kv")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// The prefixes use a hyphen separator rather than a slash because the point routes match a
	// single path segment; the prefix-ACL logic is identical either way and the slash form is
	// covered by the model tests in auth_test.go.
	auth := NewStaticTokenAuthenticator(map[string]*Identity{
		"admin": {Name: "admin", Admin: true},
		"rw":    {Name: "rw", Grants: []Grant{{Prefix: []byte("t1-"), Write: true}}},
		"ro":    {Name: "ro", Grants: []Grant{{Prefix: []byte("t1-")}}},
	})
	srv := New(db, Options{Auth: auth})
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		db.Close()
		hs.Close()
	})
	return hs.URL
}

// doAuth issues a request carrying a bearer token (when non-empty) and returns the status and body.
func doAuth(t *testing.T, token, method, url string, body io.Reader) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func TestHTTPAuthRequiresCredential(t *testing.T) {
	ts := newAuthHTTPServer(t)
	// No token at all is 401 on a keyed route.
	if status, _ := doAuth(t, "", http.MethodGet, ts+"/v1/kv/t1-a", nil); status != http.StatusUnauthorized {
		t.Fatalf("no-token GET status = %d, want 401", status)
	}
	// A bad token is also 401.
	if status, _ := doAuth(t, "bogus", http.MethodGet, ts+"/v1/kv/t1-a", nil); status != http.StatusUnauthorized {
		t.Fatalf("bad-token GET status = %d, want 401", status)
	}
}

func TestHTTPAuthHealthExempt(t *testing.T) {
	ts := newAuthHTTPServer(t)
	// Health and metrics are reachable without a credential even with auth on.
	if status, _ := doAuth(t, "", http.MethodGet, ts+"/healthz", nil); status != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200", status)
	}
	if status, _ := doAuth(t, "", http.MethodGet, ts+"/metrics", nil); status != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", status)
	}
}

func TestHTTPAuthPrefixEnforced(t *testing.T) {
	ts := newAuthHTTPServer(t)

	// The rw token writes and reads its own prefix.
	if status, _ := doAuth(t, "rw", http.MethodPut, ts+"/v1/kv/t1-a", strings.NewReader("v")); status != http.StatusOK {
		t.Fatalf("rw PUT in-prefix status = %d, want 200", status)
	}
	if status, _ := doAuth(t, "rw", http.MethodGet, ts+"/v1/kv/t1-a", nil); status != http.StatusOK {
		t.Fatalf("rw GET in-prefix status = %d, want 200", status)
	}
	// The rw token cannot touch another prefix: 403.
	if status, _ := doAuth(t, "rw", http.MethodPut, ts+"/v1/kv/t2-a", strings.NewReader("v")); status != http.StatusForbidden {
		t.Fatalf("rw PUT out-of-prefix status = %d, want 403", status)
	}
	if status, _ := doAuth(t, "rw", http.MethodGet, ts+"/v1/kv/t2-a", nil); status != http.StatusForbidden {
		t.Fatalf("rw GET out-of-prefix status = %d, want 403", status)
	}
}

func TestHTTPAuthReadOnlyCannotWrite(t *testing.T) {
	ts := newAuthHTTPServer(t)
	// The ro token reads its prefix but cannot write it.
	if status, _ := doAuth(t, "ro", http.MethodPut, ts+"/v1/kv/t1-a", strings.NewReader("v")); status != http.StatusForbidden {
		t.Fatalf("ro PUT status = %d, want 403", status)
	}
	// Seed via the rw token, then the ro token may read it.
	doAuth(t, "rw", http.MethodPut, ts+"/v1/kv/t1-a", strings.NewReader("v"))
	if status, _ := doAuth(t, "ro", http.MethodGet, ts+"/v1/kv/t1-a", nil); status != http.StatusOK {
		t.Fatalf("ro GET status = %d, want 200", status)
	}
	// The ro token cannot delete either.
	if status, _ := doAuth(t, "ro", http.MethodDelete, ts+"/v1/kv/t1-a", nil); status != http.StatusForbidden {
		t.Fatalf("ro DELETE status = %d, want 403", status)
	}
}

func TestHTTPAuthOpsEndpointsAdminOnly(t *testing.T) {
	ts := newAuthHTTPServer(t)
	// A non-admin token is forbidden from the ops endpoints.
	for _, route := range []struct {
		method, path string
	}{
		{http.MethodGet, "/v1/stats"},
		{http.MethodGet, "/v1/info"},
		{http.MethodPost, "/v1/checkpoint"},
	} {
		if status, _ := doAuth(t, "rw", route.method, ts+route.path, nil); status != http.StatusForbidden {
			t.Fatalf("rw %s %s status = %d, want 403", route.method, route.path, status)
		}
		if status, _ := doAuth(t, "admin", route.method, ts+route.path, nil); status != http.StatusOK {
			t.Fatalf("admin %s %s status = %d, want 200", route.method, route.path, status)
		}
	}
}

func TestHTTPAuthRangeDeleteEnforced(t *testing.T) {
	ts := newAuthHTTPServer(t)
	// A range delete inside the grant is allowed.
	if status, _ := doAuth(t, "rw", http.MethodDelete, ts+"/v1/kv?from=t1-a&to=t1-z", nil); status != http.StatusOK {
		t.Fatalf("rw range delete in-prefix status = %d, want 200", status)
	}
	// One that escapes the grant is forbidden.
	if status, _ := doAuth(t, "rw", http.MethodDelete, ts+"/v1/kv?from=t1-a&to=u", nil); status != http.StatusForbidden {
		t.Fatalf("rw range delete escaping prefix status = %d, want 403", status)
	}
}

func TestHTTPAuthBatchAllOrNothing(t *testing.T) {
	ts := newAuthHTTPServer(t)
	// A batch mixing an allowed and a forbidden key is rejected as a whole, before any write, so
	// the allowed half does not commit.
	body := `{"ops":[` +
		`{"kind":"set","key":"` + b64("t1-a") + `","value":"` + b64("v") + `"},` +
		`{"kind":"set","key":"` + b64("t2-a") + `","value":"` + b64("v") + `"}]}`
	if status, _ := doAuth(t, "rw", http.MethodPost, ts+"/v1/batch", strings.NewReader(body)); status != http.StatusForbidden {
		t.Fatalf("mixed-prefix batch status = %d, want 403", status)
	}
	// The allowed key must not have been written.
	if status, _ := doAuth(t, "rw", http.MethodGet, ts+"/v1/kv/t1-a", nil); status != http.StatusNotFound {
		t.Fatalf("allowed key from rejected batch was written: GET status = %d, want 404", status)
	}
}

func TestHTTPAuthDisabledByDefault(t *testing.T) {
	// With no authenticator the server is open: an unauthenticated write and read both succeed.
	hs, _ := newTestServer(t)
	if status, _ := doAuth(t, "", http.MethodPut, hs.URL+"/v1/kv/anything", strings.NewReader("v")); status != http.StatusOK {
		t.Fatalf("open-server PUT status = %d, want 200", status)
	}
	if status, _ := doAuth(t, "", http.MethodGet, hs.URL+"/v1/kv/anything", nil); status != http.StatusOK {
		t.Fatalf("open-server GET status = %d, want 200", status)
	}
}
