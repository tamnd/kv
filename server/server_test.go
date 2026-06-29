package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tamnd/kv"
)

// newTestServer opens a fresh temp database and mounts the HTTP handler on an httptest
// server, returning the server and the open db so a test can drive the wire surface and
// inspect the library state behind it. The cleanup closes both.
func newTestServer(t *testing.T) (*httptest.Server, *kv.DB) {
	t.Helper()
	path := t.TempDir() + "/test.kv"
	db, err := kv.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	srv := New(db, Options{})
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		// Close the database before the HTTP server: a closed database releases any idle
		// watch handler (Subscribe returns ErrClosed), so the server's drain has nothing to
		// wait on. The reverse order would deadlock, since hs.Close blocks on an in-flight
		// watch that only the database close can wake.
		db.Close()
		hs.Close()
	})
	return hs, db
}

// do issues a request and returns the status and body, failing the test on a transport
// error.
func do(t *testing.T, method, url string, body io.Reader) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func TestPutGetDelete(t *testing.T) {
	hs, _ := newTestServer(t)

	// PUT a value, then GET it back as the raw body.
	code, body := do(t, http.MethodPut, hs.URL+"/v1/kv/alpha", strings.NewReader("hello"))
	if code != http.StatusOK {
		t.Fatalf("put status = %d, body %s", code, body)
	}
	var vr versionResponse
	if err := json.Unmarshal(body, &vr); err != nil {
		t.Fatalf("decode version: %v", err)
	}
	if vr.Version == 0 {
		t.Fatalf("put returned zero version")
	}

	code, body = do(t, http.MethodGet, hs.URL+"/v1/kv/alpha", nil)
	if code != http.StatusOK {
		t.Fatalf("get status = %d", code)
	}
	if string(body) != "hello" {
		t.Fatalf("get body = %q, want hello", body)
	}

	// DELETE then GET should 404.
	code, _ = do(t, http.MethodDelete, hs.URL+"/v1/kv/alpha", nil)
	if code != http.StatusOK {
		t.Fatalf("delete status = %d", code)
	}
	code, _ = do(t, http.MethodGet, hs.URL+"/v1/kv/alpha", nil)
	if code != http.StatusNotFound {
		t.Fatalf("get after delete status = %d, want 404", code)
	}
}

func TestGetMissingIs404(t *testing.T) {
	hs, _ := newTestServer(t)
	code, _ := do(t, http.MethodGet, hs.URL+"/v1/kv/nope", nil)
	if code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", code)
	}
}

func TestBatch(t *testing.T) {
	hs, _ := newTestServer(t)
	req := jsonBatchRequest{Ops: []jsonOp{
		{Kind: string(OpSet), Key: b64("x"), Value: b64("1")},
		{Kind: string(OpSet), Key: b64("y"), Value: b64("2")},
		{Kind: string(OpDelete), Key: b64("x")},
	}}
	code, body := do(t, http.MethodPost, hs.URL+"/v1/batch", jsonBody(t, req))
	if code != http.StatusOK {
		t.Fatalf("batch status = %d, body %s", code, body)
	}
	code, _ = do(t, http.MethodGet, hs.URL+"/v1/kv/x", nil)
	if code != http.StatusNotFound {
		t.Fatalf("x should be deleted, status = %d", code)
	}
	code, gb := do(t, http.MethodGet, hs.URL+"/v1/kv/y", nil)
	if code != http.StatusOK || string(gb) != "2" {
		t.Fatalf("y = %q status %d", gb, code)
	}
}

func TestTxnCompareAndSet(t *testing.T) {
	hs, _ := newTestServer(t)
	do(t, http.MethodPut, hs.URL+"/v1/kv/k", strings.NewReader("old"))

	// Assert k == "old" then set it to "new": succeeds.
	req := jsonTxnRequest{
		Asserts: []jsonAssert{{Key: b64("k"), ExpectValue: b64("old")}},
		Ops:     []jsonOp{{Kind: string(OpSet), Key: b64("k"), Value: b64("new")}},
	}
	code, body := do(t, http.MethodPost, hs.URL+"/v1/txn", jsonBody(t, req))
	if code != http.StatusOK {
		t.Fatalf("txn status = %d, body %s", code, body)
	}
	code, gb := do(t, http.MethodGet, hs.URL+"/v1/kv/k", nil)
	if code != http.StatusOK || string(gb) != "new" {
		t.Fatalf("k = %q, want new", gb)
	}

	// Assert the stale value "old" again: now fails with 409 and changes nothing.
	code, _ = do(t, http.MethodPost, hs.URL+"/v1/txn", jsonBody(t, req))
	if code != http.StatusConflict {
		t.Fatalf("stale assert status = %d, want 409", code)
	}
	code, gb = do(t, http.MethodGet, hs.URL+"/v1/kv/k", nil)
	if string(gb) != "new" {
		t.Fatalf("k changed after failed assert: %q", gb)
	}
}

func TestTxnReads(t *testing.T) {
	hs, _ := newTestServer(t)
	do(t, http.MethodPut, hs.URL+"/v1/kv/present", strings.NewReader("yes"))

	req := jsonTxnRequest{Ops: []jsonOp{
		{Kind: string(OpGet), Key: b64("present")},
		{Kind: string(OpGet), Key: b64("absent")},
		{Kind: string(OpExists), Key: b64("present")},
	}}
	code, body := do(t, http.MethodPost, hs.URL+"/v1/txn", jsonBody(t, req))
	if code != http.StatusOK {
		t.Fatalf("txn status = %d, body %s", code, body)
	}
	var res jsonTxnResponse
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(res.Reads) != 3 {
		t.Fatalf("reads = %d, want 3", len(res.Reads))
	}
	if !res.Reads[0].Found || decodeB64(t, res.Reads[0].Value) != "yes" {
		t.Fatalf("read 0 = %+v", res.Reads[0])
	}
	if res.Reads[1].Found {
		t.Fatalf("read 1 should be a miss")
	}
	if !res.Reads[2].Found {
		t.Fatalf("read 2 exists should be true")
	}
}

func TestEncodingSelectors(t *testing.T) {
	hs, _ := newTestServer(t)
	// A key with bytes that do not survive a raw URL path, addressed via base64.
	rawKey := []byte{0x00, 0xff, 0x2f}
	enc := base64.StdEncoding.EncodeToString(rawKey)
	code, _ := do(t, http.MethodPut, hs.URL+"/v1/kv/"+enc+"?encoding=base64", strings.NewReader("z"))
	if code != http.StatusOK {
		t.Fatalf("put status = %d", code)
	}
	code, body := do(t, http.MethodGet, hs.URL+"/v1/kv/"+enc+"?encoding=base64", nil)
	if code != http.StatusOK || string(body) != "z" {
		t.Fatalf("get encoded key = %q status %d", body, code)
	}
}

func TestStatsAndHealth(t *testing.T) {
	hs, _ := newTestServer(t)
	code, body := do(t, http.MethodGet, hs.URL+"/v1/stats", nil)
	if code != http.StatusOK {
		t.Fatalf("stats status = %d", code)
	}
	var stats map[string]any
	if err := json.Unmarshal(body, &stats); err != nil {
		t.Fatalf("stats not JSON: %v", err)
	}

	code, body = do(t, http.MethodGet, hs.URL+"/healthz", nil)
	if code != http.StatusOK || !strings.Contains(string(body), "ok") {
		t.Fatalf("healthz = %q status %d", body, code)
	}

	code, body = do(t, http.MethodGet, hs.URL+"/metrics", nil)
	if code != http.StatusOK || len(body) == 0 {
		t.Fatalf("metrics status = %d len %d", code, len(body))
	}
}

func TestCheckpoint(t *testing.T) {
	hs, _ := newTestServer(t)
	do(t, http.MethodPut, hs.URL+"/v1/kv/k", strings.NewReader("v"))
	code, _ := do(t, http.MethodPost, hs.URL+"/v1/checkpoint", nil)
	if code != http.StatusOK {
		t.Fatalf("checkpoint status = %d", code)
	}
}

func TestTTLExpiry(t *testing.T) {
	hs, _ := newTestServer(t)
	code, _ := do(t, http.MethodPut, hs.URL+"/v1/kv/eph?ttl=10s", strings.NewReader("v"))
	if code != http.StatusOK {
		t.Fatalf("put with ttl status = %d", code)
	}
	// The key is present immediately after the write; the TTL is honored, not just accepted.
	code, _ = do(t, http.MethodGet, hs.URL+"/v1/kv/eph", nil)
	if code != http.StatusOK {
		t.Fatalf("get within ttl status = %d, want 200", code)
	}
}

// b64 base64-encodes a string for a wire op field.
func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// decodeB64 decodes a wire byte field back to a string.
func decodeB64(t *testing.T, s string) string {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Fatalf("decode b64 %q: %v", s, err)
	}
	return string(b)
}

// jsonBody marshals v to a request body.
func jsonBody(t *testing.T, v any) io.Reader {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return bytes.NewReader(b)
}
