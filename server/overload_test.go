package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/kv"
)

// testContext is the background context the overload tests pass to Shutdown.
func testContext() context.Context { return context.Background() }

func TestTokenBucketRefill(t *testing.T) {
	base := time.Unix(0, 0)
	b := &tokenBucket{tokens: 2, rate: 1, burst: 2} // 1 token/sec, ceiling 2, starts full
	// The two starting tokens spend immediately.
	if !b.allow(base) || !b.allow(base) {
		t.Fatalf("first two requests should be admitted")
	}
	// Empty now: a third at the same instant is refused.
	if b.allow(base) {
		t.Fatalf("third request at t=0 should be refused")
	}
	// After one second one token has refilled, admitting exactly one.
	if !b.allow(base.Add(time.Second)) {
		t.Fatalf("request after 1s should be admitted")
	}
	if b.allow(base.Add(time.Second)) {
		t.Fatalf("second request at t=1s should be refused")
	}
	// A long idle refills only to the burst ceiling, not unbounded.
	if !b.allow(base.Add(time.Hour)) || !b.allow(base.Add(time.Hour)) {
		t.Fatalf("two requests after a long idle should be admitted up to the ceiling")
	}
	if b.allow(base.Add(time.Hour)) {
		t.Fatalf("a third after the ceiling refill should be refused")
	}
}

func TestRateLimiterPerKey(t *testing.T) {
	base := time.Unix(0, 0)
	r := newRateLimiter(1, 1) // 1/sec, burst 1
	// Each key has its own bucket: draining one does not affect the other.
	if !r.allow("a", base) {
		t.Fatalf("a's first request should be admitted")
	}
	if r.allow("a", base) {
		t.Fatalf("a's second request should be refused")
	}
	if !r.allow("b", base) {
		t.Fatalf("b's first request should be admitted despite a being drained")
	}
	// A nil limiter admits everything.
	var none *rateLimiter
	if !none.allow("a", base) {
		t.Fatalf("nil limiter should admit")
	}
}

func TestInFlightAcquireRelease(t *testing.T) {
	f := newInFlight(2)
	if !f.acquire() || !f.acquire() {
		t.Fatalf("two acquires within the cap should succeed")
	}
	if f.acquire() {
		t.Fatalf("a third acquire past the cap should fail")
	}
	f.release()
	if !f.acquire() {
		t.Fatalf("an acquire after a release should succeed")
	}
	// A nil limiter (unlimited) always admits and tolerates release.
	var none *inFlight
	if !none.acquire() {
		t.Fatalf("nil in-flight should admit")
	}
	none.release()
}

func TestLimitListenerCaps(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ln := newLimitListener(raw, 1)
	defer ln.Close()

	accepted := make(chan net.Conn, 4)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted <- c
		}
	}()

	// The first connection is accepted; with a cap of one, the second is not until the first closes.
	c1, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial 1: %v", err)
	}
	defer c1.Close()
	var first net.Conn
	select {
	case first = <-accepted:
	case <-time.After(time.Second):
		t.Fatalf("first connection was not accepted")
	}

	c2, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial 2: %v", err)
	}
	defer c2.Close()
	select {
	case <-accepted:
		t.Fatalf("second connection accepted while the cap was full")
	case <-time.After(200 * time.Millisecond):
		// Expected: the slot is held by the first connection.
	}

	// Closing the first frees the slot, so the second is accepted.
	first.Close()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatalf("second connection was not accepted after the first closed")
	}
}

// newOverloadServer builds an in-process HTTP server (no socket) whose handler carries the auth and
// overload middleware, with a global-write token "rw" so a write authorizes and the rate limit keys
// by that identity rather than a shifting remote address.
func newOverloadServer(t *testing.T, opts Options) (*Server, *httptest.Server) {
	t.Helper()
	db, err := kv.Open(t.TempDir() + "/test.kv")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	opts.Auth = NewStaticTokenAuthenticator(map[string]*Identity{
		"rw": {Name: "rw", Grants: []Grant{{Prefix: nil, Write: true}}},
	})
	srv := New(db, opts)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		ts.Close()
		srv.Shutdown(testContext())
		db.Close()
	})
	return srv, ts
}

func TestHTTPRateLimitShed(t *testing.T) {
	_, ts := newOverloadServer(t, Options{RatePerSecond: 1, RateBurst: 1})
	put := func() int {
		req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/kv/k", strings.NewReader("v"))
		req.Header.Set("Authorization", "Bearer rw")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("put: %v", err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	// Burst of one: the first write is admitted, the immediate second is rate-limited.
	if st := put(); st != http.StatusOK {
		t.Fatalf("first write status = %d, want 200", st)
	}
	if st := put(); st != http.StatusTooManyRequests {
		t.Fatalf("second write status = %d, want 429", st)
	}
}

func TestHTTPInFlightShed(t *testing.T) {
	srv, ts := newOverloadServer(t, Options{MaxInFlight: 1})
	// Occupy the single in-flight slot directly, so the next request finds the server at its cap.
	if !srv.inflight.acquire() {
		t.Fatalf("could not take the only in-flight slot")
	}
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/kv/k", nil)
	req.Header.Set("Authorization", "Bearer rw")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("overloaded GET status = %d, want 503", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatalf("overloaded response missing Retry-After")
	}
	// Releasing the slot lets a request through again.
	srv.inflight.release()
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get after release: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode == http.StatusServiceUnavailable {
		t.Fatalf("request after release still shed")
	}
}

func TestHTTPHealthExemptFromOverload(t *testing.T) {
	srv, ts := newOverloadServer(t, Options{MaxInFlight: 1})
	// Even with the only slot taken, the operational routes answer: a probe is never shed.
	srv.inflight.acquire()
	defer srv.inflight.release()
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz under overload status = %d, want 200", resp.StatusCode)
	}
}

func TestBinaryRateLimitShed(t *testing.T) {
	db, err := kv.Open(t.TempDir() + "/test.kv")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := New(db, Options{RatePerSecond: 1, RateBurst: 1})
	go srv.ServeBinary(ln)
	defer srv.Shutdown(testContext())

	cl, err := Dial(ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cl.Close()
	// Open server keyed by remote address: the first write on the connection is admitted, the
	// immediate second is rate-limited with the typed error.
	if _, err := cl.Set([]byte("k"), []byte("v"), 0); err != nil {
		t.Fatalf("first binary set: %v", err)
	}
	if _, err := cl.Set([]byte("k"), []byte("v"), 0); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("second binary set error = %v, want ErrRateLimited", err)
	}
}

func TestCheckpointOnShutdown(t *testing.T) {
	path := t.TempDir() + "/test.kv"
	db, err := kv.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	srv := New(db, Options{CheckpointOnShutdown: true})
	if _, err := srv.Service().Set([]byte("k"), []byte("v"), 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	// Shutdown with the option folds the WAL, so the close that follows and the next open need no
	// recovery. The assertion the test can make portably is that shutdown reports no error and the
	// value survives a reopen.
	if err := srv.Shutdown(testContext()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db2, err := kv.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	v, found, err := NewService(db2).Get([]byte("k"))
	if err != nil || !found || string(v) != "v" {
		t.Fatalf("after checkpoint-on-shutdown reopen: v=%q found=%v err=%v", v, found, err)
	}
}
