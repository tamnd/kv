package server

import (
	"errors"
	"net"
	"sync"
	"time"
)

// This file holds the overload guardrails (spec 17 §4, §6): the connection limit, the in-flight
// request limit, and the per-identity rate limit. They are distinct from the size limits in
// limits.go, which bound how big one request may be; these bound how many requests, connections,
// and how fast, so a process serving many clients is protected from being swamped by their number
// rather than by one request's size. All three are off by default (a zero in Options), so a
// database on a trusted socket pays nothing, and all three apply identically to the HTTP and the
// binary faces, since both go through the same listener wrap and consult the same limiters.

// ErrOverloaded is returned when the server is already serving its configured maximum of in-flight
// requests and sheds a new one rather than queue it without bound. It is a transient condition, so
// the HTTP adapter maps it to 503 with a Retry-After and the binary adapter to statusOverloaded; a
// well-behaved client backs off and retries.
var ErrOverloaded = errors.New("kv: server overloaded, too many in-flight requests")

// ErrRateLimited is returned when a caller exceeds its configured request rate. Like ErrOverloaded
// it is transient and retryable; the HTTP adapter maps it to 429 with a Retry-After and the binary
// adapter to statusRateLimited.
var ErrRateLimited = errors.New("kv: rate limit exceeded")

// limitListener caps the number of simultaneously open connections on a listener. Accept blocks
// until a slot frees rather than rejecting outright, so the cap is backpressure on the accept loop:
// new connections wait in the kernel's accept queue instead of piling onto the process. It wraps
// any net.Listener, so both the HTTP and the binary listeners get the same cap from one wrap, and it
// sits beneath the TLS wrap so it counts TCP connections regardless of whether they are encrypted. A
// non-positive limit returns the listener unwrapped, the disabled default.
type limitListener struct {
	net.Listener
	sem chan struct{}
}

func newLimitListener(ln net.Listener, n int) net.Listener {
	if n <= 0 {
		return ln
	}
	return &limitListener{Listener: ln, sem: make(chan struct{}, n)}
}

// Accept acquires a slot before accepting, so the listener holds at most cap connections open. If
// the underlying Accept fails the slot is returned at once, since no connection will close to
// return it.
func (l *limitListener) Accept() (net.Conn, error) {
	l.sem <- struct{}{}
	conn, err := l.Listener.Accept()
	if err != nil {
		<-l.sem
		return nil, err
	}
	return &limitConn{Conn: conn, release: func() { <-l.sem }}, nil
}

// limitConn returns its slot to the listener when it is closed. The release runs once however many
// times Close is called, since net/http and the binary loop may both close a connection on their
// own paths and a double release would free a slot that was never taken.
type limitConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *limitConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

// inFlight is a counting semaphore over concurrent in-progress requests. acquire is non-blocking:
// it takes a slot if one is free and reports failure if the server is already at its limit, so an
// excess request is shed rather than queued, which keeps a request's latency bounded instead of
// letting a backlog grow without end. A nil *inFlight is the disabled state and admits everything,
// so the zero configuration costs nothing.
type inFlight struct {
	sem chan struct{}
}

func newInFlight(n int) *inFlight {
	if n <= 0 {
		return nil
	}
	return &inFlight{sem: make(chan struct{}, n)}
}

func (f *inFlight) acquire() bool {
	if f == nil {
		return true
	}
	select {
	case f.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (f *inFlight) release() {
	if f == nil {
		return
	}
	<-f.sem
}

// tokenBucket is a single rate-limited stream: it refills at a steady rate up to a burst ceiling and
// spends one token per request. It is the classic token bucket, which allows a short burst (up to
// the ceiling) while bounding the long-run average, the shape that fits a request rate where a
// client legitimately sends a flurry now and then but should not sustain an unbounded rate.
type tokenBucket struct {
	mu     sync.Mutex
	tokens float64
	last   time.Time
	rate   float64 // tokens added per second
	burst  float64 // ceiling on accumulated tokens
}

// allow refills the bucket for the elapsed time and spends a token if one is available, reporting
// whether the request is admitted. now is passed in so the caller reads the clock once per request
// and the bucket's arithmetic is deterministic in a test that controls it.
func (b *tokenBucket) allow(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.last.IsZero() {
		b.tokens += now.Sub(b.last).Seconds() * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
	}
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// rateLimiter holds one token bucket per caller key, keyed by identity name when the server
// authenticates and by remote address when it runs open, so a rate limit is per-token where a token
// names the caller and per-connection-origin otherwise. A nil *rateLimiter is the disabled state and
// admits everything. The bucket map grows with the set of distinct callers seen; for a server with a
// fixed token table that is bounded by the table, and for an open server it is bounded by the set of
// client addresses, which a connection limit already caps.
type rateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
	rate    float64
	burst   float64
}

func newRateLimiter(perSecond float64, burst int) *rateLimiter {
	if perSecond <= 0 {
		return nil
	}
	b := float64(burst)
	if b < 1 {
		b = perSecond // a burst at least the per-second rate, so one second's worth may arrive at once
	}
	return &rateLimiter{buckets: map[string]*tokenBucket{}, rate: perSecond, burst: b}
}

// allow admits or rejects one request from the caller named by key, against that caller's own
// bucket. A new key starts with a full bucket, so a caller's first request is never spuriously
// limited.
func (r *rateLimiter) allow(key string, now time.Time) bool {
	if r == nil {
		return true
	}
	r.mu.Lock()
	b := r.buckets[key]
	if b == nil {
		b = &tokenBucket{tokens: r.burst, rate: r.rate, burst: r.burst}
		r.buckets[key] = b
	}
	r.mu.Unlock()
	return b.allow(now)
}

// rateKey names the caller a rate limit applies to: the authenticated identity's name when there is
// one, falling back to the connection origin (a remote address) for an open server where no identity
// distinguishes callers. Keying by identity is what makes the limit per-token, so two connections
// sharing a token share its budget rather than each getting a full one.
func rateKey(id *Identity, fallback string) string {
	if id != nil && id.Name != "" {
		return id.Name
	}
	return fallback
}
