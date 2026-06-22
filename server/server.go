package server

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"github.com/tamnd/kv"
)

// Server is the running networked face of one database: it owns a Service over an open
// *kv.DB and the net/http machinery that exposes it. It is the unit `kv serve` runs and the
// unit a host embeds when it wants the database reachable over a socket without shelling out.
// One Server serves one database; to share several, run several. The database's lifetime is
// the caller's: Server never opens or closes it, so an embedding host keeps full control of
// when the file is created, recovered, and shut down.
type Server struct {
	svc  *Service
	http *http.Server
	// auth resolves a request's credential to an Identity and is nil when no token authentication
	// is configured. The HTTP middleware and the binary connection loop consult it; with both auth
	// and peerAuth nil every request is allowed, the default for a database served on a trusted
	// socket.
	auth Authenticator
	// peerAuth resolves a verified TLS client certificate to an Identity (mTLS), and is nil when
	// client-certificate authentication is off. When set, a connection that presents a verified
	// certificate authenticates by it without a token; the same per-prefix ACL then applies.
	peerAuth PeerAuthenticator
	// tlsConfig, when set, wraps both the HTTP and the binary listeners in TLS so traffic on the
	// wire is encrypted. It is nil for a plaintext server, which the spec permits only on loopback.
	tlsConfig *tls.Config
	// connLimit caps the simultaneously open connections on each listener; zero is unlimited. It
	// wraps the listener beneath TLS, so it counts TCP connections on either wire.
	connLimit int
	// inflight caps concurrent in-progress requests across both wires and is nil when unlimited; an
	// excess request is shed with ErrOverloaded rather than queued.
	inflight *inFlight
	// rate limits the request rate per caller (per identity, or per remote address when open) and is
	// nil when unlimited; an over-rate request is shed with ErrRateLimited.
	rate *rateLimiter
	// checkpointOnShutdown folds the WAL into the main file during graceful shutdown, so a redeploy
	// leaves the file needing no recovery on the next open (spec 17 §6).
	checkpointOnShutdown bool
	// baseCtx is cancelled by Shutdown before the HTTP server drains, so long-lived
	// streaming handlers (watch) that are idle on a blocking Subscribe observe the shutdown
	// and return instead of pinning the drain open forever. Each watch derives its context
	// from both the request and this, so it ends on either a client disconnect or a server
	// shutdown.
	baseCtx context.Context
	cancel  context.CancelFunc
}

// Options configures a Server. Addr is the listen address for the HTTP surface (for example
// ":8480" or "127.0.0.1:0" to pick a free port in a test); an empty Addr defaults to the
// standard kv port. Limits, when non-nil, replaces the default request limits (spec 17 §6); a
// nil Limits keeps the Service's defaults, which is why it is a pointer rather than a value that
// could not tell "all limits off" from "unset". The two timeouts bound a slow client without
// touching a streaming response: ReadHeaderTimeout caps how long a client may dawdle sending its
// request headers, and IdleTimeout caps how long a kept-alive connection may sit between
// requests; neither limits the duration of a scan or a watch, which by design run as long as
// their data does. The zero value of Options is usable: New fills every default.
type Options struct {
	Addr              string
	Limits            *Limits
	ReadHeaderTimeout time.Duration
	IdleTimeout       time.Duration
	// Auth, when non-nil, turns on token authentication and per-prefix authorization (spec 17 §6):
	// every request outside the operational health and metrics routes must carry a credential the
	// Authenticator resolves to an Identity, and each operation is checked against that identity's
	// grants. A nil Auth leaves token authentication off.
	Auth Authenticator
	// PeerAuth, when non-nil, turns on mTLS client-certificate authentication: a connection that
	// presents a certificate the configured client CA verifies is authenticated by it, and PeerAuth
	// maps that certificate to an Identity whose grants are then enforced exactly as a token's are.
	// It composes with Auth: a request may authenticate by certificate or by token. With both Auth
	// and PeerAuth nil the server is open.
	PeerAuth PeerAuthenticator
	// TLSConfig, when non-nil, wraps both listeners in TLS. For mTLS, set its ClientAuth and
	// ClientCAs so the stack verifies client certificates before PeerAuth ever sees them. A nil
	// TLSConfig serves in the clear, which NonLoopbackRequiresTLS refuses for an off-host bind.
	TLSConfig *tls.Config
	// MaxConns caps the simultaneously open connections on each listener (spec 17 §4); zero leaves
	// connections unbounded. The cap is backpressure on the accept loop: a connection past the cap
	// waits in the kernel's accept queue rather than being served.
	MaxConns int
	// MaxInFlight caps concurrent in-progress requests across both wires (spec 17 §6); zero leaves
	// them unbounded. A request past the cap is shed with a retryable overload error rather than
	// queued, so a request's latency stays bounded under a flood.
	MaxInFlight int
	// RatePerSecond and RateBurst configure a per-caller request rate (spec 17 §6): RatePerSecond is
	// the sustained requests per second a caller may make and RateBurst is how many may arrive at
	// once before the steady rate applies. A zero RatePerSecond leaves the rate unlimited. The limit
	// is keyed per identity when auth is on and per remote address when the server runs open.
	RatePerSecond float64
	RateBurst     int
	// CheckpointOnShutdown folds the WAL into the main file as the last step of a graceful shutdown
	// (spec 17 §6), so a redeploy does not leave the file needing recovery on its next open.
	CheckpointOnShutdown bool
}

// defaultAddr is kv's registered-by-convention HTTP port, used when Options.Addr is empty.
const defaultAddr = ":8480"

// The default HTTP timeouts bound a slow or idle client. They are deliberately the two timeouts
// that do not interfere with a long streaming response: a header-read deadline and a keep-alive
// idle deadline. A blanket read or write deadline is omitted on purpose, since it would cut a
// healthy scan or watch off mid-stream.
const (
	defaultReadHeaderTimeout = 10 * time.Second
	defaultIdleTimeout       = 120 * time.Second
)

// New builds a Server over an already-open database. It wires the Service and the HTTP mux
// but binds no socket; call ListenAndServe or Serve to start accepting. The database must
// outlive the Server.
func New(db *kv.DB, opts Options) *Server {
	addr := opts.Addr
	if addr == "" {
		addr = defaultAddr
	}
	ctx, cancel := context.WithCancel(context.Background())
	svc := NewService(db)
	if opts.Limits != nil {
		svc.SetLimits(*opts.Limits)
	}
	readHeaderTimeout := opts.ReadHeaderTimeout
	if readHeaderTimeout == 0 {
		readHeaderTimeout = defaultReadHeaderTimeout
	}
	idleTimeout := opts.IdleTimeout
	if idleTimeout == 0 {
		idleTimeout = defaultIdleTimeout
	}
	srv := &Server{
		svc:                  svc,
		baseCtx:              ctx,
		cancel:               cancel,
		auth:                 opts.Auth,
		peerAuth:             opts.PeerAuth,
		tlsConfig:            opts.TLSConfig,
		connLimit:            opts.MaxConns,
		inflight:             newInFlight(opts.MaxInFlight),
		rate:                 newRateLimiter(opts.RatePerSecond, opts.RateBurst),
		checkpointOnShutdown: opts.CheckpointOnShutdown,
	}
	srv.http = &http.Server{
		Addr:              addr,
		Handler:           srv.httpHandler(),
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
	}
	return srv
}

// authEnabled reports whether the server enforces access control, which it does when either token
// or client-certificate authentication is configured. With neither, the server is open and both
// the HTTP middleware and the binary gate pass everything through, the default for a trusted
// socket.
func (srv *Server) authEnabled() bool { return srv.auth != nil || srv.peerAuth != nil }

// Service returns the transport-agnostic core, for a host that wants to drive operations in
// process alongside the served surface, or to mount the handler itself.
func (srv *Server) Service() *Service { return srv.svc }

// Handler returns the HTTP handler, so a host can mount the database under its own mux or
// wrap it in middleware instead of letting Server own the listener.
func (srv *Server) Handler() http.Handler { return srv.http.Handler }

// Addr returns the configured listen address.
func (srv *Server) Addr() string { return srv.http.Addr }

// ListenAndServe binds the HTTP listener and serves until the server is shut down or the
// listener fails, the one-call path a host takes when it has no listener of its own. It returns
// http.ErrServerClosed on a clean Shutdown, which the caller treats as success. When a TLS config
// is set, the bound listener is wrapped in TLS, so this one call serves HTTPS.
func (srv *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", srv.http.Addr)
	if err != nil {
		return err
	}
	return srv.Serve(ln)
}

// Serve serves HTTP on an already-bound listener, the path `kv serve` and the tests take after
// opening a listener on port zero so they know the real address before traffic starts. When a TLS
// config is set the listener is wrapped in TLS, so a connection accepted from it is encrypted; the
// handlers are unchanged either way, since the TLS termination is transparent to them.
func (srv *Server) Serve(ln net.Listener) error {
	ln = srv.wrapListener(ln)
	srv.http.Addr = ln.Addr().String()
	return srv.http.Serve(ln)
}

// wrapListener applies the connection cap and then TLS to a raw listener, the shared wrap both
// faces take. The connection limit is innermost so it counts TCP connections regardless of TLS, and
// TLS is outermost so a connection accepted from the result is already encrypted; either wrap is a
// no-op when its feature is off.
func (srv *Server) wrapListener(ln net.Listener) net.Listener {
	return srv.wrapTLS(newLimitListener(ln, srv.connLimit))
}

// Shutdown stops accepting new connections and waits for in-flight requests to finish, or
// for ctx to expire, then returns. It does not close the database: the caller owns that, and
// closing it after Shutdown returns drains served work first.
//
// The order is deliberate (spec 17 §6): cancel the base context so streaming handlers and parked
// binary connections observe the shutdown, drain the HTTP server, force-discard any open
// interactive transactions so no snapshot is pinned, and finally, when configured, fold the WAL into
// the main file with a checkpoint so a redeploy leaves the file needing no recovery on its next
// open. The transaction discard runs after the HTTP drain so a request committing during the drain
// still finds its session; the checkpoint runs after the discard so no open snapshot holds back the
// fold. Releasing the file lock is the caller's Close, since the caller owns the database.
func (srv *Server) Shutdown(ctx context.Context) error {
	srv.cancel()
	err := srv.http.Shutdown(ctx)
	srv.svc.Close()
	if srv.checkpointOnShutdown {
		if cerr := srv.svc.Checkpoint(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}
