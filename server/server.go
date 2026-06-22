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
	srv := &Server{svc: svc, baseCtx: ctx, cancel: cancel, auth: opts.Auth, peerAuth: opts.PeerAuth, tlsConfig: opts.TLSConfig}
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
	ln = srv.wrapTLS(ln)
	srv.http.Addr = ln.Addr().String()
	return srv.http.Serve(ln)
}

// Shutdown stops accepting new connections and waits for in-flight requests to finish, or
// for ctx to expire, then returns. It does not close the database: the caller owns that, and
// closing it after Shutdown returns drains served work first. Later slices fold transaction
// draining and a final checkpoint into this path.
func (srv *Server) Shutdown(ctx context.Context) error {
	srv.cancel()
	err := srv.http.Shutdown(ctx)
	// Force-discard any open interactive transactions and stop their reaper, releasing the
	// snapshots they pin before the caller closes the database (spec 17 §6). This runs after the
	// HTTP drain so a request committing an interactive transaction during the drain still finds
	// its session; anything still open after the drain belongs to a gone client and is discarded.
	srv.svc.Close()
	return err
}
