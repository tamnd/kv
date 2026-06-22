package server

import (
	"context"
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
	// auth resolves a request's credential to an Identity and is nil when the server runs open.
	// The HTTP middleware and, in a later slice, the binary connection loop consult it; a nil
	// auth means every request is allowed, the default for a database served on a trusted socket.
	auth Authenticator
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
	// Auth, when non-nil, turns on authentication and per-prefix authorization (spec 17 §6):
	// every request outside the operational health and metrics routes must carry a credential the
	// Authenticator resolves to an Identity, and each operation is checked against that identity's
	// grants. A nil Auth leaves the server open, the default, for a database on a trusted socket.
	Auth Authenticator
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
	srv := &Server{svc: svc, baseCtx: ctx, cancel: cancel, auth: opts.Auth}
	srv.http = &http.Server{
		Addr:              addr,
		Handler:           srv.httpHandler(),
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
	}
	return srv
}

// Service returns the transport-agnostic core, for a host that wants to drive operations in
// process alongside the served surface, or to mount the handler itself.
func (srv *Server) Service() *Service { return srv.svc }

// Handler returns the HTTP handler, so a host can mount the database under its own mux or
// wrap it in middleware instead of letting Server own the listener.
func (srv *Server) Handler() http.Handler { return srv.http.Handler }

// Addr returns the configured listen address.
func (srv *Server) Addr() string { return srv.http.Addr }

// ListenAndServe binds the HTTP listener and serves until the server is shut down or the
// listener fails, the one-call path `kv serve` takes. It returns http.ErrServerClosed on a
// clean Shutdown, which the caller treats as success.
func (srv *Server) ListenAndServe() error {
	return srv.http.ListenAndServe()
}

// Serve serves HTTP on an already-bound listener, the path a test takes after opening a
// listener on port zero so it knows the real address before traffic starts.
func (srv *Server) Serve(ln net.Listener) error {
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
