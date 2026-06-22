package server

import (
	"context"
	"net/http"
	"strings"
)

// This file is the HTTP half of authentication and authorization (spec 17 §6). It sits between the
// router and the handlers as two thin layers. The middleware runs first on every request: when the
// server has an authenticator configured it reads the request's credential, resolves it to an
// Identity, and stashes that Identity on the request context, rejecting an unknown or missing
// credential with 401 before any handler runs. The per-handler authorize helper runs second,
// inside each handler once it knows which keys the request touches, and refuses with 403 when the
// resolved Identity lacks a grant for those keys. Splitting it this way keeps authentication in one
// place while letting authorization see the decoded keys, which only the handler has.
//
// When no authenticator is configured the server is open: the middleware passes every request
// through and the authorize helper allows everything, so a database served on a trusted socket
// needs no tokens. Turning auth on is purely additive, which keeps the default behavior the same
// as before this slice.

// identityContextKey is the unexported key under which the middleware stores the authenticated
// Identity on a request context. Being unexported, no other package can read or forge it.
type identityContextKey struct{}

// withIdentity returns a copy of ctx carrying id, for the middleware to attach a resolved identity
// that downstream handlers read with identityFrom.
func withIdentity(ctx context.Context, id *Identity) context.Context {
	return context.WithValue(ctx, identityContextKey{}, id)
}

// identityFrom returns the identity the middleware attached, or nil when the request carried none
// (which happens only on an exempt route or when auth is disabled).
func identityFrom(ctx context.Context) *Identity {
	id, _ := ctx.Value(identityContextKey{}).(*Identity)
	return id
}

// authExemptPaths are the operational routes reachable without a credential even when auth is on:
// liveness, readiness, and the metrics scrape. They expose no key data and are the paths a load
// balancer or a Prometheus scraper hits without an identity, so requiring a token on them would
// break health checking for no security gain.
func isAuthExempt(path string) bool {
	switch path {
	case "/healthz", "/readyz", "/metrics":
		return true
	default:
		return false
	}
}

// authMiddleware wraps the router with authentication. With no authenticator it is a pass-through,
// preserving the open default. With one, it lets the exempt operational routes through unchecked
// and, for every other route, extracts the bearer credential, authenticates it, and either attaches
// the resolved identity to the request context or rejects the request with 401. A handler that runs
// after this middleware is therefore guaranteed an identity in context whenever auth is on.
func (srv *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if srv.auth == nil || isAuthExempt(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		id, ok := srv.auth.Authenticate(bearerToken(r))
		if !ok {
			// Advertise bearer auth so a well-behaved client knows how to retry, then refuse. The
			// body names the failure without leaking which credential was tried.
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, ErrUnauthenticated.Error(), http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(withIdentity(r.Context(), id)))
	})
}

// bearerToken extracts the credential a request carries: an Authorization: Bearer <token> header,
// or failing that an X-Api-Token header, the two ways a client presents a static token. It returns
// the empty string when neither is present, which never authenticates.
func bearerToken(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		const prefix = "Bearer "
		if len(h) >= len(prefix) && strings.EqualFold(h[:len(prefix)], prefix) {
			return strings.TrimSpace(h[len(prefix):])
		}
	}
	return strings.TrimSpace(r.Header.Get("X-Api-Token"))
}

// authorize is the per-handler authorization gate. With auth disabled it allows everything. With
// auth on, it reads the identity the middleware attached and runs the supplied predicate against
// it, returning ErrForbidden when the predicate denies and ErrUnauthenticated in the defensive case
// that no identity is present (which the middleware should have prevented). The handler passes a
// predicate naming exactly the access the request needs, for example read of one key or write of a
// range, so authorization is expressed where the keys are known.
func (srv *Server) authorize(r *http.Request, allowed func(*Identity) bool) error {
	if srv.auth == nil {
		return nil
	}
	id := identityFrom(r.Context())
	if id == nil {
		return ErrUnauthenticated
	}
	if !allowed(id) {
		return ErrForbidden
	}
	return nil
}

// authorizeOps authorizes a whole transaction or batch: every assert is a read of its key and
// every op is checked by its kind, so the request is allowed only when the identity may perform all
// of it. Checking the entire set before applying any of it keeps a partially-authorized request
// from committing the part it was allowed, which would violate the atomicity the request promises.
func (srv *Server) authorizeOps(r *http.Request, asserts []Assert, ops []Op) error {
	return srv.authorize(r, func(id *Identity) bool {
		for _, a := range asserts {
			if !id.canRead(a.Key) {
				return false
			}
		}
		for _, op := range ops {
			if !id.canDoOp(op) {
				return false
			}
		}
		return true
	})
}

// isAdmin is the authorize predicate for the operational endpoints (stats, info, checkpoint,
// compact), which act on the whole database rather than a key range and so require an admin
// identity. It is a named function so the ops handlers read the same as the keyed ones.
func isAdmin(id *Identity) bool { return id.Admin }

// scanAuthPrefix derives the key prefix a scan is confined to, for authorizing it against a read
// grant. An explicit prefix is that confinement directly. Otherwise every key the scan can return
// lies in [from, to), whose keys all share the longest common prefix of from and to, so that common
// prefix is a sound (if sometimes conservative) bound: a grant covering it covers every key the
// scan could yield. With neither a prefix nor bounds the scan spans the whole keyspace and the
// common prefix is empty, which only a global read grant covers.
func scanAuthPrefix(prefix, from, to []byte) []byte {
	if len(prefix) > 0 {
		return prefix
	}
	return commonPrefix(from, to)
}

// commonPrefix returns the longest byte prefix shared by a and b.
func commonPrefix(a, b []byte) []byte {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return a[:i]
}
