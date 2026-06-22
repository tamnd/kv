package server

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
)

// This file is the transport-security half of the server's operational surface (spec 17 §6). It
// covers two related things the spec asks for: TLS as the boundary that protects traffic on the
// wire, and mTLS as a second way to authenticate a caller, by the client certificate it presents
// rather than by a bearer token. The encryption-at-rest layer (spec 14) protects the file; TLS
// protects the connection, and the two are independent, so a database can be encrypted on disk and
// served in the clear on loopback, or unencrypted on disk and served over TLS to other hosts.
//
// TLS itself lives in the standard library's crypto/tls, so turning it on is a matter of handing
// the Server a *tls.Config and letting it wrap each listener. The Server does not load certificates
// or build the config; that is file I/O the CLI does, which keeps the library free of any opinion
// about where a certificate lives. mTLS identity resolution is the new piece here: when a TLS
// config requires a client certificate, the verified certificate names the caller, and a
// PeerAuthenticator turns that certificate into the same Identity a token would, so the per-prefix
// ACL applies identically however the caller authenticated.

// ErrInsecureBind is returned when a server is asked to bind a non-loopback address without TLS,
// which the spec forbids by default because such a bind carries traffic off the host in the clear.
var ErrInsecureBind = errors.New("kv: refusing to serve a non-loopback address without TLS")

// PeerAuthenticator resolves a verified TLS client certificate to an Identity, the mTLS analog of
// Authenticator. It is consulted only after the TLS stack has verified the certificate against the
// configured client CA, so the certificate is already known to be authentic; the authenticator's
// job is only to map that authenticated certificate to the caller's grants. A false result means
// the certificate is valid but names no identity the server knows, which the adapters treat as
// unauthenticated, the same as an unknown token.
type PeerAuthenticator interface {
	// AuthenticatePeer maps a verified client certificate to an identity. The second result is
	// false when the certificate names no known identity.
	AuthenticatePeer(cert *x509.Certificate) (*Identity, bool)
}

// CommonNameAuthenticator authenticates a client certificate by its subject Common Name, the
// simplest mTLS mapping: an operator issues each client a certificate whose CN is the identity
// name, and the table binds each CN to its grants. It is the certificate twin of
// StaticTokenAuthenticator, and like it the table is read-only after construction, so it is safe
// for concurrent use.
type CommonNameAuthenticator struct {
	names map[string]*Identity
}

// NewCommonNameAuthenticator builds a CN authenticator from a name-to-identity map. The map is
// copied so a later mutation of the caller's map does not change the table. An empty or nil map
// authenticates nothing, so a certificate whose CN is not listed is rejected rather than admitted
// with no grants.
func NewCommonNameAuthenticator(names map[string]*Identity) *CommonNameAuthenticator {
	cp := make(map[string]*Identity, len(names))
	for cn, id := range names {
		cp[cn] = id
	}
	return &CommonNameAuthenticator{names: cp}
}

// AuthenticatePeer looks the certificate's subject Common Name up in the table. A certificate with
// an empty CN never authenticates, so a certificate that carries its identity only in a SAN or not
// at all is rejected rather than matched against an empty table entry.
func (a *CommonNameAuthenticator) AuthenticatePeer(cert *x509.Certificate) (*Identity, bool) {
	cn := cert.Subject.CommonName
	if cn == "" {
		return nil, false
	}
	id, ok := a.names[cn]
	return id, ok
}

// NonLoopbackRequiresTLS reports an error when addr binds an interface that is not loopback and no
// TLS is configured, the spec's default that traffic leaving the host must be encrypted. A loopback
// bind (a 127.0.0.0/8 address, ::1, or localhost) needs no TLS because the traffic never leaves the
// machine, and a configured TLS config satisfies the requirement on any address. An empty addr is a
// disabled listener and is always fine. A caller that genuinely wants to serve a non-loopback
// address in the clear bypasses this check explicitly rather than having it silently not apply.
func NonLoopbackRequiresTLS(addr string, tlsConfigured bool) error {
	if addr == "" || tlsConfigured || isLoopbackHost(addr) {
		return nil
	}
	return ErrInsecureBind
}

// isLoopbackHost reports whether the host portion of a listen address is a loopback name or
// address. An address with no host (":8480", which binds every interface) is not loopback, so it
// requires TLS, since binding every interface exposes the port off-host.
func isLoopbackHost(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr // addr may be a bare host with no port
	}
	if host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// wrapTLS wraps a listener in TLS when the server has a config, leaving it untouched otherwise. It
// is the one place both the HTTP and the binary serve paths route through, so the two faces get TLS
// the same way: a connection accepted from the returned listener is a *tls.Conn whose handshake the
// transport drives transparently, which is why neither the HTTP handlers nor the binary loop need
// to know whether they are speaking over TLS.
func (srv *Server) wrapTLS(ln net.Listener) net.Listener {
	if srv.tlsConfig == nil {
		return ln
	}
	return tls.NewListener(ln, srv.tlsConfig)
}

// peerIdentity resolves the identity a TLS connection's client certificate names, or nil when the
// connection is not TLS, presents no certificate, or the server has no peer authenticator. It
// drives the TLS handshake explicitly so the verified certificate is available before the first
// byte of the protocol is read, which lets the binary loop bind the identity to the connection up
// front rather than waiting for an in-band handshake. A handshake failure yields no identity; the
// transport will surface the error on the first real read.
func (srv *Server) peerIdentity(conn net.Conn) (*Identity, bool) {
	if srv.peerAuth == nil {
		return nil, false
	}
	tc, ok := conn.(*tls.Conn)
	if !ok {
		return nil, false
	}
	if err := tc.Handshake(); err != nil {
		return nil, false
	}
	state := tc.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return nil, false
	}
	return srv.peerAuth.AuthenticatePeer(state.PeerCertificates[0])
}
