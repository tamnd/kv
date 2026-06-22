package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/tamnd/kv/server"
)

// cmdServe opens a database and serves it over the network (spec 17): the CLI's job is to
// open the file, hand the writer to a server.Server, and run the listener until a signal
// arrives, then shut down cleanly so served work drains and the file closes coherently. The
// served surface is the same operation set the library and the rest of the CLI expose, on a
// socket instead of in process, so a database can be shared across processes or hosts.
func cmdServe(args []string) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := fs.String("addr", ":8480", "listen address for the HTTP surface")
	binaryAddr := fs.String("binary-addr", "", "listen address for the binary protocol (empty disables it)")
	// Request limits (spec 17 §6). The defaults come from the server; a flag overrides one
	// dimension, and a zero value disables that one limit. They guard the single process against a
	// request large enough to spike its memory or stall its writer.
	defaults := server.DefaultLimits()
	maxKeySize := fs.Int("max-key-size", defaults.MaxKeySize, "largest key in bytes (0 disables)")
	maxValueSize := fs.Int("max-value-size", defaults.MaxValueSize, "largest value in bytes (0 disables)")
	maxBatchOps := fs.Int("max-batch-ops", defaults.MaxBatchOps, "most ops in a batch or transaction (0 disables)")
	maxScanLimit := fs.Int("max-scan-limit", defaults.MaxScanLimit, "most pairs one scan returns (0 disables)")
	// Overload guardrails (spec 17 §4, §6): caps on how many connections, how many concurrent
	// requests, and how fast one caller may send, plus a final checkpoint on shutdown. Each is off
	// by default (zero), so a database on a trusted socket pays nothing; an exposed one sets them to
	// protect the single process from being swamped by the number of requests rather than their size.
	maxConns := fs.Int("max-conns", 0, "most simultaneously open connections per listener (0 unlimited)")
	maxInFlight := fs.Int("max-in-flight", 0, "most concurrent in-progress requests (0 unlimited)")
	ratePerSecond := fs.Float64("rate", 0, "per-caller request rate in requests per second (0 unlimited)")
	rateBurst := fs.Int("rate-burst", 0, "per-caller burst of requests allowed at once (0 defaults to the rate)")
	checkpointOnShutdown := fs.Bool("checkpoint-on-shutdown", false, "fold the WAL into the main file on graceful shutdown")
	// Authentication is opt-in (spec 17 §6): -auth-file names a token table, and an empty value
	// leaves the server open for a database on a trusted socket. The file maps each token to an
	// identity and its per-prefix grants; see server.ParseTokenAuth for the format.
	authFile := fs.String("auth-file", "", "path to a token table to require authentication (empty serves open)")
	// JWT bearer authentication (spec 17 §6): an alternative to the static token table that validates
	// signed JWTs and maps their claims to the same per-prefix identities. A key source is required to
	// turn it on, exactly one of a shared HMAC secret, a PEM public key, or an OIDC JWKS URL. The issuer
	// and audience, when set, must match the token's iss and aud. JWT and -auth-file are mutually
	// exclusive, since a server has one authenticator.
	jwtHS256SecretFile := fs.String("jwt-hs256-secret-file", "", "path to a file holding the HMAC secret for HS256/384/512 JWTs")
	jwtPublicKeyFile := fs.String("jwt-public-key-file", "", "path to a PEM public key (RSA or EC) verifying RS/PS/ES JWTs")
	jwtJWKSURL := fs.String("jwt-jwks-url", "", "URL of an OIDC JWKS endpoint to fetch signing keys from")
	jwtIssuer := fs.String("jwt-issuer", "", "required JWT issuer (iss claim); empty accepts any issuer")
	jwtAudience := fs.String("jwt-audience", "", "required JWT audience (aud claim); empty accepts any audience")
	// Transport security (spec 17 §6). -tls-cert/-tls-key turn on TLS on both listeners; -tls-client-ca
	// additionally turns on mTLS, verifying client certificates against that CA and mapping each to an
	// identity by the -mtls-identity-file table. A non-loopback bind without TLS is refused unless
	// -insecure is given, so an off-host port is encrypted by default.
	tlsCert := fs.String("tls-cert", "", "path to the server TLS certificate (PEM); enables TLS with -tls-key")
	tlsKey := fs.String("tls-key", "", "path to the server TLS private key (PEM)")
	tlsClientCA := fs.String("tls-client-ca", "", "path to a CA bundle (PEM) to verify client certificates (enables mTLS)")
	mtlsIdentityFile := fs.String("mtls-identity-file", "", "path to a CN-to-identity table for mTLS (same format as -auth-file)")
	insecure := fs.Bool("insecure", false, "allow serving a non-loopback address without TLS")
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv serve <db> [-addr host:port] [-binary-addr host:port] [-auth-file path | -jwt-jwks-url url] [-tls-cert path -tls-key path] [limit flags]")
	}
	limits := server.Limits{
		MaxKeySize:   *maxKeySize,
		MaxValueSize: *maxValueSize,
		MaxBatchOps:  *maxBatchOps,
		MaxScanLimit: *maxScanLimit,
	}

	// Resolve the authenticator before opening the database so a malformed auth file, an unreadable
	// key, or a contradictory flag combination fails before the file is touched. The token table and
	// JWT are mutually exclusive, since the server holds one authenticator; a nil result leaves the
	// server open.
	jwtConfigured := *jwtHS256SecretFile != "" || *jwtPublicKeyFile != "" || *jwtJWKSURL != ""
	if *authFile != "" && jwtConfigured {
		return fail(fmt.Errorf("kv: -auth-file and the -jwt-* flags are mutually exclusive"))
	}
	var auth server.Authenticator
	switch {
	case *authFile != "":
		a, err := loadAuthFile(*authFile)
		if err != nil {
			return fail(err)
		}
		auth = a
	case jwtConfigured:
		a, err := buildJWT(*jwtHS256SecretFile, *jwtPublicKeyFile, *jwtJWKSURL, *jwtIssuer, *jwtAudience)
		if err != nil {
			return fail(err)
		}
		auth = a
	}

	// Build the TLS config and the mTLS peer authenticator from the flags, all before opening the
	// database so a missing certificate or a malformed identity table fails before the file is
	// touched. A nil tlsConfig serves in the clear.
	tlsConfig, peerAuth, err := buildTLS(*tlsCert, *tlsKey, *tlsClientCA, *mtlsIdentityFile)
	if err != nil {
		return fail(err)
	}
	// Refuse a non-loopback bind without TLS by default, so a port reachable off-host is encrypted.
	// -insecure overrides for a trusted private network the operator vouches for.
	if !*insecure {
		if err := server.NonLoopbackRequiresTLS(*addr, tlsConfig != nil); err != nil {
			return fail(err)
		}
		if err := server.NonLoopbackRequiresTLS(*binaryAddr, tlsConfig != nil); err != nil {
			return fail(err)
		}
	}

	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()

	// Bind the listener before announcing readiness so the printed address is the real one,
	// including the OS-assigned port when -addr ends in :0, and so a bind failure is reported
	// before any traffic is promised.
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return fail(err)
	}
	srv := server.New(d, server.Options{
		Addr:                 ln.Addr().String(),
		Limits:               &limits,
		Auth:                 auth,
		PeerAuth:             peerAuth,
		TLSConfig:            tlsConfig,
		MaxConns:             *maxConns,
		MaxInFlight:          *maxInFlight,
		RatePerSecond:        *ratePerSecond,
		RateBurst:            *rateBurst,
		CheckpointOnShutdown: *checkpointOnShutdown,
	})

	// The announced scheme reflects whether TLS is on, so the printed URL is one a client can use
	// directly: https/kvs over TLS, http/kv in the clear.
	httpScheme, binScheme := "http", "kv"
	if tlsConfig != nil {
		httpScheme, binScheme = "https", "kvs"
	}

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	fmt.Fprintf(os.Stderr, "kv: serving %s on %s://%s\n", fs.Arg(0), httpScheme, ln.Addr().String())

	// The binary protocol is opt-in: when -binary-addr is set, bind a second listener and serve
	// the efficient wire on it alongside HTTP. The same Service backs both, so the two faces
	// agree on every operation. A closed listener on shutdown ends ServeBinary without error.
	if *binaryAddr != "" {
		bln, err := net.Listen("tcp", *binaryAddr)
		if err != nil {
			return fail(err)
		}
		go func() { errc <- srv.ServeBinary(bln) }()
		fmt.Fprintf(os.Stderr, "kv: serving %s binary on %s://%s\n", fs.Arg(0), binScheme, bln.Addr().String())
	}

	// Run until the listener fails or an interrupt/terminate signal arrives, then drain
	// in-flight requests with a bounded shutdown before the deferred Close folds the file.
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errc:
		if err != nil && err != http.ErrServerClosed {
			return fail(err)
		}
		return exitOK
	case sig := <-sigc:
		fmt.Fprintf(os.Stderr, "kv: %s, shutting down\n", sig)
		if err := srv.Shutdown(context.Background()); err != nil {
			return fail(err)
		}
		return exitOK
	}
}

// loadAuthFile opens a token table file and parses it into an authenticator. It closes the file
// before returning, so a parse failure does not leak a handle. The parse error already names the
// offending line, so the caller's fail wraps a message an operator can act on.
func loadAuthFile(path string) (server.Authenticator, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return server.ParseTokenAuth(f)
}

// buildJWT assembles a JWT bearer authenticator from the CLI flags. It requires exactly one key
// source, a shared HMAC secret, a PEM public key, or an OIDC JWKS URL, so an operator cannot
// half-configure it; the issuer and audience are optional and, when set, become required claim
// checks. The HMAC secret is the file content with a trailing newline trimmed, so a secret written
// with a plain editor or `echo` works without a stray byte; a deployment that needs an exact secret
// with trailing whitespace should not store it in a text file.
func buildJWT(hs256SecretFile, publicKeyFile, jwksURL, issuer, audience string) (server.Authenticator, error) {
	sources := 0
	for _, s := range []string{hs256SecretFile, publicKeyFile, jwksURL} {
		if s != "" {
			sources++
		}
	}
	if sources != 1 {
		return nil, fmt.Errorf("kv: configure exactly one of -jwt-hs256-secret-file, -jwt-public-key-file, or -jwt-jwks-url")
	}

	var keys server.KeySource
	switch {
	case hs256SecretFile != "":
		raw, err := os.ReadFile(hs256SecretFile)
		if err != nil {
			return nil, fmt.Errorf("kv: reading JWT HMAC secret: %w", err)
		}
		secret := bytes.TrimRight(raw, "\r\n")
		if len(secret) == 0 {
			return nil, fmt.Errorf("kv: JWT HMAC secret file %s is empty", hs256SecretFile)
		}
		keys = server.NewStaticKeySet(nil, secret)
	case publicKeyFile != "":
		pub, err := loadPEMPublicKey(publicKeyFile)
		if err != nil {
			return nil, err
		}
		keys = server.NewStaticKeySet(nil, pub)
	case jwksURL != "":
		keys = server.NewRemoteKeySet(server.JWKSOptions{URL: jwksURL})
	}

	return server.NewJWTAuthenticator(server.JWTOptions{
		Keys:     keys,
		Issuer:   issuer,
		Audience: audience,
	}), nil
}

// loadPEMPublicKey reads a PEM-encoded public key (an RSA or EC key in PKIX/SubjectPublicKeyInfo
// form, the `BEGIN PUBLIC KEY` block) and returns the parsed key for the JWT validator. A PKCS#1 RSA
// key (`BEGIN RSA PUBLIC KEY`) is accepted too, since some tooling emits that form.
func loadPEMPublicKey(path string) (any, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("kv: reading JWT public key: %w", err)
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("kv: -jwt-public-key-file %s contains no PEM block", path)
	}
	if pub, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		return pub, nil
	}
	if pub, err := x509.ParsePKCS1PublicKey(block.Bytes); err == nil {
		return pub, nil
	}
	return nil, fmt.Errorf("kv: -jwt-public-key-file %s is not a supported RSA or EC public key", path)
}

// buildTLS assembles the server's TLS config and mTLS peer authenticator from the four transport
// flags, returning nils when TLS is off. It validates the flag combinations so a half-configured
// setup fails at startup with a clear message rather than a confusing runtime error: a certificate
// needs its key and the reverse, mTLS needs the certificate it secures, and a peer identity table
// needs the client CA that makes a certificate trustworthy. When a client CA is given, the config
// requires and verifies a client certificate, so the peer authenticator only ever sees a
// certificate the TLS stack already vouched for.
func buildTLS(certPath, keyPath, clientCAPath, identityPath string) (*tls.Config, server.PeerAuthenticator, error) {
	if (certPath == "") != (keyPath == "") {
		return nil, nil, fmt.Errorf("kv: -tls-cert and -tls-key must be given together")
	}
	if certPath == "" {
		// No server certificate: TLS is off. mTLS without TLS is a contradiction, so reject the
		// client-side flags rather than silently ignoring them.
		if clientCAPath != "" || identityPath != "" {
			return nil, nil, fmt.Errorf("kv: -tls-client-ca and -mtls-identity-file require -tls-cert/-tls-key")
		}
		return nil, nil, nil
	}

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("kv: loading TLS certificate: %w", err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}

	if clientCAPath == "" {
		if identityPath != "" {
			return nil, nil, fmt.Errorf("kv: -mtls-identity-file requires -tls-client-ca")
		}
		return cfg, nil, nil
	}

	// mTLS: verify client certificates against the given CA bundle and map each to an identity.
	caPEM, err := os.ReadFile(clientCAPath)
	if err != nil {
		return nil, nil, fmt.Errorf("kv: reading client CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, nil, fmt.Errorf("kv: -tls-client-ca %s contains no PEM certificates", clientCAPath)
	}
	cfg.ClientCAs = pool
	cfg.ClientAuth = tls.RequireAndVerifyClientCert

	var peerAuth server.PeerAuthenticator
	if identityPath != "" {
		f, err := os.Open(identityPath)
		if err != nil {
			return nil, nil, err
		}
		defer f.Close()
		pa, err := server.ParsePeerAuth(f)
		if err != nil {
			return nil, nil, err
		}
		peerAuth = pa
	}
	return cfg, peerAuth, nil
}
