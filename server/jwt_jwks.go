package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"sync"
	"time"
)

// This file is the OIDC half of the JWT authenticator: a KeySource that fetches an issuer's public
// keys from its JWKS endpoint (the "jwks_uri" an OpenID provider advertises) instead of holding a
// fixed key. It is what lets the validator trust a token signed by an external identity provider that
// rotates its signing keys: the token's kid selects the key, and the set fetches and caches the
// provider's current keys, refetching when it sees a kid it does not yet hold. It is still
// zero-dependency: the fetch is net/http, the JWK parsing is encoding/json plus math/big, and the
// keys it produces are the same *rsa.PublicKey and *ecdsa.PublicKey the static set produces, so the
// validator does not know or care which source a key came from.

// ErrNoKey is returned by a key source when it holds no key for a kid even after a refresh. The
// validator folds it into ErrJWTInvalid like any other failure, so the wire still learns only
// "unauthenticated".
var ErrNoKey = errors.New("kv: no JWKS key for kid")

// jwk is one key in a JWKS document, the JSON Web Key the JWKS endpoint serves. Only the fields this
// validator needs are decoded: the key type and id, the RSA modulus and exponent, and the EC curve
// and coordinates. A key whose kty is neither RSA nor EC is skipped rather than failing the whole
// set, since a provider may publish key types this server does not verify with.
type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	N   string `json:"n"`   // RSA modulus, base64url big-endian
	E   string `json:"e"`   // RSA public exponent, base64url big-endian
	Crv string `json:"crv"` // EC curve name
	X   string `json:"x"`   // EC x coordinate, base64url
	Y   string `json:"y"`   // EC y coordinate, base64url
}

// jwks is the top-level JWKS document, a set of keys under "keys".
type jwks struct {
	Keys []jwk `json:"keys"`
}

// RemoteKeySet is a KeySource backed by an OIDC JWKS endpoint. It caches the parsed keys by kid and
// refetches when asked for a kid it does not hold, throttled so a flood of tokens with an unknown kid
// cannot turn into a flood of fetches at the provider. It is safe for concurrent use: a mutex guards
// the cache and the last-fetch time, and a refetch happens under the lock so two callers racing on
// the same unknown kid make one request, not two.
type RemoteKeySet struct {
	url    string
	client *http.Client

	mu       sync.Mutex
	byKID    map[string]any
	lastSync time.Time
	// minRefresh bounds how often an unknown-kid miss may trigger a refetch, so a key the provider has
	// genuinely retired does not cause a fetch per request. Within the window a miss returns no key
	// without a fetch.
	minRefresh time.Duration
	now        func() time.Time
}

// JWKSOptions configures a RemoteKeySet. URL is the JWKS endpoint and is required. Client defaults to
// a client with a short timeout, so a slow or hung provider cannot stall an authentication
// indefinitely. MinRefresh defaults to a few seconds. Now is injectable for tests.
type JWKSOptions struct {
	URL        string
	Client     *http.Client
	MinRefresh time.Duration
	Now        func() time.Time
}

// defaultJWKSRefresh is how long a RemoteKeySet waits between unknown-kid refetches by default.
const defaultJWKSRefresh = 5 * time.Second

// defaultJWKSTimeout bounds a single JWKS fetch by default, so authentication fails fast when the
// provider is unreachable rather than hanging the request.
const defaultJWKSTimeout = 10 * time.Second

// NewRemoteKeySet builds a JWKS-backed key source. It does not fetch eagerly: the first Key call (or
// an explicit Refresh) populates the cache, so constructing one never blocks on the network.
func NewRemoteKeySet(opts JWKSOptions) *RemoteKeySet {
	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: defaultJWKSTimeout}
	}
	minRefresh := opts.MinRefresh
	if minRefresh == 0 {
		minRefresh = defaultJWKSRefresh
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &RemoteKeySet{
		url:        opts.URL,
		client:     client,
		byKID:      map[string]any{},
		minRefresh: minRefresh,
		now:        now,
	}
}

// Key returns the cached key for a kid, fetching once if the cache is empty or the kid is unknown and
// the refresh throttle allows it. The alg is ignored: the kid identifies the key and the validator
// already restricts which algorithms it verifies. A miss after a permitted refetch returns false,
// which fails the token.
func (s *RemoteKeySet) Key(alg, kid string) (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if k, ok := s.lookupLocked(kid); ok {
		return k, true
	}
	// Cache miss. Refetch when the cache has never been filled or the throttle window has elapsed,
	// then look again. The throttle stops an unknown kid from fetching on every request.
	if s.lastSync.IsZero() || s.now().Sub(s.lastSync) >= s.minRefresh {
		s.fetchLocked()
		if k, ok := s.lookupLocked(kid); ok {
			return k, true
		}
	}
	return nil, false
}

// lookupLocked returns the key for a kid from the cache, or, when the token carried no kid and the
// set holds exactly one key, that single key. The single-key fallback matches a provider that
// publishes one signing key without a kid, while a multi-key set requires a kid to disambiguate.
func (s *RemoteKeySet) lookupLocked(kid string) (any, bool) {
	if kid != "" {
		k, ok := s.byKID[kid]
		return k, ok
	}
	if len(s.byKID) == 1 {
		for _, k := range s.byKID {
			return k, true
		}
	}
	return nil, false
}

// Refresh fetches the JWKS now, replacing the cache. A host may call it at startup to fail fast on a
// misconfigured endpoint, or on a schedule to pick up rotations before a token with the new kid
// arrives. It is safe to call concurrently with Key.
func (s *RemoteKeySet) Refresh() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fetchLocked()
}

// fetchLocked retrieves and parses the JWKS, replacing the cache on success and stamping the
// last-fetch time whether or not it succeeded, so a failing endpoint is retried no faster than the
// throttle. It must be called with the mutex held.
func (s *RemoteKeySet) fetchLocked() error {
	s.lastSync = s.now()
	resp, err := s.client.Get(s.url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ErrNoKey
	}
	// Bound the document so a hostile or broken endpoint cannot stream an unbounded body into memory.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	var doc jwks
	if err := json.Unmarshal(body, &doc); err != nil {
		return err
	}
	next := make(map[string]any, len(doc.Keys))
	for _, k := range doc.Keys {
		pub, err := k.publicKey()
		if err != nil {
			continue // skip a key type or encoding this server does not verify with, keep the rest
		}
		// A key without a kid is stored under the empty string; lookupLocked's single-key fallback
		// reaches it only when it is the lone key in the set.
		next[k.Kid] = pub
	}
	s.byKID = next
	return nil
}

// publicKey turns one JWK into a crypto public key, an *rsa.PublicKey for an RSA key or an
// *ecdsa.PublicKey for an EC key. An unsupported key type or a malformed parameter is an error, which
// fetchLocked treats as "skip this key".
func (k jwk) publicKey() (any, error) {
	switch k.Kty {
	case "RSA":
		n, err := decodeBigInt(k.N)
		if err != nil {
			return nil, err
		}
		e, err := decodeSegment(k.E)
		if err != nil {
			return nil, err
		}
		if len(e) == 0 {
			return nil, ErrNoKey
		}
		return &rsa.PublicKey{N: n, E: int(new(big.Int).SetBytes(e).Int64())}, nil
	case "EC":
		curve, err := curveForName(k.Crv)
		if err != nil {
			return nil, err
		}
		x, err := decodeBigInt(k.X)
		if err != nil {
			return nil, err
		}
		y, err := decodeBigInt(k.Y)
		if err != nil {
			return nil, err
		}
		return &ecdsa.PublicKey{Curve: curve, X: x, Y: y}, nil
	default:
		return nil, ErrNoKey
	}
}

// curveForName maps a JWK curve name to its elliptic.Curve, covering the three NIST curves the ES
// algorithms use. An unrecognized curve is unsupported.
func curveForName(name string) (elliptic.Curve, error) {
	switch name {
	case "P-256":
		return elliptic.P256(), nil
	case "P-384":
		return elliptic.P384(), nil
	case "P-521":
		return elliptic.P521(), nil
	default:
		return nil, ErrNoKey
	}
}

// decodeBigInt base64url-decodes a JWK integer parameter (a modulus or coordinate) into a big-endian
// big.Int, the encoding RFC 7518 specifies for these fields.
func decodeBigInt(seg string) (*big.Int, error) {
	b, err := base64.RawURLEncoding.DecodeString(seg)
	if err != nil {
		return nil, err
	}
	return new(big.Int).SetBytes(b), nil
}
