package server

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/hmac"
	"crypto/rsa"
	_ "crypto/sha256" // register SHA-256 for crypto.Hash.New
	_ "crypto/sha512" // register SHA-384 and SHA-512
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"time"
)

// This file is the JWT bearer authenticator (spec 17 §6: "static API tokens, mTLS identities, or a
// JWT/OIDC validator"). It is the third Authenticator behind the one interface the auth slices
// defined, so it needs no change to either wire: the HTTP face already passes the bearer token to
// Authenticate and the binary face already passes the opAuth credential, so a JWT validates the same
// on either protocol and a verified token's claims map to the same per-prefix Identity a static
// token would. The validator is self-contained and zero-dependency: it parses the compact JWS,
// verifies the signature with the standard library's crypto, checks the registered time and
// issuer/audience claims, and hands the claims to a mapper that produces the Identity. Asymmetric
// keys come from a KeySource, which a static set satisfies directly and an OIDC JWKS endpoint
// satisfies over HTTP (jwt_jwks.go), so the same validator serves a shared-secret deployment and an
// OIDC one.

// ErrJWTInvalid is the single error every validation failure folds into, on purpose: a JWT
// authenticator must not tell an attacker which step failed (a bad signature, an expired token, a
// wrong audience), since that distinction is a probing oracle. The authenticator returns it
// internally and the adapters surface only "unauthenticated", so the wire never carries the reason.
var ErrJWTInvalid = errors.New("kv: invalid JWT")

// Claims is the decoded JWT payload as a generic map, the shape a ClaimsMapper reads to build an
// Identity. It is a map rather than a fixed struct because the claims that carry authorization are
// deployment-specific: one issuer puts a tenant in "sub", another in a custom "kv_grants" claim, and
// the mapper is where that policy lives. The registered claims (iss, aud, exp, nbf) are read by the
// validator before the mapper ever runs.
type Claims map[string]any

// str returns a string claim, false when absent or not a string.
func (c Claims) str(key string) (string, bool) {
	v, ok := c[key].(string)
	return v, ok
}

// num returns a numeric claim as a float64, the JSON number type, false when absent or not a
// number. The registered time claims (exp, nbf, iat) are seconds since the epoch, possibly
// fractional, which a float64 holds exactly for any realistic timestamp.
func (c Claims) num(key string) (float64, bool) {
	v, ok := c[key].(float64)
	return v, ok
}

// hasAudience reports whether the token's aud claim contains want. The claim is a string or an array
// of strings per the JWT spec, so both shapes are accepted: a single audience matches directly and a
// list matches when any element equals want.
func (c Claims) hasAudience(want string) bool {
	switch a := c["aud"].(type) {
	case string:
		return a == want
	case []any:
		for _, e := range a {
			if s, ok := e.(string); ok && s == want {
				return true
			}
		}
	}
	return false
}

// ClaimsMapper turns a verified token's claims into an Identity. It runs only after the signature and
// the registered claims have checked out, so it may trust the claims it reads. Returning false
// rejects the token even though it verified, for a claim set the deployment does not accept (for
// example a missing subject). The mapper is the seam where an issuer's claim conventions become this
// server's per-prefix grants.
type ClaimsMapper func(Claims) (*Identity, bool)

// DefaultClaimsMapper is the mapper used when none is configured. It reads the subject from "sub" for
// the identity name, an admin flag from a boolean "kv_admin" claim, and per-prefix grants from a
// "kv_grants" string claim written in the same grammar as the token table ("admin", "r:<prefix>",
// "rw:<prefix>", space or comma separated). A token that verifies but carries no grants and is not
// admin still authenticates: it names a caller the ACL then forbids from every key, which is the
// right split between "who you are" (authentication) and "what you may do" (authorization).
func DefaultClaimsMapper(c Claims) (*Identity, bool) {
	sub, ok := c.str("sub")
	if !ok || sub == "" {
		return nil, false
	}
	id := &Identity{Name: sub}
	if admin, ok := c["kv_admin"].(bool); ok && admin {
		id.Admin = true
	}
	if gs, ok := c.str("kv_grants"); ok && gs != "" {
		grants, err := parseGrantList(gs)
		if err != nil {
			return nil, false
		}
		id.Grants = grants
	}
	return id, true
}

// parseGrantList parses a space- or comma-separated grant list into Grants, reusing the per-grant
// grammar the token table uses so the two configuration surfaces describe access the same way. An
// "admin" entry is folded into the returned grants as a global read-write grant, since a Grant has no
// admin bit; a mapper that wants the Identity.Admin bypass reads the kv_admin claim instead.
func parseGrantList(s string) ([]Grant, error) {
	fields := strings.FieldsFunc(s, func(r rune) bool { return r == ' ' || r == ',' || r == '\t' })
	var grants []Grant
	for _, f := range fields {
		if f == "admin" {
			grants = append(grants, Grant{Prefix: nil, Write: true})
			continue
		}
		g, err := parseGrant(f)
		if err != nil {
			return nil, err
		}
		grants = append(grants, g)
	}
	return grants, nil
}

// KeySource resolves the key that verifies a token's signature, given the token header's algorithm
// and key id. It returns a []byte for the HMAC family and a crypto.PublicKey (an *rsa.PublicKey or
// *ecdsa.PublicKey) for the asymmetric families. The kid lets a source hold several keys and rotate
// them, which is what an OIDC JWKS endpoint does; a source with a single key ignores the kid. A false
// result means no key is known for that header, which fails the token.
type KeySource interface {
	Key(alg, kid string) (any, bool)
}

// StaticKeySet is a KeySource over a fixed set of keys: a map from key id to key plus an optional
// default for tokens that carry no kid or a kid not in the map. It is the source for a shared-secret
// HMAC deployment (one secret as the default) or a fixed asymmetric key, the cases that do not need
// the dynamic fetch a JWKS endpoint provides.
type StaticKeySet struct {
	byKID map[string]any
	def   any
}

// NewStaticKeySet builds a static key set. A nil or empty byKID with a non-nil def is the common
// single-key case; both may be set so a default covers tokens without a matching kid.
func NewStaticKeySet(byKID map[string]any, def any) *StaticKeySet {
	cp := make(map[string]any, len(byKID))
	for k, v := range byKID {
		cp[k] = v
	}
	return &StaticKeySet{byKID: cp, def: def}
}

// Key returns the key for a kid, or the default when the kid is absent or unmatched. The alg is
// ignored here; the validator already restricts which algorithms it will verify, and a static set
// trusts the operator who installed its keys.
func (s *StaticKeySet) Key(alg, kid string) (any, bool) {
	if kid != "" {
		if k, ok := s.byKID[kid]; ok {
			return k, true
		}
	}
	if s.def != nil {
		return s.def, true
	}
	return nil, false
}

// JWTOptions configures a JWTAuthenticator. Keys is required; everything else is optional with a safe
// default. Issuer and Audience, when set, are required to match the token's iss and aud claims, the
// checks that stop a token minted for another service from being replayed here. Mapper defaults to
// DefaultClaimsMapper. Leeway tolerates a little clock skew on the time checks. Now is injectable so
// a test drives the clock; it defaults to time.Now.
type JWTOptions struct {
	Keys     KeySource
	Issuer   string
	Audience string
	Mapper   ClaimsMapper
	Leeway   time.Duration
	Now      func() time.Time
}

// JWTAuthenticator validates a JWT bearer token and maps its claims to an Identity. It implements
// Authenticator, so it is configured as Options.Auth exactly where a static token table would be, and
// both wires authenticate against it unchanged. It is safe for concurrent use: its configuration is
// read-only after construction and its KeySource is responsible for its own synchronization (the
// JWKS source locks its cache).
type JWTAuthenticator struct {
	keys     KeySource
	issuer   string
	audience string
	mapper   ClaimsMapper
	leeway   time.Duration
	now      func() time.Time
}

// NewJWTAuthenticator builds a validator from options, filling the mapper and clock defaults. Keys
// must be non-nil; a validator with no key source could verify nothing and is a configuration error
// the caller catches by construction.
func NewJWTAuthenticator(opts JWTOptions) *JWTAuthenticator {
	mapper := opts.Mapper
	if mapper == nil {
		mapper = DefaultClaimsMapper
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &JWTAuthenticator{
		keys:     opts.Keys,
		issuer:   opts.Issuer,
		audience: opts.Audience,
		mapper:   mapper,
		leeway:   opts.Leeway,
		now:      now,
	}
}

// Authenticate validates the credential as a JWT and returns the mapped Identity. Every failure
// returns (nil, false) with no distinction, so the wire learns only that the token did not
// authenticate, never why. An empty credential is rejected before any parsing.
func (a *JWTAuthenticator) Authenticate(credential string) (*Identity, bool) {
	if credential == "" {
		return nil, false
	}
	claims, err := a.verify(credential)
	if err != nil {
		return nil, false
	}
	return a.mapper(claims)
}

// verify does the cryptographic and temporal validation, returning the claims on success. It is
// separated from Authenticate so a test can assert on the typed failure while the public method keeps
// the single opaque result.
func (a *JWTAuthenticator) verify(token string) (Claims, error) {
	// A compact JWS is three base64url segments joined by dots: header, payload, signature. The
	// signing input the signature covers is the first two segments and their dot, taken verbatim, so
	// the bytes are matched exactly rather than re-encoded.
	p1 := strings.IndexByte(token, '.')
	if p1 < 0 {
		return nil, ErrJWTInvalid
	}
	p2 := strings.IndexByte(token[p1+1:], '.')
	if p2 < 0 {
		return nil, ErrJWTInvalid
	}
	p2 += p1 + 1
	signingInput := token[:p2]
	headerSeg, payloadSeg, sigSeg := token[:p1], token[p1+1:p2], token[p2+1:]

	header, err := decodeSegment(headerSeg)
	if err != nil {
		return nil, ErrJWTInvalid
	}
	var hdr struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
		Typ string `json:"typ"`
	}
	if err := json.Unmarshal(header, &hdr); err != nil {
		return nil, ErrJWTInvalid
	}
	// "none" is refused outright: an unsigned token is exactly the downgrade attack a validator must
	// not accept, regardless of configuration.
	if hdr.Alg == "" || strings.EqualFold(hdr.Alg, "none") {
		return nil, ErrJWTInvalid
	}
	key, ok := a.keys.Key(hdr.Alg, hdr.Kid)
	if !ok {
		return nil, ErrJWTInvalid
	}
	sig, err := decodeSegment(sigSeg)
	if err != nil {
		return nil, ErrJWTInvalid
	}
	if err := verifySignature(hdr.Alg, key, signingInput, sig); err != nil {
		return nil, ErrJWTInvalid
	}

	payload, err := decodeSegment(payloadSeg)
	if err != nil {
		return nil, ErrJWTInvalid
	}
	var claims Claims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, ErrJWTInvalid
	}
	if err := a.validateClaims(claims); err != nil {
		return nil, err
	}
	return claims, nil
}

// validateClaims checks the registered claims: expiry and not-before against the clock with leeway,
// and issuer and audience against the configured expectations when those are set. The time checks use
// leeway so a small clock difference between the issuer and this server does not reject a token that
// is valid to the second.
func (a *JWTAuthenticator) validateClaims(c Claims) error {
	now := a.now()
	if exp, ok := c.num("exp"); ok {
		if now.After(time.Unix(int64(exp), 0).Add(a.leeway)) {
			return ErrJWTInvalid
		}
	}
	if nbf, ok := c.num("nbf"); ok {
		if now.Before(time.Unix(int64(nbf), 0).Add(-a.leeway)) {
			return ErrJWTInvalid
		}
	}
	if a.issuer != "" {
		iss, ok := c.str("iss")
		if !ok || iss != a.issuer {
			return ErrJWTInvalid
		}
	}
	if a.audience != "" && !c.hasAudience(a.audience) {
		return ErrJWTInvalid
	}
	return nil
}

// verifySignature checks the signature over signingInput with key, dispatching on the JWS algorithm.
// The supported families are HMAC (HS256/384/512), RSA PKCS#1 v1.5 (RS*), RSA-PSS (PS*), and ECDSA
// (ES*), which together cover the algorithms an OIDC provider realistically signs with. An
// unsupported alg fails, never silently passes.
func verifySignature(alg string, key any, signingInput string, sig []byte) error {
	h, ok := hashForAlg(alg)
	if !ok {
		return ErrJWTInvalid
	}
	digest := hashBytes(h, signingInput)

	switch {
	case strings.HasPrefix(alg, "HS"):
		secret, ok := key.([]byte)
		if !ok {
			return ErrJWTInvalid
		}
		mac := hmac.New(h.New, secret)
		mac.Write([]byte(signingInput))
		expected := mac.Sum(nil)
		// Constant-time compare so a mismatch leaks nothing about how many leading bytes matched.
		if subtle.ConstantTimeCompare(expected, sig) != 1 {
			return ErrJWTInvalid
		}
		return nil

	case strings.HasPrefix(alg, "RS"):
		pub, ok := key.(*rsa.PublicKey)
		if !ok {
			return ErrJWTInvalid
		}
		return rsaErr(rsa.VerifyPKCS1v15(pub, h, digest, sig))

	case strings.HasPrefix(alg, "PS"):
		pub, ok := key.(*rsa.PublicKey)
		if !ok {
			return ErrJWTInvalid
		}
		return rsaErr(rsa.VerifyPSS(pub, h, digest, sig, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: h}))

	case strings.HasPrefix(alg, "ES"):
		pub, ok := key.(*ecdsa.PublicKey)
		if !ok {
			return ErrJWTInvalid
		}
		// JWS ECDSA signatures are the fixed-width concatenation of r and s (P1363), each the curve's
		// byte size, not the ASN.1 form ecdsa.SignASN1 produces, so the halves are split by length.
		size := (pub.Curve.Params().BitSize + 7) / 8
		if len(sig) != 2*size {
			return ErrJWTInvalid
		}
		r := new(big.Int).SetBytes(sig[:size])
		s := new(big.Int).SetBytes(sig[size:])
		if !ecdsa.Verify(pub, digest, r, s) {
			return ErrJWTInvalid
		}
		return nil

	default:
		return ErrJWTInvalid
	}
}

// rsaErr folds a non-nil RSA verification error into ErrJWTInvalid so every failure path returns the
// one opaque error.
func rsaErr(err error) error {
	if err != nil {
		return ErrJWTInvalid
	}
	return nil
}

// hashForAlg maps a JWS algorithm to its hash by the trailing size, the digit shared across the HS,
// RS, PS, and ES families. An unrecognized size is not supported.
func hashForAlg(alg string) (crypto.Hash, bool) {
	if len(alg) < 3 {
		return 0, false
	}
	switch alg[len(alg)-3:] {
	case "256":
		return crypto.SHA256, true
	case "384":
		return crypto.SHA384, true
	case "512":
		return crypto.SHA512, true
	default:
		return 0, false
	}
}

// hashBytes returns the digest of s under h, the input the asymmetric verifiers check the signature
// against.
func hashBytes(h crypto.Hash, s string) []byte {
	hasher := h.New()
	hasher.Write([]byte(s))
	return hasher.Sum(nil)
}

// decodeSegment base64url-decodes one JWS segment. JWS uses unpadded base64url, so RawURLEncoding is
// the exact alphabet and padding the spec mandates.
func decodeSegment(seg string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(seg)
}
