package server

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/kv"
)

// fixedNow is the clock the JWT tests run against, so the registered time claims are checked against a
// known instant rather than the wall clock. Tokens set exp and nbf relative to this value.
var fixedNow = time.Unix(1_000_000, 0)

func jwtClock() time.Time { return fixedNow }

// encodeJWT assembles a compact JWS from a header and claims, signing the header.payload signing
// input with sign. The tests build tokens this way so they exercise the real wire format the
// validator parses, rather than reaching inside it.
func encodeJWT(t *testing.T, header, claims map[string]any, sign func(signingInput string) []byte) string {
	t.Helper()
	hb, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	cb, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	input := base64.RawURLEncoding.EncodeToString(hb) + "." + base64.RawURLEncoding.EncodeToString(cb)
	sig := sign(input)
	return input + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// signHS256 signs with an HMAC-SHA256 secret, the shared-secret case.
func signHS256(t *testing.T, secret []byte, claims map[string]any) string {
	return encodeJWT(t, map[string]any{"alg": "HS256", "typ": "JWT"}, claims, func(in string) []byte {
		m := hmac.New(sha256.New, secret)
		m.Write([]byte(in))
		return m.Sum(nil)
	})
}

// i2osp left-pads a big integer to a fixed width, the form a JWS ECDSA signature uses for r and s.
func i2osp(x *big.Int, size int) []byte {
	b := x.Bytes()
	if len(b) >= size {
		return b[len(b)-size:]
	}
	out := make([]byte, size)
	copy(out[size-len(b):], b)
	return out
}

func TestJWTHS256Valid(t *testing.T) {
	secret := []byte("a-shared-secret")
	auth := NewJWTAuthenticator(JWTOptions{
		Keys: NewStaticKeySet(nil, secret),
		Now:  jwtClock,
	})
	token := signHS256(t, secret, map[string]any{
		"sub":       "alice",
		"exp":       fixedNow.Add(time.Hour).Unix(),
		"kv_grants": "rw:app/",
	})
	id, ok := auth.Authenticate(token)
	if !ok {
		t.Fatalf("valid HS256 token rejected")
	}
	if id.Name != "alice" {
		t.Fatalf("identity name = %q, want alice", id.Name)
	}
	if !id.canWrite([]byte("app/k")) {
		t.Fatalf("granted prefix not writable")
	}
	if id.canWrite([]byte("other/k")) {
		t.Fatalf("ungranted prefix should not be writable")
	}
}

func TestJWTRejectsTamperedSignature(t *testing.T) {
	secret := []byte("a-shared-secret")
	auth := NewJWTAuthenticator(JWTOptions{Keys: NewStaticKeySet(nil, secret), Now: jwtClock})
	token := signHS256(t, secret, map[string]any{"sub": "alice", "exp": fixedNow.Add(time.Hour).Unix()})
	// Flip the last character of the signature segment.
	tampered := token[:len(token)-1]
	if token[len(token)-1] == 'A' {
		tampered += "B"
	} else {
		tampered += "A"
	}
	if _, ok := auth.Authenticate(tampered); ok {
		t.Fatalf("tampered signature accepted")
	}
}

func TestJWTRejectsWrongSecret(t *testing.T) {
	auth := NewJWTAuthenticator(JWTOptions{Keys: NewStaticKeySet(nil, []byte("right")), Now: jwtClock})
	token := signHS256(t, []byte("wrong"), map[string]any{"sub": "alice", "exp": fixedNow.Add(time.Hour).Unix()})
	if _, ok := auth.Authenticate(token); ok {
		t.Fatalf("token signed with the wrong secret accepted")
	}
}

func TestJWTRejectsExpired(t *testing.T) {
	secret := []byte("s")
	auth := NewJWTAuthenticator(JWTOptions{Keys: NewStaticKeySet(nil, secret), Now: jwtClock})
	token := signHS256(t, secret, map[string]any{"sub": "alice", "exp": fixedNow.Add(-time.Second).Unix()})
	if _, ok := auth.Authenticate(token); ok {
		t.Fatalf("expired token accepted")
	}
}

func TestJWTRejectsNotYetValid(t *testing.T) {
	secret := []byte("s")
	auth := NewJWTAuthenticator(JWTOptions{Keys: NewStaticKeySet(nil, secret), Now: jwtClock})
	token := signHS256(t, secret, map[string]any{
		"sub": "alice",
		"exp": fixedNow.Add(time.Hour).Unix(),
		"nbf": fixedNow.Add(time.Minute).Unix(),
	})
	if _, ok := auth.Authenticate(token); ok {
		t.Fatalf("not-yet-valid token accepted")
	}
}

func TestJWTLeewayToleratesSkew(t *testing.T) {
	secret := []byte("s")
	auth := NewJWTAuthenticator(JWTOptions{Keys: NewStaticKeySet(nil, secret), Now: jwtClock, Leeway: time.Minute})
	// Expired by 30s but within a minute of leeway: accepted.
	token := signHS256(t, secret, map[string]any{"sub": "alice", "exp": fixedNow.Add(-30 * time.Second).Unix()})
	if _, ok := auth.Authenticate(token); !ok {
		t.Fatalf("token within leeway rejected")
	}
}

func TestJWTIssuerAudienceEnforced(t *testing.T) {
	secret := []byte("s")
	auth := NewJWTAuthenticator(JWTOptions{
		Keys:     NewStaticKeySet(nil, secret),
		Issuer:   "https://issuer.example",
		Audience: "kv",
		Now:      jwtClock,
	})
	base := map[string]any{"sub": "alice", "exp": fixedNow.Add(time.Hour).Unix()}
	good := func() map[string]any {
		return map[string]any{"sub": "alice", "exp": fixedNow.Add(time.Hour).Unix(), "iss": "https://issuer.example", "aud": "kv"}
	}
	// Right issuer and audience: accepted.
	if _, ok := auth.Authenticate(signHS256(t, secret, good())); !ok {
		t.Fatalf("token with matching iss/aud rejected")
	}
	// Missing issuer: rejected.
	if _, ok := auth.Authenticate(signHS256(t, secret, base)); ok {
		t.Fatalf("token missing iss accepted")
	}
	// Wrong audience: rejected.
	wrongAud := good()
	wrongAud["aud"] = "other"
	if _, ok := auth.Authenticate(signHS256(t, secret, wrongAud)); ok {
		t.Fatalf("token with wrong aud accepted")
	}
}

func TestJWTAudienceArray(t *testing.T) {
	secret := []byte("s")
	auth := NewJWTAuthenticator(JWTOptions{Keys: NewStaticKeySet(nil, secret), Audience: "kv", Now: jwtClock})
	token := signHS256(t, secret, map[string]any{
		"sub": "alice",
		"exp": fixedNow.Add(time.Hour).Unix(),
		"aud": []any{"other", "kv"},
	})
	if _, ok := auth.Authenticate(token); !ok {
		t.Fatalf("token whose aud array contains the expected audience rejected")
	}
}

func TestJWTRejectsNoneAlg(t *testing.T) {
	auth := NewJWTAuthenticator(JWTOptions{Keys: NewStaticKeySet(nil, []byte("s")), Now: jwtClock})
	// A token with alg "none" and an empty signature is the classic downgrade attack.
	token := encodeJWT(t, map[string]any{"alg": "none", "typ": "JWT"},
		map[string]any{"sub": "alice", "exp": fixedNow.Add(time.Hour).Unix()},
		func(string) []byte { return nil })
	if _, ok := auth.Authenticate(token); ok {
		t.Fatalf("alg=none token accepted")
	}
}

func TestJWTRejectsUnknownKid(t *testing.T) {
	secret := []byte("s")
	// A keyed set with no default: a token whose kid is unknown finds no key.
	auth := NewJWTAuthenticator(JWTOptions{Keys: NewStaticKeySet(map[string]any{"k1": secret}, nil), Now: jwtClock})
	token := encodeJWT(t, map[string]any{"alg": "HS256", "kid": "k2"},
		map[string]any{"sub": "alice", "exp": fixedNow.Add(time.Hour).Unix()},
		func(in string) []byte { m := hmac.New(sha256.New, secret); m.Write([]byte(in)); return m.Sum(nil) })
	if _, ok := auth.Authenticate(token); ok {
		t.Fatalf("token with unknown kid accepted")
	}
}

func TestJWTRejectsMalformed(t *testing.T) {
	auth := NewJWTAuthenticator(JWTOptions{Keys: NewStaticKeySet(nil, []byte("s")), Now: jwtClock})
	for _, bad := range []string{"", "notajwt", "only.two", "a.b.c.d"} {
		if _, ok := auth.Authenticate(bad); ok {
			t.Fatalf("malformed token %q accepted", bad)
		}
	}
}

func TestJWTRS256Valid(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	auth := NewJWTAuthenticator(JWTOptions{Keys: NewStaticKeySet(nil, &priv.PublicKey), Now: jwtClock})
	token := encodeJWT(t, map[string]any{"alg": "RS256", "typ": "JWT"},
		map[string]any{"sub": "svc", "exp": fixedNow.Add(time.Hour).Unix(), "kv_admin": true},
		func(in string) []byte {
			sum := sha256.Sum256([]byte(in))
			sig, err := rsa.SignPKCS1v15(rand.Reader, priv, crypto.SHA256, sum[:])
			if err != nil {
				t.Fatalf("sign: %v", err)
			}
			return sig
		})
	id, ok := auth.Authenticate(token)
	if !ok {
		t.Fatalf("valid RS256 token rejected")
	}
	if !id.Admin {
		t.Fatalf("kv_admin claim did not map to an admin identity")
	}
}

func TestJWTES256Valid(t *testing.T) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	auth := NewJWTAuthenticator(JWTOptions{Keys: NewStaticKeySet(nil, &priv.PublicKey), Now: jwtClock})
	token := encodeJWT(t, map[string]any{"alg": "ES256", "typ": "JWT"},
		map[string]any{"sub": "svc", "exp": fixedNow.Add(time.Hour).Unix()},
		func(in string) []byte {
			sum := sha256.Sum256([]byte(in))
			r, s, err := ecdsa.Sign(rand.Reader, priv, sum[:])
			if err != nil {
				t.Fatalf("sign: %v", err)
			}
			return append(i2osp(r, 32), i2osp(s, 32)...)
		})
	if _, ok := auth.Authenticate(token); !ok {
		t.Fatalf("valid ES256 token rejected")
	}
}

func TestDefaultClaimsMapperNoSubject(t *testing.T) {
	if _, ok := DefaultClaimsMapper(Claims{"exp": float64(1)}); ok {
		t.Fatalf("claims without a subject mapped to an identity")
	}
}

func TestParseGrantList(t *testing.T) {
	// The read-only and read-write prefix grants, checked without an admin entry so each grant's own
	// reach is what the assertions see.
	grants, err := parseGrantList("r:read/, rw:write/")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(grants) != 2 {
		t.Fatalf("got %d grants, want 2", len(grants))
	}
	id := &Identity{Grants: grants}
	if !id.canRead([]byte("read/x")) || id.canWrite([]byte("read/x")) {
		t.Fatalf("r: grant should read but not write")
	}
	if !id.canWrite([]byte("write/x")) {
		t.Fatalf("rw: grant should write")
	}
	if id.canRead([]byte("other/x")) {
		t.Fatalf("an ungranted prefix should not be readable")
	}
	// An admin entry folds into a global read-write grant, so it reaches any key.
	adminGrants, err := parseGrantList("admin")
	if err != nil {
		t.Fatalf("parse admin: %v", err)
	}
	if !(&Identity{Grants: adminGrants}).canWrite([]byte("anything")) {
		t.Fatalf("admin grant should write any key")
	}
	if _, err := parseGrantList("bogus:x"); err == nil {
		t.Fatalf("a malformed grant should error")
	}
}

// newJWTHTTPServer mounts the HTTP handler with a JWT authenticator over a shared HS256 secret.
func newJWTHTTPServer(t *testing.T, secret []byte) string {
	t.Helper()
	db, err := kv.Open(t.TempDir() + "/test.kv")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	auth := NewJWTAuthenticator(JWTOptions{Keys: NewStaticKeySet(nil, secret), Now: jwtClock})
	srv := New(db, Options{Auth: auth})
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		db.Close()
		hs.Close()
	})
	return hs.URL
}

func TestHTTPJWTEnforced(t *testing.T) {
	secret := []byte("http-secret")
	ts := newJWTHTTPServer(t, secret)
	token := signHS256(t, secret, map[string]any{
		"sub":       "alice",
		"exp":       fixedNow.Add(time.Hour).Unix(),
		"kv_grants": "rw:t1-",
	})
	// In-prefix write is admitted.
	if status, _ := doAuth(t, token, http.MethodPut, ts+"/v1/kv/t1-a", strings.NewReader("v")); status != http.StatusOK {
		t.Fatalf("in-prefix PUT status = %d, want 200", status)
	}
	// Out-of-prefix write is forbidden by the mapped grant.
	if status, _ := doAuth(t, token, http.MethodPut, ts+"/v1/kv/t2-a", strings.NewReader("v")); status != http.StatusForbidden {
		t.Fatalf("out-of-prefix PUT status = %d, want 403", status)
	}
	// No token is unauthenticated.
	if status, _ := doAuth(t, "", http.MethodGet, ts+"/v1/kv/t1-a", nil); status != http.StatusUnauthorized {
		t.Fatalf("no-token GET status = %d, want 401", status)
	}
	// An expired token is unauthenticated.
	expired := signHS256(t, secret, map[string]any{"sub": "alice", "exp": fixedNow.Add(-time.Hour).Unix(), "kv_grants": "rw:t1-"})
	if status, _ := doAuth(t, expired, http.MethodGet, ts+"/v1/kv/t1-a", nil); status != http.StatusUnauthorized {
		t.Fatalf("expired-token GET status = %d, want 401", status)
	}
}

func TestBinaryJWTEnforced(t *testing.T) {
	secret := []byte("binary-secret")
	db, err := kv.Open(t.TempDir() + "/test.kv")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	auth := NewJWTAuthenticator(JWTOptions{Keys: NewStaticKeySet(nil, secret), Now: jwtClock})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := New(db, Options{Auth: auth})
	go srv.ServeBinary(ln)
	t.Cleanup(func() {
		srv.Shutdown(context.Background())
		db.Close()
	})

	cl := dialClient(t, ln.Addr().String())
	token := signHS256(t, secret, map[string]any{
		"sub":       "alice",
		"exp":       fixedNow.Add(time.Hour).Unix(),
		"kv_grants": "rw:t1-",
	})
	name, err := cl.Authenticate(token)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if name != "alice" {
		t.Fatalf("identity name = %q, want alice", name)
	}
	if _, err := cl.Set([]byte("t1-a"), []byte("v"), 0); err != nil {
		t.Fatalf("in-prefix Set: %v", err)
	}
	if _, err := cl.Set([]byte("t2-a"), []byte("v"), 0); !errors.Is(err, ErrForbidden) {
		t.Fatalf("out-of-prefix Set error = %v, want ErrForbidden", err)
	}

	// A bad token over the same wire is rejected at the handshake.
	bad := dialClient(t, ln.Addr().String())
	if _, err := bad.Authenticate(signHS256(t, []byte("wrong"), map[string]any{"sub": "x", "exp": fixedNow.Add(time.Hour).Unix()})); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("bad-token handshake error = %v, want ErrUnauthenticated", err)
	}
}
