package server

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// jwksDoc renders a one-key JWKS document for the public half of key, under the given kid. It serves
// the RSA and EC shapes the RemoteKeySet parses, so the test drives the real JWK encoding rather than
// reaching inside the parser.
func jwksDoc(t *testing.T, kid string, pub any) []byte {
	t.Helper()
	var key map[string]any
	switch p := pub.(type) {
	case *rsa.PublicKey:
		e := make([]byte, 0, 4)
		for v := p.E; v > 0; v >>= 8 {
			e = append([]byte{byte(v & 0xff)}, e...)
		}
		key = map[string]any{
			"kty": "RSA",
			"kid": kid,
			"alg": "RS256",
			"use": "sig",
			"n":   base64.RawURLEncoding.EncodeToString(p.N.Bytes()),
			"e":   base64.RawURLEncoding.EncodeToString(e),
		}
	case *ecdsa.PublicKey:
		key = map[string]any{
			"kty": "EC",
			"kid": kid,
			"crv": "P-256",
			"x":   base64.RawURLEncoding.EncodeToString(p.X.Bytes()),
			"y":   base64.RawURLEncoding.EncodeToString(p.Y.Bytes()),
		}
	default:
		t.Fatalf("unsupported key type %T", pub)
	}
	doc, err := json.Marshal(map[string]any{"keys": []any{key}})
	if err != nil {
		t.Fatalf("marshal jwks: %v", err)
	}
	return doc
}

func TestRemoteKeySetRSA(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	doc := jwksDoc(t, "k1", &priv.PublicKey)
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Write(doc)
	}))
	defer srv.Close()

	keys := NewRemoteKeySet(JWKSOptions{URL: srv.URL, Now: jwtClock})
	auth := NewJWTAuthenticator(JWTOptions{Keys: keys, Now: jwtClock})

	token := encodeJWT(t, map[string]any{"alg": "RS256", "kid": "k1", "typ": "JWT"},
		map[string]any{"sub": "svc", "exp": fixedNow.Add(time.Hour).Unix(), "kv_grants": "rw:app/"},
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
		t.Fatalf("token signed by the JWKS key was rejected")
	}
	if !id.canWrite([]byte("app/x")) {
		t.Fatalf("granted prefix not writable")
	}
	// A second token reuses the cached key: the endpoint is not fetched again within the throttle.
	token2 := encodeJWT(t, map[string]any{"alg": "RS256", "kid": "k1", "typ": "JWT"},
		map[string]any{"sub": "svc2", "exp": fixedNow.Add(time.Hour).Unix()},
		func(in string) []byte {
			sum := sha256.Sum256([]byte(in))
			sig, _ := rsa.SignPKCS1v15(rand.Reader, priv, crypto.SHA256, sum[:])
			return sig
		})
	if _, ok := auth.Authenticate(token2); !ok {
		t.Fatalf("second token rejected")
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("JWKS endpoint fetched %d times, want 1 (cache should serve the second token)", got)
	}
}

func TestRemoteKeySetEC(t *testing.T) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	doc := jwksDoc(t, "ec1", &priv.PublicKey)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write(doc)
	}))
	defer srv.Close()

	keys := NewRemoteKeySet(JWKSOptions{URL: srv.URL, Now: jwtClock})
	auth := NewJWTAuthenticator(JWTOptions{Keys: keys, Now: jwtClock})
	token := encodeJWT(t, map[string]any{"alg": "ES256", "kid": "ec1", "typ": "JWT"},
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
		t.Fatalf("EC token signed by the JWKS key was rejected")
	}
}

func TestRemoteKeySetUnknownKid(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	doc := jwksDoc(t, "k1", &priv.PublicKey)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write(doc)
	}))
	defer srv.Close()

	// MinRefresh 0 lets every miss refetch, but the kid is genuinely absent, so it still fails.
	keys := NewRemoteKeySet(JWKSOptions{URL: srv.URL, MinRefresh: time.Nanosecond, Now: jwtClock})
	if _, ok := keys.Key("RS256", "absent"); ok {
		t.Fatalf("unknown kid resolved to a key")
	}
}

func TestRemoteKeySetRefreshError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	keys := NewRemoteKeySet(JWKSOptions{URL: srv.URL, Now: jwtClock})
	if err := keys.Refresh(); err == nil {
		t.Fatalf("Refresh against a failing endpoint should error")
	}
}
