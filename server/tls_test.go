package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/tamnd/kv"
)

func TestNonLoopbackRequiresTLS(t *testing.T) {
	cases := []struct {
		addr    string
		hasTLS  bool
		wantErr bool
	}{
		{"127.0.0.1:8480", false, false}, // loopback, no TLS: fine
		{"localhost:8480", false, false}, // loopback by name: fine
		{"[::1]:8480", false, false},     // IPv6 loopback: fine
		{"0.0.0.0:8480", false, true},    // all interfaces, no TLS: refused
		{":8480", false, true},           // empty host (all interfaces): refused
		{"10.0.0.5:8480", false, true},   // routable address, no TLS: refused
		{"10.0.0.5:8480", true, false},   // routable address with TLS: fine
		{"", false, false},               // disabled listener: fine
	}
	for _, c := range cases {
		err := NonLoopbackRequiresTLS(c.addr, c.hasTLS)
		if (err != nil) != c.wantErr {
			t.Errorf("NonLoopbackRequiresTLS(%q, %v) error = %v, wantErr %v", c.addr, c.hasTLS, err, c.wantErr)
		}
		if err != nil && !errors.Is(err, ErrInsecureBind) {
			t.Errorf("NonLoopbackRequiresTLS(%q, %v) = %v, want ErrInsecureBind", c.addr, c.hasTLS, err)
		}
	}
}

func TestCommonNameAuthenticator(t *testing.T) {
	a := NewCommonNameAuthenticator(map[string]*Identity{
		"rw": {Name: "rw", Grants: []Grant{{Prefix: []byte("t1-"), Write: true}}},
	})
	// A cert whose CN is listed resolves to its identity.
	if id, ok := a.AuthenticatePeer(&x509.Certificate{Subject: pkix.Name{CommonName: "rw"}}); !ok || id.Name != "rw" {
		t.Fatalf("known CN = %v, %v; want the rw identity", id, ok)
	}
	// An unknown CN does not authenticate.
	if _, ok := a.AuthenticatePeer(&x509.Certificate{Subject: pkix.Name{CommonName: "nope"}}); ok {
		t.Fatalf("unknown CN authenticated")
	}
	// An empty CN never authenticates.
	if _, ok := a.AuthenticatePeer(&x509.Certificate{}); ok {
		t.Fatalf("empty CN authenticated")
	}
}

func TestParsePeerAuth(t *testing.T) {
	const table = `
# CN              name   grants
rw-client         rw     rw:t1-
admin-client      admin  admin
`
	pa, err := ParsePeerAuth(strings.NewReader(table))
	if err != nil {
		t.Fatalf("ParsePeerAuth: %v", err)
	}
	if id, ok := pa.AuthenticatePeer(&x509.Certificate{Subject: pkix.Name{CommonName: "rw-client"}}); !ok || id.Name != "rw" {
		t.Fatalf("rw-client = %v, %v", id, ok)
	}
	if id, ok := pa.AuthenticatePeer(&x509.Certificate{Subject: pkix.Name{CommonName: "admin-client"}}); !ok || !id.Admin {
		t.Fatalf("admin-client = %v, %v", id, ok)
	}
}

// testCA is a throwaway certificate authority for the TLS tests: it signs a server certificate and
// client certificates, and the tests trust it through a pool so a self-signed chain verifies.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

var certSerial int64 = 100

func nextSerial() *big.Int {
	certSerial++
	return big.NewInt(certSerial)
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          nextSerial(),
		Subject:               pkix.Name{CommonName: "kv-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &testCA{cert: cert, key: key, pool: pool}
}

// leaf signs a leaf certificate with the CA. A server leaf carries a 127.0.0.1 SAN so a client
// dialing the loopback verifies it; a client leaf carries its CN, which the peer authenticator maps
// to an identity.
func (ca *testCA) leaf(t *testing.T, cn string, server bool) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: nextSerial(),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	if server {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		tmpl.DNSNames = []string{"localhost"}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("leaf cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

// startTLSServer opens a temp database and serves both HTTP and binary over TLS on loopback ports,
// returning the two addresses. The server config requires and verifies a client certificate and
// maps it to an identity by CN, the mTLS path.
func startTLSServer(t *testing.T, ca *testCA, peer PeerAuthenticator, clientAuth tls.ClientAuthType, auth Authenticator) (httpAddr, binAddr string) {
	t.Helper()
	db, err := kv.Open(t.TempDir() + "/test.kv")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{ca.leaf(t, "127.0.0.1", true)},
		ClientCAs:    ca.pool,
		ClientAuth:   clientAuth,
		MinVersion:   tls.VersionTLS12,
	}
	hln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("http listen: %v", err)
	}
	bln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("binary listen: %v", err)
	}
	srv := New(db, Options{Auth: auth, PeerAuth: peer, TLSConfig: cfg})
	go srv.Serve(hln)
	go srv.ServeBinary(bln)
	t.Cleanup(func() {
		srv.Shutdown(context.Background())
		db.Close()
	})
	return hln.Addr().String(), bln.Addr().String()
}

func TestTLSMutualHTTP(t *testing.T) {
	ca := newTestCA(t)
	peer := NewCommonNameAuthenticator(map[string]*Identity{
		"rw-client": {Name: "rw", Grants: []Grant{{Prefix: []byte("t1-"), Write: true}}},
	})
	httpAddr, _ := startTLSServer(t, ca, peer, tls.RequireAndVerifyClientCert, nil)

	clientCert := ca.leaf(t, "rw-client", false)
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs:      ca.pool,
		Certificates: []tls.Certificate{clientCert},
	}}}
	base := "https://" + httpAddr

	// The client certificate names the rw identity, so a write in its prefix is allowed.
	if st := tlsDo(t, client, http.MethodPut, base+"/v1/kv/t1-a", "v"); st != http.StatusOK {
		t.Fatalf("mTLS PUT in-prefix status = %d, want 200", st)
	}
	// A write outside the granted prefix is forbidden, proving the cert mapped to the ACL.
	if st := tlsDo(t, client, http.MethodPut, base+"/v1/kv/t2-a", "v"); st != http.StatusForbidden {
		t.Fatalf("mTLS PUT out-of-prefix status = %d, want 403", st)
	}
}

func TestTLSMutualBinary(t *testing.T) {
	ca := newTestCA(t)
	peer := NewCommonNameAuthenticator(map[string]*Identity{
		"rw-client": {Name: "rw", Grants: []Grant{{Prefix: []byte("t1-"), Write: true}}},
	})
	_, binAddr := startTLSServer(t, ca, peer, tls.RequireAndVerifyClientCert, nil)

	clientCert := ca.leaf(t, "rw-client", false)
	conn, err := tls.Dial("tcp", binAddr, &tls.Config{
		RootCAs:      ca.pool,
		Certificates: []tls.Certificate{clientCert},
	})
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	cl := NewClient(conn)
	defer cl.Close()

	// The connection authenticated by its client certificate, so no opAuth is needed: a write in the
	// granted prefix is allowed and one outside it is forbidden.
	if _, err := cl.Set([]byte("t1-a"), []byte("v"), 0); err != nil {
		t.Fatalf("mTLS binary Set in-prefix: %v", err)
	}
	if _, err := cl.Set([]byte("t2-a"), []byte("v"), 0); !errors.Is(err, ErrForbidden) {
		t.Fatalf("mTLS binary Set out-of-prefix error = %v, want ErrForbidden", err)
	}
}

func TestTLSTokenOverHTTPS(t *testing.T) {
	ca := newTestCA(t)
	auth := NewStaticTokenAuthenticator(map[string]*Identity{
		"rw": {Name: "rw", Grants: []Grant{{Prefix: []byte("t1-"), Write: true}}},
	})
	// No client cert required here: the caller authenticates with a bearer token over the encrypted
	// connection, the common deployment where TLS protects the wire and tokens name the caller.
	httpAddr, _ := startTLSServer(t, ca, nil, tls.NoClientCert, auth)

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: ca.pool}}}
	base := "https://" + httpAddr

	req, _ := http.NewRequest(http.MethodPut, base+"/v1/kv/t1-a", strings.NewReader("v"))
	req.Header.Set("Authorization", "Bearer rw")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("token-over-TLS PUT: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("token-over-TLS PUT status = %d, want 200", resp.StatusCode)
	}
	// A request with no token is unauthenticated even over TLS.
	noTok, _ := http.NewRequest(http.MethodGet, base+"/v1/kv/t1-a", nil)
	resp2, err := client.Do(noTok)
	if err != nil {
		t.Fatalf("no-token GET: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no-token GET status = %d, want 401", resp2.StatusCode)
	}
}

// tlsDo issues a request and returns its status, closing the body.
func tlsDo(t *testing.T, client *http.Client, method, url, body string) int {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	resp.Body.Close()
	return resp.StatusCode
}
