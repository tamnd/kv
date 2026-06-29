package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tamnd/kv"
)

// tightLimits is a small limit set the size tests drive against, so a modest payload trips each
// bound without allocating anything large.
func tightLimits() Limits {
	return Limits{MaxKeySize: 8, MaxValueSize: 8, MaxBatchOps: 3}
}

// newBinaryServerWithLimits serves the binary protocol with the given limits and returns a
// connected client, mirroring newBinaryServerAddr but threading limits through Options.
func newBinaryServerWithLimits(t *testing.T, l Limits) *Client {
	t.Helper()
	db, err := kv.Open(t.TempDir() + "/test.kv")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := New(db, Options{Limits: &l})
	go srv.ServeBinary(ln)
	t.Cleanup(func() {
		srv.Shutdown(context.Background())
		db.Close()
	})
	return dialClient(t, ln.Addr().String())
}

// newHTTPServerWithLimits mounts the HTTP handler with the given limits on an httptest server and
// returns its URL.
func newHTTPServerWithLimits(t *testing.T, l Limits) string {
	t.Helper()
	db, err := kv.Open(t.TempDir() + "/test.kv")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	srv := New(db, Options{Limits: &l})
	hs := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		db.Close()
		hs.Close()
	})
	return hs.URL
}

func TestLimitsServiceRejectsOversizedWrites(t *testing.T) {
	db := newTestDB(t)
	svc := NewService(db)
	defer svc.Close()
	svc.SetLimits(tightLimits())

	big := []byte("0123456789") // 10 bytes, over the 8-byte bounds

	if _, err := svc.Set(big, []byte("v"), 0); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("set with big key err = %v, want ErrLimitExceeded", err)
	}
	if _, err := svc.Set([]byte("k"), big, 0); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("set with big value err = %v, want ErrLimitExceeded", err)
	}
	if _, err := svc.Merge([]byte("k"), big); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("merge with big operand err = %v, want ErrLimitExceeded", err)
	}
	if _, err := svc.Delete(big); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("delete with big key err = %v, want ErrLimitExceeded", err)
	}
	if _, err := svc.DeleteRange(big, big); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("delete range with big bound err = %v, want ErrLimitExceeded", err)
	}

	// A request within the bounds still works, so the limit gates the oversized case only.
	if _, err := svc.Set([]byte("k"), []byte("v"), 0); err != nil {
		t.Fatalf("set within limits: %v", err)
	}
}

func TestLimitsServiceRejectsOversizedBatch(t *testing.T) {
	db := newTestDB(t)
	svc := NewService(db)
	defer svc.Close()
	svc.SetLimits(tightLimits())

	ops := []Op{
		{Kind: OpSet, Key: []byte("a"), Value: []byte("1")},
		{Kind: OpSet, Key: []byte("b"), Value: []byte("2")},
		{Kind: OpSet, Key: []byte("c"), Value: []byte("3")},
		{Kind: OpSet, Key: []byte("d"), Value: []byte("4")},
	}
	if _, err := svc.Batch(ops); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("batch over op cap err = %v, want ErrLimitExceeded", err)
	}
	// An oversized operand inside an otherwise-fine batch is also caught.
	if _, err := svc.Batch([]Op{{Kind: OpSet, Key: []byte("k"), Value: []byte("0123456789")}}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("batch with big value err = %v, want ErrLimitExceeded", err)
	}
	// A batch within both bounds commits.
	if _, err := svc.Batch(ops[:2]); err != nil {
		t.Fatalf("batch within limits: %v", err)
	}
}

func TestLimitsServiceTxnAssertAndOpsBounded(t *testing.T) {
	db := newTestDB(t)
	svc := NewService(db)
	defer svc.Close()
	svc.SetLimits(tightLimits())

	big := []byte("0123456789")
	if _, err := svc.Txn(TxnRequest{Asserts: []Assert{{Key: big}}}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("txn big assert key err = %v, want ErrLimitExceeded", err)
	}
	if _, err := svc.Txn(TxnRequest{Asserts: []Assert{{Key: []byte("k"), ExpectValue: big}}}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("txn big expect value err = %v, want ErrLimitExceeded", err)
	}
	if _, err := svc.Txn(TxnRequest{Ops: []Op{{Kind: OpSet, Key: big, Value: []byte("v")}}}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("txn big op key err = %v, want ErrLimitExceeded", err)
	}
}

func TestLimitsInteractiveTxnBounded(t *testing.T) {
	cl := newBinaryServerWithLimits(t, tightLimits())
	txn, err := cl.BeginTxn(true)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer txn.Discard()
	if err := txn.Set([]byte("k"), []byte("0123456789"), 0); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("interactive txn set over value limit err = %v, want ErrLimitExceeded", err)
	}
}

func TestLimitsHTTPReturns413(t *testing.T) {
	ts := newHTTPServerWithLimits(t, tightLimits())
	// PUT is the setter; a value over the limit maps to 413 Request Entity Too Large.
	status, _ := do(t, http.MethodPut, ts+"/v1/kv/k", strings.NewReader("0123456789"))
	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", status, http.StatusRequestEntityTooLarge)
	}
}
