package server

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/tamnd/kv"
)

// This file is the HTTP/JSON protocol adapter (spec 17 §2.2): a REST-ish mapping of the
// operation surface onto net/http so any client with curl or an HTTP library reaches the
// database without a special stack. It decodes a request, calls the transport-agnostic
// Service, and encodes the result; it adds no storage semantics. Point values move as raw
// request and response bodies so binary data needs no wrapping, and the structured endpoints
// (txn, batch, stats) speak JSON with byte fields base64-encoded since JSON cannot hold raw
// bytes. The router is the standard library's method-and-path ServeMux, so the dependency
// footprint stays at the standard library.

// httpHandler builds the net/http mux for a Service. It is separated from Server so a test
// can mount it on httptest without binding a socket.
func (srv *Server) httpHandler() http.Handler {
	mux := http.NewServeMux()
	s := srv.svc

	mux.HandleFunc("GET /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key, err := decodeKey(r)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		v, found, err := s.Get(key)
		if err != nil {
			writeServiceErr(w, err)
			return
		}
		if !found {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(v)
	})

	mux.HandleFunc("PUT /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key, err := decodeKey(r)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		value, err := io.ReadAll(r.Body)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		ttl, err := parseTTL(r)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		version, err := s.Set(key, value, ttl)
		if err != nil {
			writeServiceErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, versionResponse{Version: version})
	})

	mux.HandleFunc("DELETE /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key, err := decodeKey(r)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		version, err := s.Delete(key)
		if err != nil {
			writeServiceErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, versionResponse{Version: version})
	})

	// Range delete shares the /v1/kv path with no key segment, selected by the from/to
	// query bounds, matching the spec's DELETE /v1/kv?from=&to= mapping.
	mux.HandleFunc("DELETE /v1/kv", func(w http.ResponseWriter, r *http.Request) {
		enc := r.URL.Query().Get("encoding")
		lo, err := decodeBytes(r.URL.Query().Get("from"), enc)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		hi, err := decodeBytes(r.URL.Query().Get("to"), enc)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		version, err := s.DeleteRange(lo, hi)
		if err != nil {
			writeServiceErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, versionResponse{Version: version})
	})

	mux.HandleFunc("POST /v1/txn", func(w http.ResponseWriter, r *http.Request) {
		var req jsonTxnRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		decoded, err := req.decode()
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		res, err := s.Txn(decoded)
		if err != nil {
			writeServiceErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, encodeTxnResult(res))
	})

	mux.HandleFunc("POST /v1/batch", func(w http.ResponseWriter, r *http.Request) {
		var req jsonBatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		ops, err := decodeOps(req.Ops)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		version, err := s.Batch(ops)
		if err != nil {
			writeServiceErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, versionResponse{Version: version})
	})

	mux.HandleFunc("GET /v1/scan", srv.handleScan)
	mux.HandleFunc("GET /v1/watch", srv.handleWatch)

	mux.HandleFunc("GET /v1/stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, s.Stats())
	})
	mux.HandleFunc("GET /v1/info", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, s.Stats())
	})

	mux.HandleFunc("POST /v1/checkpoint", func(w http.ResponseWriter, r *http.Request) {
		if err := s.Checkpoint(); err != nil {
			writeServiceErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, okResponse{OK: true})
	})
	mux.HandleFunc("POST /v1/compact", func(w http.ResponseWriter, r *http.Request) {
		budget := 0
		if b := r.URL.Query().Get("budget"); b != "" {
			n, err := strconv.Atoi(b)
			if err != nil {
				writeErr(w, http.StatusBadRequest, err)
				return
			}
			budget = n
		}
		reclaimed, err := s.Compact(budget)
		if err != nil {
			writeServiceErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, compactResponse{Reclaimed: reclaimed})
	})

	// Operational endpoints: liveness and the Prometheus metrics surface, the same numbers
	// the CLI's metrics command prints, rendered by the same library code (spec 17 §6).
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := s.DB().WriteMetrics(w); err != nil {
			writeServiceErr(w, err)
		}
	})

	return mux
}

// decodeKey reads the {key} path segment and decodes it under the request's ?encoding
// selector. Default raw means the URL-unescaped segment bytes; base64 and hex reinterpret
// the segment text, the escape hatch for binary keys that do not survive a URL path.
func decodeKey(r *http.Request) ([]byte, error) {
	return decodeBytes(r.PathValue("key"), r.URL.Query().Get("encoding"))
}

// decodeBytes turns a request string into bytes under an encoding selector.
func decodeBytes(s, enc string) ([]byte, error) {
	switch enc {
	case "", "raw":
		return []byte(s), nil
	case "base64":
		return base64.StdEncoding.DecodeString(s)
	case "hex":
		return hex.DecodeString(s)
	default:
		return nil, errors.New("kv: unknown encoding " + enc)
	}
}

// parseTTL reads the optional ?ttl query as a Go duration (for example 30s, 5m), zero when
// absent, so a set carries an expiry without a separate endpoint.
func parseTTL(r *http.Request) (time.Duration, error) {
	t := r.URL.Query().Get("ttl")
	if t == "" {
		return 0, nil
	}
	return time.ParseDuration(t)
}

// versionResponse, okResponse, and compactResponse are the small JSON results of the write
// and ops endpoints.
type versionResponse struct {
	Version uint64 `json:"version"`
}
type okResponse struct {
	OK bool `json:"ok"`
}
type compactResponse struct {
	Reclaimed int `json:"reclaimed"`
}

// jsonOp is the wire form of an Op: byte fields are base64 strings since JSON has no raw-byte
// type, and ttl_ms carries a set's TTL in milliseconds.
type jsonOp struct {
	Kind  string `json:"kind"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
	Lo    string `json:"lo,omitempty"`
	Hi    string `json:"hi,omitempty"`
	TTLMs int64  `json:"ttl_ms,omitempty"`
}

// jsonAssert is the wire form of an Assert.
type jsonAssert struct {
	Key          string `json:"key"`
	ExpectValue  string `json:"expect_value,omitempty"`
	ExpectAbsent bool   `json:"expect_absent,omitempty"`
}

type jsonTxnRequest struct {
	Asserts []jsonAssert `json:"asserts,omitempty"`
	Ops     []jsonOp     `json:"ops,omitempty"`
}

type jsonBatchRequest struct {
	Ops []jsonOp `json:"ops"`
}

type jsonReadResult struct {
	Found bool   `json:"found"`
	Value string `json:"value,omitempty"`
}

type jsonTxnResponse struct {
	Reads   []jsonReadResult `json:"reads"`
	Version uint64           `json:"version"`
}

// decode turns a wire transaction request into the Service's TxnRequest, base64-decoding
// every byte field.
func (req jsonTxnRequest) decode() (TxnRequest, error) {
	out := TxnRequest{}
	for _, a := range req.Asserts {
		key, err := base64.StdEncoding.DecodeString(a.Key)
		if err != nil {
			return TxnRequest{}, err
		}
		val, err := base64.StdEncoding.DecodeString(a.ExpectValue)
		if err != nil {
			return TxnRequest{}, err
		}
		out.Asserts = append(out.Asserts, Assert{Key: key, ExpectValue: val, ExpectAbsent: a.ExpectAbsent})
	}
	ops, err := decodeOps(req.Ops)
	if err != nil {
		return TxnRequest{}, err
	}
	out.Ops = ops
	return out, nil
}

// decodeOps base64-decodes a slice of wire ops into Service ops.
func decodeOps(in []jsonOp) ([]Op, error) {
	var out []Op
	for _, o := range in {
		key, err := base64.StdEncoding.DecodeString(o.Key)
		if err != nil {
			return nil, err
		}
		val, err := base64.StdEncoding.DecodeString(o.Value)
		if err != nil {
			return nil, err
		}
		lo, err := base64.StdEncoding.DecodeString(o.Lo)
		if err != nil {
			return nil, err
		}
		hi, err := base64.StdEncoding.DecodeString(o.Hi)
		if err != nil {
			return nil, err
		}
		out = append(out, Op{
			Kind:  OpKind(o.Kind),
			Key:   key,
			Value: val,
			Lo:    lo,
			Hi:    hi,
			TTL:   time.Duration(o.TTLMs) * time.Millisecond,
		})
	}
	return out, nil
}

// encodeTxnResult turns a TxnResult into its wire form, base64-encoding read values.
func encodeTxnResult(res TxnResult) jsonTxnResponse {
	out := jsonTxnResponse{Version: res.Version, Reads: make([]jsonReadResult, 0, len(res.Reads))}
	for _, rr := range res.Reads {
		jr := jsonReadResult{Found: rr.Found}
		if rr.Found {
			jr.Value = base64.StdEncoding.EncodeToString(rr.Value)
		}
		out.Reads = append(out.Reads, jr)
	}
	return out
}

// writeJSON encodes v as a JSON response body with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// writeErr writes a plain-text error with an explicit status, for request-decoding failures
// the caller classifies as 400.
func writeErr(w http.ResponseWriter, status int, err error) {
	http.Error(w, err.Error(), status)
}

// writeServiceErr maps a library or service error to an HTTP status (spec 17 §6): a missing
// key is 404, a conflict or failed assertion is 409, a read-only database is 405, a fenced
// or corrupt database is 503, and anything else is 500. The mapping mirrors the CLI's exit
// codes so the same typed error gives the same signal on every surface.
func writeServiceErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, kv.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, kv.ErrConflict), errors.Is(err, ErrAssertFailed):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, kv.ErrReadOnly):
		http.Error(w, err.Error(), http.StatusMethodNotAllowed)
	case errors.Is(err, kv.ErrNeedsRecovery), errors.Is(err, kv.ErrCorrupt), errors.Is(err, kv.ErrClosed):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
