package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/tamnd/kv"
)

// This file holds the two streaming HTTP handlers. A scan writes its result as NDJSON, one
// JSON object per line, flushed as each pair is produced, so a client reads rows as the
// server finds them and the server never buffers the whole range. A watch writes its result
// as Server-Sent Events, one event per committed batch, held open until the client
// disconnects or the database closes. Both rely on the http.ResponseWriter implementing
// http.Flusher, which the standard library's server always does, so a row or an event reaches
// the client at the moment it is produced rather than when a buffer happens to fill.

// jsonScanRow is one NDJSON line of a scan: a key and, unless the scan is keys-only, a value,
// both base64-encoded since the bytes are arbitrary.
type jsonScanRow struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// jsonChange is one SSE event of a watch: a committed mutation. Kind is the string form of
// the change kind; Key and Value are base64; Version is the commit version the batch shares.
type jsonChange struct {
	Kind    string `json:"kind"`
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"`
	Version uint64 `json:"version"`
}

// handleScan streams a range or prefix scan as NDJSON (spec 17 §2.2). The query selects the
// range: prefix, from (inclusive lower), to (exclusive upper), reverse, limit, and keys_only,
// with from/to/prefix decoded under the shared ?encoding selector. Each produced pair is
// written as one JSON line and flushed, so the response is a live stream, not a materialized
// array. An error mid-stream cannot change the already-sent status, so it ends the stream and
// is recorded in a trailing error line, the NDJSON convention for a stream that failed after
// it began.
func (srv *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	enc := q.Get("encoding")
	prefix, err := decodeBytes(q.Get("prefix"), enc)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	lower, err := decodeBytes(q.Get("from"), enc)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	upper, err := decodeBytes(q.Get("to"), enc)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	limit := 0
	if l := q.Get("limit"); l != "" {
		n, e := strconv.Atoi(l)
		if e != nil || n < 0 {
			writeErr(w, http.StatusBadRequest, errInvalidQuery("limit"))
			return
		}
		limit = n
	}
	// A scan may return any key in its selected region, so it needs a read grant covering that
	// region: the explicit prefix, or the bound the from/to range shares. Authorizing before the
	// header is written lets a denial be a clean 403 rather than a torn stream.
	if err := srv.authorize(r, func(id *Identity) bool {
		return id.canReadScan(scanAuthPrefix(prefix, lower, upper))
	}); err != nil {
		writeServiceErr(w, err)
		return
	}

	opts := ScanOptions{
		Lower:    nilIfEmpty(lower),
		Upper:    nilIfEmpty(upper),
		Prefix:   nilIfEmpty(prefix),
		Reverse:  q.Get("reverse") == "true",
		KeysOnly: q.Get("keys_only") == "true",
		Limit:    limit,
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	enc2 := json.NewEncoder(w)
	flusher, _ := w.(http.Flusher)

	scanErr := srv.svc.Scan(opts, func(key, value []byte) error {
		// A client that hung up aborts the scan early so the server stops iterating for a
		// reader that will never read.
		select {
		case <-r.Context().Done():
			return r.Context().Err()
		default:
		}
		row := jsonScanRow{Key: base64.StdEncoding.EncodeToString(key)}
		if !opts.KeysOnly {
			row.Value = base64.StdEncoding.EncodeToString(value)
		}
		if e := enc2.Encode(row); e != nil {
			return e
		}
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	})
	if scanErr != nil && r.Context().Err() == nil {
		// The header is already sent, so the failure rides a trailing JSON line the client
		// can distinguish from a row by its single error field.
		enc2.Encode(map[string]string{"error": scanErr.Error()})
		if flusher != nil {
			flusher.Flush()
		}
	}
}

// handleWatch streams the change feed as Server-Sent Events (spec 17 §2.2). The query selects
// the feed: prefix (under ?encoding) restricts to matching keys, and since, when positive,
// drops every change at or before that version so a client that reconnects does not re-see
// what it already has. The handler holds the request open and writes one data event per
// committed batch until the client disconnects (its request context is cancelled) or the
// database closes the feed. Because the library feed starts at the moment of subscription and
// carries no backlog, since only filters; it does not replay history.
func (srv *Server) handleWatch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix, err := decodeBytes(q.Get("prefix"), q.Get("encoding"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	var since uint64
	if s := q.Get("since"); s != "" {
		n, e := strconv.ParseUint(s, 10, 64)
		if e != nil {
			writeErr(w, http.StatusBadRequest, errInvalidQuery("since"))
			return
		}
		since = n
	}

	// A watch delivers every committed change under its prefix, so it needs a read grant covering
	// that prefix; an empty prefix watches the whole keyspace and needs a global read grant.
	if err := srv.authorize(r, func(id *Identity) bool { return id.canReadScan(prefix) }); err != nil {
		writeServiceErr(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	// Flush the response headers immediately. An SSE client's request does not return from
	// its round trip until the headers arrive, and this handler then blocks in Subscribe
	// waiting for the first change, so without an early flush a client that writes before it
	// reads (the common watch-then-write pattern) would deadlock against its own unsent
	// response head.
	if flusher != nil {
		flusher.Flush()
	}

	// The watch ends on a client disconnect (the request context) or a server shutdown (the
	// base context), so an idle feed never pins a drain open. AfterFunc cancels the
	// request-derived context when the base context is cancelled.
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	defer context.AfterFunc(srv.baseCtx, cancel)()

	enc := json.NewEncoder(w)
	err = srv.svc.Watch(ctx, nilIfEmpty(prefix), func(batch []kv.Change) error {
		for _, c := range batch {
			if since > 0 && c.Version <= since {
				continue
			}
			ev := jsonChange{
				Kind:    changeKindString(c.Kind),
				Key:     base64.StdEncoding.EncodeToString(c.Key),
				Version: c.Version,
			}
			if len(c.Value) > 0 {
				ev.Value = base64.StdEncoding.EncodeToString(c.Value)
			}
			// SSE frames a record as "data: <payload>\n\n"; the payload here is one JSON
			// object, so a browser EventSource or any line reader recovers it directly.
			if _, e := w.Write([]byte("data: ")); e != nil {
				return e
			}
			if e := enc.Encode(ev); e != nil {
				return e
			}
			if _, e := w.Write([]byte("\n")); e != nil {
				return e
			}
		}
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	})
	// A cancelled context is the normal end of a watch (the client hung up) and a closed
	// database is an orderly shutdown; neither is an error worth a status the client cannot
	// receive anyway, since the stream is already open.
	_ = err
}

// changeKindString maps a change kind to its wire string, the same vocabulary the op kinds
// use so a watch event and a batch op name the same operation the same way.
func changeKindString(k kv.ChangeKind) string {
	switch k {
	case kv.ChangeSet:
		return "set"
	case kv.ChangeDelete:
		return "delete"
	case kv.ChangeMerge:
		return "merge"
	case kv.ChangeRangeDelete:
		return "delete_range"
	default:
		return "unknown"
	}
}

// nilIfEmpty turns an empty byte slice into nil so an absent bound or prefix reaches the
// library as unbounded rather than as an empty-string bound, which would match nothing useful.
func nilIfEmpty(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}

// errInvalidQuery reports a malformed query parameter, mapped to 400 by the caller.
func errInvalidQuery(name string) error {
	return &queryError{name: name}
}

type queryError struct{ name string }

func (e *queryError) Error() string { return "kv: invalid query parameter " + e.name }
