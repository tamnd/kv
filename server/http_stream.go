package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/tamnd/kv"
)

// This file holds the streaming HTTP watch handler. A watch writes its result as Server-Sent
// Events, one event per committed batch, held open until the client disconnects or the
// database closes. It relies on the http.ResponseWriter implementing http.Flusher, which the
// standard library's server always does, so an event reaches the client at the moment it is
// produced rather than when a buffer happens to fill.

// jsonChange is one SSE event of a watch: a committed mutation. Kind is the string form of
// the change kind; Key and Value are base64; Version is the commit version the batch shares.
type jsonChange struct {
	Kind    string `json:"kind"`
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"`
	Version uint64 `json:"version"`
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

// nilIfEmpty turns an empty byte slice into nil so an absent prefix reaches the library as
// unbounded rather than as an empty-string prefix, which would match nothing useful.
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
