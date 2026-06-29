package server

import (
	"bufio"
	"context"
	"net"

	"github.com/tamnd/kv"
)

// This file is the streaming half of the binary protocol: the watch opcode, whose response is
// not one frame but a run of them. The frame format is the same self-delimiting length-prefixed
// message the unary path uses, which is exactly why a stream works: each result is its own
// frame, so the reader pulls one item at a time and the server flushes one item at a time, and a
// watch that runs for a day costs one item of memory on each side. A streaming request is a
// single request frame; the response is zero or more item frames followed by an end frame, or an
// error frame if the operation failed after it began. The streaming dispatch lives here rather
// than in the unary dispatchBinary because the connection loop must write many frames for one
// request, which the unary path's one-request-one-response shape cannot express.

// streamTag is the first byte of every frame in a streaming response. It tells the reader
// whether an item, the clean end of the stream, or a mid-stream error follows.
type streamTag byte

const (
	streamItem  streamTag = 0 // an item follows: a watch change
	streamEnd   streamTag = 1 // the stream ended cleanly; nothing follows
	streamError streamTag = 2 // a status byte then a length-prefixed message follows
)

// encodeStreamError builds an error frame: the error tag, a classified status, and the message,
// the streaming analog of the unary encodeError. A client turns it back into a typed error the
// same way it does a unary failure, so a stream that fails partway reports the same error a
// unary call would.
func encodeStreamError(s status, msg string) []byte {
	e := encoder{buf: []byte{byte(streamError), byte(s)}}
	e.bytes([]byte(msg))
	return e.buf
}

// serveBinaryWatch streams the change feed as item frames until the client disconnects or the
// server shuts down. Unlike a scan it has no natural end, so it takes over the connection for its
// whole life: the connection loop ends after this returns rather than looking for another
// request, which keeps a single reader on the socket and avoids racing the disconnect detector
// below for the stream's bytes.
//
// Two things end a watch. The server's base context (cancelled by Shutdown) is the parent of the
// watch context, so a shutdown ends every live feed. A client disconnect is detected by a small
// goroutine that blocks reading the connection; a watching client sends nothing after its
// request, so any read returning at all means the client hung up, and that cancels the feed. A
// real feed error (the consumer fell too far behind) rides an error frame before the connection
// closes, so a lagged client learns why.
func (srv *Server) serveBinaryWatch(conn net.Conn, w *bufio.Writer, body []byte, sess *binarySession) {
	d := newDecoder(body)
	d.byte() // opWatch
	prefix := d.bytes()
	since := d.uint64()
	if d.err != nil {
		writeFrame(w, encodeStreamError(statusBadRequest, "kv: malformed watch request"))
		w.Flush()
		return
	}
	// A watch delivers every committed change under its prefix, so it needs a read grant covering
	// that prefix; an empty prefix watches the whole keyspace and needs a global read grant.
	if err := srv.authorizeBinary(sess, func(id *Identity) bool { return id.canReadScan(prefix) }); err != nil {
		writeFrame(w, encodeStreamError(statusForError(err), err.Error()))
		w.Flush()
		return
	}

	ctx, cancel := context.WithCancel(srv.baseCtx)
	defer cancel()
	// A watching client only reads; any byte or EOF from it means it is done, so cancel the feed
	// when a read returns. The goroutine unblocks when the connection closes after this returns.
	go func() {
		var scratch [1]byte
		conn.Read(scratch[:])
		cancel()
	}()

	err := srv.svc.Watch(ctx, nilIfEmpty(prefix), func(batch []kv.Change) error {
		for _, c := range batch {
			if since > 0 && c.Version <= since {
				continue
			}
			e := encoder{buf: []byte{byte(streamItem)}}
			e.byte(changeKindToByte(c.Kind))
			e.bytes(c.Key)
			e.bytes(c.Value)
			e.uint64(c.Version)
			if err := writeFrame(w, e.buf); err != nil {
				return err
			}
		}
		return w.Flush()
	})
	// A cancelled context is the normal end (client hung up or server shut down); only a genuine
	// feed error is worth telling the client about, and only if the channel is still open.
	if err != nil && ctx.Err() == nil {
		writeFrame(w, encodeStreamError(statusForError(err), err.Error()))
		w.Flush()
	}
}

// changeKindToByte maps a change kind to its wire byte, the binary analog of changeKindString.
func changeKindToByte(k kv.ChangeKind) byte {
	switch k {
	case kv.ChangeSet:
		return 1
	case kv.ChangeDelete:
		return 2
	case kv.ChangeMerge:
		return 3
	case kv.ChangeRangeDelete:
		return 4
	default:
		return 0
	}
}

// changeKindFromByte is the inverse, used by the client decoder.
func changeKindFromByte(b byte) kv.ChangeKind {
	switch b {
	case 1:
		return kv.ChangeSet
	case 2:
		return kv.ChangeDelete
	case 3:
		return kv.ChangeMerge
	case 4:
		return kv.ChangeRangeDelete
	default:
		return kv.ChangeKind(0)
	}
}
