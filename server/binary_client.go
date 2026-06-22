package server

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/tamnd/kv"
)

// errUnknownStreamTag reports a stream frame whose leading tag the client does not recognize, a
// protocol violation that ends the stream.
var errUnknownStreamTag = errors.New("kv: unknown stream tag")

// Client is the reference client for the binary protocol: it dials a kv server's binary
// listener and offers the operation set as Go methods, encoding each call into a request frame
// and decoding the response. It exists so a host embedding kv can talk the efficient protocol
// without re-implementing the wire, and so the protocol has a tested round-trip partner. The
// client is safe for concurrent use: each call holds a mutex for the duration of its
// request-and-response, so calls on one connection serialize rather than interleave their
// frames. A host that wants parallelism opens several clients.
type Client struct {
	mu   sync.Mutex
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

// Dial connects to a binary server at addr and returns a ready Client. The caller closes it.
func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return NewClient(conn), nil
}

// NewClient wraps an established connection, for a host that dials with its own timeouts or
// over its own transport.
func NewClient(conn net.Conn) *Client {
	return &Client{conn: conn, r: bufio.NewReader(conn), w: bufio.NewWriter(conn)}
}

// Close closes the underlying connection.
func (c *Client) Close() error { return c.conn.Close() }

// roundTrip writes one request body and reads one response body under the connection lock, the
// single choke point every call funnels through so request and response frames stay paired on
// the wire.
func (c *Client) roundTrip(body []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := writeFrame(c.w, body); err != nil {
		return nil, err
	}
	if err := c.w.Flush(); err != nil {
		return nil, err
	}
	return readFrame(c.r)
}

// Get reads a key, reporting whether it was present.
func (c *Client) Get(key []byte) (value []byte, found bool, err error) {
	e := encoder{buf: []byte{byte(opGet)}}
	e.bytes(key)
	resp, err := c.roundTrip(e.buf)
	if err != nil {
		return nil, false, err
	}
	return decodeReadResponse(resp)
}

// Exists reports whether a key is present.
func (c *Client) Exists(key []byte) (bool, error) {
	e := encoder{buf: []byte{byte(opExists)}}
	e.bytes(key)
	resp, err := c.roundTrip(e.buf)
	if err != nil {
		return false, err
	}
	_, found, err := decodeReadResponse(resp)
	return found, err
}

// Set upserts a key with an optional TTL and returns the commit version.
func (c *Client) Set(key, value []byte, ttl time.Duration) (uint64, error) {
	e := encoder{buf: []byte{byte(opSet)}}
	e.bytes(key)
	e.bytes(value)
	e.uint64(uint64(ttl / time.Millisecond))
	return c.versionCall(e.buf)
}

// Delete removes a key and returns the commit version.
func (c *Client) Delete(key []byte) (uint64, error) {
	e := encoder{buf: []byte{byte(opDelete)}}
	e.bytes(key)
	return c.versionCall(e.buf)
}

// DeleteRange removes [lo, hi) and returns the commit version.
func (c *Client) DeleteRange(lo, hi []byte) (uint64, error) {
	e := encoder{buf: []byte{byte(opDeleteRange)}}
	e.bytes(lo)
	e.bytes(hi)
	return c.versionCall(e.buf)
}

// Merge applies the merge operator to a key and returns the commit version.
func (c *Client) Merge(key, operand []byte) (uint64, error) {
	e := encoder{buf: []byte{byte(opMerge)}}
	e.bytes(key)
	e.bytes(operand)
	return c.versionCall(e.buf)
}

// Txn runs a single-shot transaction and returns its reads and commit version.
func (c *Client) Txn(req TxnRequest) (TxnResult, error) {
	e := encoder{buf: []byte{byte(opTxn)}}
	e.uint64(uint64(len(req.Asserts)))
	for _, a := range req.Asserts {
		e.bytes(a.Key)
		if a.ExpectAbsent {
			e.byte(1)
		} else {
			e.byte(0)
		}
		e.bytes(a.ExpectValue)
	}
	encodeOpList(&e, req.Ops)
	resp, err := c.roundTrip(e.buf)
	if err != nil {
		return TxnResult{}, err
	}
	return decodeTxnResponse(resp)
}

// Batch applies a list of write ops atomically and returns the commit version.
func (c *Client) Batch(ops []Op) (uint64, error) {
	e := encoder{buf: []byte{byte(opBatch)}}
	encodeOpList(&e, ops)
	return c.versionCall(e.buf)
}

// Stats returns the server's space-and-durability snapshot.
func (c *Client) Stats() (kv.Stats, error) {
	resp, err := c.roundTrip([]byte{byte(opStats)})
	if err != nil {
		return kv.Stats{}, err
	}
	d := newDecoder(resp)
	st := status(d.byte())
	if st != statusOK {
		return kv.Stats{}, decodeError(d, st)
	}
	var stats kv.Stats
	if e := unmarshalJSON(d.bytes(), &stats); e != nil {
		return kv.Stats{}, e
	}
	return stats, nil
}

// Checkpoint folds the WAL into the main file.
func (c *Client) Checkpoint() error {
	resp, err := c.roundTrip([]byte{byte(opCheckpoint)})
	if err != nil {
		return err
	}
	d := newDecoder(resp)
	if st := status(d.byte()); st != statusOK {
		return decodeError(d, st)
	}
	return nil
}

// Compact runs one bounded maintenance round and returns the pages reclaimed.
func (c *Client) Compact(budget int) (int, error) {
	e := encoder{buf: []byte{byte(opCompact)}}
	e.uint64(uint64(budget))
	resp, err := c.roundTrip(e.buf)
	if err != nil {
		return 0, err
	}
	d := newDecoder(resp)
	if st := status(d.byte()); st != statusOK {
		return 0, decodeError(d, st)
	}
	return int(d.uint64()), nil
}

// versionCall performs a request whose success response is a single commit version, the shared
// tail of every write method.
func (c *Client) versionCall(body []byte) (uint64, error) {
	resp, err := c.roundTrip(body)
	if err != nil {
		return 0, err
	}
	d := newDecoder(resp)
	if st := status(d.byte()); st != statusOK {
		return 0, decodeError(d, st)
	}
	return d.uint64(), nil
}

// Scan streams a range scan, calling yield once per pair in key order until the range is
// exhausted, the server reports an error, or yield returns an error. It holds the connection for
// the scan's duration, so it serializes with other calls on the same Client. value is nil in
// KeysOnly mode. If yield returns an error the scan stops early and the connection is closed,
// since frames the server already queued would otherwise be read by the next call; a caller that
// stops a scan early should treat the Client as spent. A clean completion leaves the connection
// reusable.
func (c *Client) Scan(opts ScanOptions, yield func(key, value []byte) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := encoder{buf: []byte{byte(opScan)}}
	e.bytes(opts.Lower)
	e.bytes(opts.Upper)
	e.bytes(opts.Prefix)
	e.byte(boolByte(opts.Reverse))
	e.byte(boolByte(opts.KeysOnly))
	e.uint64(uint64(opts.Limit))
	if err := writeFrame(c.w, e.buf); err != nil {
		return err
	}
	if err := c.w.Flush(); err != nil {
		return err
	}
	for {
		frame, err := readFrame(c.r)
		if err != nil {
			return err
		}
		d := newDecoder(frame)
		switch streamTag(d.byte()) {
		case streamItem:
			key := d.bytes()
			value := d.bytes()
			if d.err != nil {
				return d.err
			}
			if opts.KeysOnly {
				value = nil
			}
			if e := yield(key, value); e != nil {
				c.conn.Close()
				return e
			}
		case streamEnd:
			return nil
		case streamError:
			st := status(d.byte())
			return decodeError(d, st)
		default:
			return errUnknownStreamTag
		}
	}
}

// Watch streams committed changes whose key has the given prefix, calling yield once per change
// in commit order until ctx is cancelled, the server ends the feed, or yield returns an error. A
// since cursor above zero drops changes at or before that version. A watch takes over the
// connection for its life, so a Client driving a watch should be dedicated to it. Cancellation
// closes the connection to unblock the read; a ctx-cancelled watch returns ctx.Err().
func (c *Client) Watch(ctx context.Context, prefix []byte, since uint64, yield func(kv.Change) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := encoder{buf: []byte{byte(opWatch)}}
	e.bytes(prefix)
	e.uint64(since)
	if err := writeFrame(c.w, e.buf); err != nil {
		return err
	}
	if err := c.w.Flush(); err != nil {
		return err
	}
	// Cancelling ctx closes the connection so the blocking readFrame returns at once.
	stop := context.AfterFunc(ctx, func() { c.conn.Close() })
	defer stop()
	for {
		frame, err := readFrame(c.r)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		d := newDecoder(frame)
		switch streamTag(d.byte()) {
		case streamItem:
			kind := changeKindFromByte(d.byte())
			key := d.bytes()
			value := d.bytes()
			version := d.uint64()
			if d.err != nil {
				return d.err
			}
			ch := kv.Change{Kind: kind, Key: key, Value: nilIfEmpty(value), Version: version}
			if e := yield(ch); e != nil {
				c.conn.Close()
				return e
			}
		case streamEnd:
			return nil
		case streamError:
			st := status(d.byte())
			return decodeError(d, st)
		default:
			return errUnknownStreamTag
		}
	}
}

// boolByte encodes a bool as a wire byte, the encoding the scan options use for their flags.
func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// encodeOpList encodes a count-prefixed op list, the client mirror of decodeOpList.
func encodeOpList(e *encoder, ops []Op) {
	e.uint64(uint64(len(ops)))
	for _, op := range ops {
		e.byte(opKindToByte(op.Kind))
		e.bytes(op.Key)
		e.bytes(op.Value)
		e.bytes(op.Lo)
		e.bytes(op.Hi)
		e.uint64(uint64(op.TTL / time.Millisecond))
	}
}

// decodeReadResponse decodes a get/exists response into a value and found flag, mapping a
// non-OK status to a typed error.
func decodeReadResponse(resp []byte) ([]byte, bool, error) {
	d := newDecoder(resp)
	st := status(d.byte())
	if st != statusOK {
		return nil, false, decodeError(d, st)
	}
	if d.byte() == 0 {
		return nil, false, nil
	}
	return d.bytes(), true, d.err
}

// decodeTxnResponse decodes a transaction response into its reads and version.
func decodeTxnResponse(resp []byte) (TxnResult, error) {
	d := newDecoder(resp)
	st := status(d.byte())
	if st != statusOK {
		return TxnResult{}, decodeError(d, st)
	}
	var res TxnResult
	res.Version = d.uint64()
	n := d.uint64()
	for i := uint64(0); i < n; i++ {
		if d.byte() == 1 {
			res.Reads = append(res.Reads, ReadResult{Found: true, Value: d.bytes()})
		} else {
			res.Reads = append(res.Reads, ReadResult{})
		}
	}
	if d.err != nil {
		return TxnResult{}, d.err
	}
	return res, nil
}

// decodeError reads the message that follows a non-OK status and reconstructs the typed error.
func decodeError(d *decoder, st status) error {
	msg := string(d.bytes())
	return errBinaryStatus(st, msg)
}
