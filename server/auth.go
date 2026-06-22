package server

import (
	"bytes"
	"errors"
)

// This file defines the server's authentication and authorization model (spec 17 §6). The
// server is a multiplexer in front of one database, so access control is middleware in front of
// the operation surface, not a concern of the engine: the engine stores and retrieves bytes, and
// the question of whether a given caller may read or write a given key is answered before a
// Service method is ever called. The model has two halves. Authentication answers "who is this
// caller", turning a credential a request carries into an Identity or rejecting it. Authorization
// answers "may this caller do this", checking the Identity's grants against the keys an operation
// touches.
//
// The authorization model is per-key-prefix, which is the shape multi-tenant-by-prefix
// deployments already have: a tenant owns a key prefix, and a token scoped to that tenant may
// read or write only keys under that prefix. A grant is a prefix plus whether it carries write,
// so a token can be given read-only access to one prefix and read-write to another. An admin
// identity bypasses the prefix checks entirely and is the only identity allowed to drive the
// operational endpoints (stats, checkpoint, compact), since those act on the whole database
// rather than on a key range.
//
// Both protocol adapters share this one model. The HTTP adapter authenticates a bearer token and
// authorizes each request's keys; the binary adapter authenticates once per connection and
// authorizes each operation. Because the grant logic lives here and not in either adapter, a
// token means the same thing on either wire.

// ErrUnauthenticated is returned when a request carries no valid credential and the server
// requires one. The HTTP adapter maps it to 401, the signal that the caller must authenticate.
var ErrUnauthenticated = errors.New("kv: unauthenticated")

// ErrForbidden is returned when an authenticated caller lacks a grant for the keys it tried to
// touch. The HTTP adapter maps it to 403, the signal that the caller is known but not allowed.
var ErrForbidden = errors.New("kv: forbidden")

// Grant authorizes access to every key under Prefix. Write distinguishes a read-only grant from
// a read-write one: a read-only grant lets the identity get, exists, scan, and watch under the
// prefix, and a write grant additionally lets it set, delete, range-delete, and merge there. An
// empty Prefix covers the whole keyspace, so a read-only grant on the empty prefix is a global
// reader and a write grant on it is a global writer.
type Grant struct {
	Prefix []byte
	Write  bool
}

// Identity is an authenticated caller and its grants. Name labels the identity for logging and
// carries no authority of its own. Admin grants unconditional access, including to the
// operational endpoints, and is meant for an operator token rather than a tenant. Grants is the
// per-prefix access a non-admin identity carries.
type Identity struct {
	Name   string
	Admin  bool
	Grants []Grant
}

// canRead reports whether the identity may read key: an admin always may, and otherwise some
// grant's prefix must be a prefix of the key, so the key lies within a granted region.
func (id *Identity) canRead(key []byte) bool {
	if id.Admin {
		return true
	}
	for _, g := range id.Grants {
		if bytes.HasPrefix(key, g.Prefix) {
			return true
		}
	}
	return false
}

// canWrite reports whether the identity may write key: like canRead, but only write grants
// count, so a read-only grant does not authorize a mutation.
func (id *Identity) canWrite(key []byte) bool {
	if id.Admin {
		return true
	}
	for _, g := range id.Grants {
		if g.Write && bytes.HasPrefix(key, g.Prefix) {
			return true
		}
	}
	return false
}

// canReadScan reports whether the identity may scan the region the request selects. A scan that
// names a prefix is allowed when a read grant covers that prefix, since every key the scan can
// return then lies within the grant. A scan with no prefix would range over the whole keyspace,
// so only a grant on the empty prefix (or admin) authorizes it. A bounded scan with from/to but
// no prefix is treated as whole-keyspace for this check unless its bounds share a covered prefix,
// which the caller passes as the prefix argument when it can derive one; otherwise it needs a
// global read grant.
func (id *Identity) canReadScan(prefix []byte) bool {
	if id.Admin {
		return true
	}
	for _, g := range id.Grants {
		if bytes.HasPrefix(prefix, g.Prefix) {
			return true
		}
	}
	return false
}

// canWriteRange reports whether the identity may delete every key in [lo, hi). It is allowed when
// some write grant fully contains the range: the grant's prefix is a prefix of lo, and hi does
// not pass the grant's upper bound, so no key outside the grant can be deleted. An admin always
// may. A range that no single grant contains is refused even if several grants together would
// cover it, since a partial grant must not authorize deleting keys it does not cover.
func (id *Identity) canWriteRange(lo, hi []byte) bool {
	if id.Admin {
		return true
	}
	for _, g := range id.Grants {
		if !g.Write {
			continue
		}
		if !bytes.HasPrefix(lo, g.Prefix) {
			continue
		}
		// hi must not exceed the grant's prefix range. The exclusive upper bound of a prefix is
		// the prefix with its last non-0xff byte incremented; an empty prefix has no upper bound,
		// covering everything. A grant on the empty prefix therefore contains any range whose lo
		// it covers, regardless of hi.
		ub := prefixUpperBound(g.Prefix)
		if ub == nil {
			return true
		}
		// An empty hi means the range runs to the end of the keyspace, so only an unbounded grant
		// (handled above) contains it; a bounded grant does not. A non-empty hi at or below the
		// grant's upper bound keeps the whole range inside the grant.
		if len(hi) > 0 && bytes.Compare(hi, ub) <= 0 {
			return true
		}
	}
	return false
}

// canDoOp reports whether the identity may perform one operation from a transaction or batch: a
// read op needs read access to its key, a point write needs write access to its key, and a range
// delete needs write access across its whole range. An unrecognized kind is left to pass here and
// be rejected by the Service, so authorization never has to invent a verdict for an op it cannot
// classify.
func (id *Identity) canDoOp(op Op) bool {
	switch op.Kind {
	case OpGet, OpExists:
		return id.canRead(op.Key)
	case OpSet, OpDelete, OpMerge:
		return id.canWrite(op.Key)
	case OpDeleteRange:
		return id.canWriteRange(op.Lo, op.Hi)
	default:
		return true
	}
}

// canDoTxn reports whether the identity may perform a whole transaction or batch: every assert
// is a read of its key and every op is checked by its kind, so the set is allowed only when the
// identity may do all of it. Both protocol adapters check the entire set before applying any of
// it, so a partially-authorized request never commits the part it was allowed, which would break
// the atomicity the request promises. The grant logic lives here, not in either adapter, so a
// token authorizes the same transaction on either wire.
func (id *Identity) canDoTxn(asserts []Assert, ops []Op) bool {
	for _, a := range asserts {
		if !id.canRead(a.Key) {
			return false
		}
	}
	for _, op := range ops {
		if !id.canDoOp(op) {
			return false
		}
	}
	return true
}

// prefixUpperBound returns the smallest key that is greater than every key having the given
// prefix, or nil if the prefix has no upper bound (it is empty or all 0xff), meaning it covers
// keys without limit. It is the standard prefix-to-range-end transform: drop trailing 0xff bytes,
// then increment the last remaining byte.
func prefixUpperBound(prefix []byte) []byte {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] != 0xff {
			ub := make([]byte, i+1)
			copy(ub, prefix[:i+1])
			ub[i]++
			return ub
		}
	}
	return nil
}

// Authenticator turns the credential a request carries into an Identity. It is the pluggable seam
// the spec calls for: a static token table to start, with room for mTLS identities or a JWT/OIDC
// validator behind the same interface later. A nil bool result means the credential did not
// authenticate, which the adapters turn into ErrUnauthenticated.
type Authenticator interface {
	// Authenticate maps a credential (a bearer token, for the static table) to an identity. The
	// second result is false when the credential is unknown or empty.
	Authenticate(credential string) (*Identity, bool)
}

// StaticTokenAuthenticator authenticates against a fixed table of token to identity, the simplest
// real authenticator: an operator configures a handful of tokens, each bound to an identity with
// its grants. It is safe for concurrent use because the table is read-only after construction.
type StaticTokenAuthenticator struct {
	tokens map[string]*Identity
}

// NewStaticTokenAuthenticator builds a token authenticator from a token-to-identity map. The map
// is copied so a later mutation of the caller's map does not change the authenticator's table. An
// empty or nil map authenticates nothing, which makes every credentialed request unauthenticated
// rather than accidentally granting access.
func NewStaticTokenAuthenticator(tokens map[string]*Identity) *StaticTokenAuthenticator {
	cp := make(map[string]*Identity, len(tokens))
	for tok, id := range tokens {
		cp[tok] = id
	}
	return &StaticTokenAuthenticator{tokens: cp}
}

// Authenticate looks the credential up in the token table. An empty credential never
// authenticates, so a request with no token is rejected rather than matched against an empty
// table entry.
func (a *StaticTokenAuthenticator) Authenticate(credential string) (*Identity, bool) {
	if credential == "" {
		return nil, false
	}
	id, ok := a.tokens[credential]
	return id, ok
}
