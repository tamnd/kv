package server

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"
)

// This file parses a token table from a small line-oriented text file, the format `kv serve`
// reads to turn an operator's token list into a StaticTokenAuthenticator. The library half of the
// server stays free of any file format, but a server needs some concrete way to be told its
// tokens, and a flat text file is the zero-dependency one: no YAML or JSON parser, no schema, just
// one identity per line. An embedding host that prefers to build identities in code ignores this
// and constructs a StaticTokenAuthenticator (or its own Authenticator) directly.
//
// The format is one identity per line:
//
//	<token> <name> <grant>[,<grant>...]
//
// where each grant is one of:
//
//	admin        the identity is an administrator (full access, including the ops endpoints)
//	r:<prefix>   read access to every key under <prefix>
//	rw:<prefix>  read and write access to every key under <prefix>
//
// A prefix may be empty (r: or rw: with nothing after the colon) to grant global read or write.
// Blank lines and lines beginning with '#' are ignored, so the file can carry comments. A token
// must be unique; a repeated token is an error rather than a silent last-wins, since a duplicate
// almost always means a copy-paste mistake an operator wants to hear about.

// errEmptyToken and friends name the parse failures so a caller can tell a malformed line from an
// I/O error.
var (
	errEmptyToken    = errors.New("kv: auth config: empty token")
	errDupToken      = errors.New("kv: auth config: duplicate token")
	errMissingGrants = errors.New("kv: auth config: missing grants")
	errBadGrant      = errors.New("kv: auth config: malformed grant")
)

// ParseTokenAuth reads a token table from r and returns a StaticTokenAuthenticator. It returns an
// error on the first malformed line or on a duplicate token, naming the line number, so a
// misconfigured file fails loudly at startup rather than silently granting or denying access. An
// empty input yields an authenticator that authenticates nothing, which a caller can detect and
// treat as "auth requested but no tokens", though it is usually a sign the file was wrong.
func ParseTokenAuth(r io.Reader) (*StaticTokenAuthenticator, error) {
	tokens := map[string]*Identity{}
	sc := bufio.NewScanner(r)
	line := 0
	for sc.Scan() {
		line++
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		id, token, err := parseTokenLine(text)
		if err != nil {
			return nil, lineError(line, err)
		}
		if _, dup := tokens[token]; dup {
			return nil, lineError(line, errDupToken)
		}
		tokens[token] = id
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return NewStaticTokenAuthenticator(tokens), nil
}

// parseTokenLine parses one non-comment line into its token and identity.
func parseTokenLine(text string) (id *Identity, token string, err error) {
	fields := strings.Fields(text)
	if len(fields) < 1 || fields[0] == "" {
		return nil, "", errEmptyToken
	}
	token = fields[0]
	if len(fields) < 3 {
		return nil, "", errMissingGrants
	}
	name := fields[1]
	id = &Identity{Name: name}
	for _, spec := range strings.Split(fields[2], ",") {
		spec = strings.TrimSpace(spec)
		if spec == "" {
			continue
		}
		if spec == "admin" {
			id.Admin = true
			continue
		}
		g, err := parseGrant(spec)
		if err != nil {
			return nil, "", err
		}
		id.Grants = append(id.Grants, g)
	}
	if !id.Admin && len(id.Grants) == 0 {
		return nil, "", errMissingGrants
	}
	return id, token, nil
}

// parseGrant parses one grant spec ("r:<prefix>" or "rw:<prefix>") into a Grant.
func parseGrant(spec string) (Grant, error) {
	switch {
	case strings.HasPrefix(spec, "rw:"):
		return Grant{Prefix: []byte(spec[len("rw:"):]), Write: true}, nil
	case strings.HasPrefix(spec, "r:"):
		return Grant{Prefix: []byte(spec[len("r:"):])}, nil
	default:
		return Grant{}, errBadGrant
	}
}

// lineError wraps a parse error with its line number so the message points an operator at the
// offending line.
func lineError(line int, err error) error {
	return &authConfigError{line: line, err: err}
}

type authConfigError struct {
	line int
	err  error
}

func (e *authConfigError) Error() string {
	return e.err.Error() + " at line " + strconv.Itoa(e.line)
}
func (e *authConfigError) Unwrap() error { return e.err }
