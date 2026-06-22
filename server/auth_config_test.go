package server

import (
	"errors"
	"strings"
	"testing"
)

func TestParseTokenAuth(t *testing.T) {
	cfg := `
# a comment line and a blank line above
admintok  ops    admin
rwtok     tenant rw:t1/
rotok     reader r:t1/
multitok  mixed  r:ro/,rw:rw/
globaltok all    rw:
`
	a, err := ParseTokenAuth(strings.NewReader(cfg))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// admin
	if id, ok := a.Authenticate("admintok"); !ok || !id.Admin {
		t.Fatalf("admintok did not resolve to an admin")
	}
	// rw on a prefix
	if id, ok := a.Authenticate("rwtok"); !ok || !id.canWrite([]byte("t1/a")) || id.canWrite([]byte("t2/a")) {
		t.Fatalf("rwtok grants wrong")
	}
	// ro on a prefix
	if id, ok := a.Authenticate("rotok"); !ok || id.canWrite([]byte("t1/a")) || !id.canRead([]byte("t1/a")) {
		t.Fatalf("rotok grants wrong")
	}
	// two grants on one token
	if id, ok := a.Authenticate("multitok"); !ok ||
		!id.canRead([]byte("ro/x")) || id.canWrite([]byte("ro/x")) ||
		!id.canWrite([]byte("rw/x")) {
		t.Fatalf("multitok grants wrong")
	}
	// empty-prefix global write
	if id, ok := a.Authenticate("globaltok"); !ok || !id.canWrite([]byte("anything")) {
		t.Fatalf("globaltok should be a global writer")
	}
}

func TestParseTokenAuthErrors(t *testing.T) {
	cases := []struct {
		name string
		cfg  string
		want error
	}{
		{"missing grants", "tok name", errMissingGrants},
		{"no grant after name", "tok name ", errMissingGrants},
		{"bad grant verb", "tok name x:foo", errBadGrant},
		{"duplicate token", "tok a admin\ntok b admin", errDupToken},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseTokenAuth(strings.NewReader(c.cfg))
			if !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
		})
	}
}

func TestParseTokenAuthEmpty(t *testing.T) {
	// An all-comment file authenticates nothing rather than erroring, so a caller can decide what
	// an empty table means.
	a, err := ParseTokenAuth(strings.NewReader("# nothing here\n\n"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, ok := a.Authenticate("whatever"); ok {
		t.Fatalf("empty table authenticated a token")
	}
}
