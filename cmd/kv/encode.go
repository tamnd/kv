package main

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/tamnd/kv"
)

// boolFlag is the unexported interface flag uses to mark valueless flags; we detect it
// so the reorder below knows which flags consume the following token.
type boolFlag interface{ IsBoolFlag() bool }

// parseArgs parses args into fs while allowing flags to appear after positional
// arguments, which the standard flag package does not: it stops at the first
// non-flag. Commands here read like `kv scan <db> --prefix p`, with the db path first,
// so we partition tokens into flags (with their values) and positionals, then feed the
// flags first. This keeps the natural `kv <cmd> <db> [flags]` order working.
func parseArgs(fs *flag.FlagSet, args []string) error {
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		if len(a) > 1 && a[0] == '-' {
			flags = append(flags, a)
			name := strings.TrimLeft(a, "-")
			if strings.IndexByte(name, '=') >= 0 {
				continue // self-contained --name=value
			}
			f := fs.Lookup(name)
			if f != nil {
				if bf, ok := f.Value.(boolFlag); !ok || !bf.IsBoolFlag() {
					if i+1 < len(args) {
						flags = append(flags, args[i+1])
						i++
					}
				}
			}
			continue
		}
		positional = append(positional, a)
	}
	return fs.Parse(append(flags, positional...))
}

// enc selects how key/value bytes are decoded from arguments and encoded for output,
// so the tool is safe on non-text data (spec 16 §2).
type enc struct {
	hex    bool
	base64 bool
}

// decode turns a command-line argument into raw bytes per the active encoding.
func (e enc) decode(s string) ([]byte, error) {
	switch {
	case e.hex:
		return hex.DecodeString(s)
	case e.base64:
		return base64.StdEncoding.DecodeString(s)
	default:
		return []byte(s), nil
	}
}

// encode renders raw bytes for output per the active encoding.
func (e enc) encode(b []byte) string {
	switch {
	case e.hex:
		return hex.EncodeToString(b)
	case e.base64:
		return base64.StdEncoding.EncodeToString(b)
	default:
		return string(b)
	}
}

// openDB opens the database at path for the CLI, mapping an open failure to the right
// exit code. A missing file is exit 3.
func openDB(path string, opts ...kv.Option) (*kv.DB, int) {
	if _, err := os.Stat(path); err != nil {
		fmt.Fprintf(os.Stderr, "kv: cannot open %s: %v\n", path, err)
		return nil, exitOpen
	}
	d, err := kv.Open(path, opts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kv: cannot open %s: %v\n", path, err)
		return nil, codeFor(err)
	}
	return d, exitOK
}

// codeFor maps a library error to the CLI exit code that mirrors it (spec 16 §7).
func codeFor(err error) int {
	switch {
	case err == nil:
		return exitOK
	case errors.Is(err, kv.ErrNotFound):
		return exitNotFound
	case errors.Is(err, kv.ErrConflict):
		return exitConflict
	case errors.Is(err, kv.ErrCorrupt):
		return exitCorrupt
	case errors.Is(err, kv.ErrNeedsRecovery):
		return exitLock
	case errors.Is(err, kv.ErrReadOnly):
		return exitUsage
	default:
		return exitIO
	}
}

// fail prints an error and returns its exit code, for the common one-line error path.
func fail(err error) int {
	fmt.Fprintf(os.Stderr, "kv: %v\n", err)
	return codeFor(err)
}

// usageErr prints a usage message for a subcommand and returns exit 2.
func usageErr(format string, a ...any) int {
	fmt.Fprintf(os.Stderr, "kv: "+format+"\n", a...)
	return exitUsage
}
