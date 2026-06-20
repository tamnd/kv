package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/tamnd/kv"
)

// encFlags registers the shared --hex/--base64 binary-encoding flags on a flag set.
func encFlags(fs *flag.FlagSet) *enc {
	e := &enc{}
	fs.BoolVar(&e.hex, "hex", false, "keys and values are hex-encoded")
	fs.BoolVar(&e.base64, "base64", false, "keys and values are base64-encoded")
	return e
}

// cmdCreate creates a new database file with create-time options. An existing file is
// an error: create never clobbers.
func cmdCreate(args []string) int {
	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	engine := fs.String("engine", "btree", "storage core: btree or lsm")
	pageSize := fs.Int("page-size", 0, "page size in bytes (0 = default)")
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv create <db> [--engine btree|lsm] [--page-size N]")
	}
	path := fs.Arg(0)
	if _, err := os.Stat(path); err == nil {
		return usageErr("refusing to create %s: file already exists", path)
	}

	var opts []kv.Option
	switch *engine {
	case "btree":
		opts = append(opts, kv.WithEngine(kv.BTree))
	case "lsm":
		opts = append(opts, kv.WithEngine(kv.LSM))
	default:
		return usageErr("unknown engine %q (want btree or lsm)", *engine)
	}
	if *pageSize > 0 {
		opts = append(opts, kv.WithPageSize(*pageSize))
	}

	d, err := kv.Open(path, opts...)
	if err != nil {
		return fail(err)
	}
	if err := d.Close(); err != nil {
		return fail(err)
	}
	return exitOK
}

// cmdGet prints the value for a key, or exits 1 if the key is absent.
func cmdGet(args []string) int {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	e := encFlags(fs)
	raw := fs.Bool("raw", false, "print value bytes only, no newline")
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 2 {
		return usageErr("usage: kv get <db> <key>")
	}
	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()

	key, err := e.decode(fs.Arg(1))
	if err != nil {
		return usageErr("bad key: %v", err)
	}
	var val []byte
	verr := d.View(func(txn *kv.Txn) error {
		v, err := txn.GetCopy(key)
		val = v
		return err
	})
	if errors.Is(verr, kv.ErrNotFound) {
		return exitNotFound
	}
	if verr != nil {
		return fail(verr)
	}
	if *raw {
		os.Stdout.Write(val)
	} else {
		fmt.Println(e.encode(val))
	}
	return exitOK
}

// cmdSet upserts a key to a value. The value comes from the positional argument,
// --value-file, or stdin when the value argument is omitted.
func cmdSet(args []string) int {
	fs := flag.NewFlagSet("set", flag.ContinueOnError)
	e := encFlags(fs)
	valueFile := fs.String("value-file", "", "read the value from this file (- for stdin)")
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() < 2 {
		return usageErr("usage: kv set <db> <key> <value> | kv set <db> <key> --value-file F")
	}
	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()

	key, err := e.decode(fs.Arg(1))
	if err != nil {
		return usageErr("bad key: %v", err)
	}
	value, code := resolveValue(fs, e, *valueFile)
	if code != exitOK {
		return code
	}
	if err := d.Update(func(txn *kv.Txn) error { return txn.Set(key, value) }); err != nil {
		return fail(err)
	}
	return exitOK
}

// resolveValue produces the value for set/merge from the positional argument, a file,
// or stdin.
func resolveValue(fs *flag.FlagSet, e *enc, valueFile string) ([]byte, int) {
	if valueFile != "" {
		var b []byte
		var err error
		if valueFile == "-" {
			b, err = io.ReadAll(os.Stdin)
		} else {
			b, err = os.ReadFile(valueFile)
		}
		if err != nil {
			return nil, fail(err)
		}
		return b, exitOK
	}
	if fs.NArg() < 3 {
		return nil, usageErr("missing value (provide it as an argument or with --value-file)")
	}
	v, err := e.decode(fs.Arg(2))
	if err != nil {
		return nil, usageErr("bad value: %v", err)
	}
	return v, exitOK
}

// cmdDel deletes one key. Deleting an absent key is a no-op success (idempotent).
func cmdDel(args []string) int {
	fs := flag.NewFlagSet("del", flag.ContinueOnError)
	e := encFlags(fs)
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 2 {
		return usageErr("usage: kv del <db> <key>")
	}
	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()

	key, err := e.decode(fs.Arg(1))
	if err != nil {
		return usageErr("bad key: %v", err)
	}
	if err := d.Update(func(txn *kv.Txn) error { return txn.Delete(key) }); err != nil {
		return fail(err)
	}
	return exitOK
}

// cmdDelRange range-deletes [lo, hi).
func cmdDelRange(args []string) int {
	fs := flag.NewFlagSet("del-range", flag.ContinueOnError)
	e := encFlags(fs)
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 3 {
		return usageErr("usage: kv del-range <db> <lo> <hi>")
	}
	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()

	lo, err := e.decode(fs.Arg(1))
	if err != nil {
		return usageErr("bad lo: %v", err)
	}
	hi, err := e.decode(fs.Arg(2))
	if err != nil {
		return usageErr("bad hi: %v", err)
	}
	if err := d.Update(func(txn *kv.Txn) error { return txn.DeleteRange(lo, hi) }); err != nil {
		return fail(err)
	}
	return exitOK
}

// cmdExists exits 0 if the key is present, 1 if absent (key-only, no value fetch).
func cmdExists(args []string) int {
	fs := flag.NewFlagSet("exists", flag.ContinueOnError)
	e := encFlags(fs)
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 2 {
		return usageErr("usage: kv exists <db> <key>")
	}
	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()

	key, err := e.decode(fs.Arg(1))
	if err != nil {
		return usageErr("bad key: %v", err)
	}
	var present bool
	if err := d.View(func(txn *kv.Txn) error {
		ok, err := txn.Exists(key)
		present = ok
		return err
	}); err != nil {
		return fail(err)
	}
	if !present {
		return exitNotFound
	}
	return exitOK
}

// cmdMerge applies the registered merge operator to a key. Without a merge operator
// registered, the library treats the operand as a plain set; the CLI cannot register
// one, so this records a blind operand the next reader resolves against the default.
func cmdMerge(args []string) int {
	fs := flag.NewFlagSet("merge", flag.ContinueOnError)
	e := encFlags(fs)
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 3 {
		return usageErr("usage: kv merge <db> <key> <operand>")
	}
	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()

	key, err := e.decode(fs.Arg(1))
	if err != nil {
		return usageErr("bad key: %v", err)
	}
	operand, err := e.decode(fs.Arg(2))
	if err != nil {
		return usageErr("bad operand: %v", err)
	}
	if err := d.Update(func(txn *kv.Txn) error { return txn.Merge(key, operand) }); err != nil {
		return fail(err)
	}
	return exitOK
}
