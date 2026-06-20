package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/tamnd/kv"
)

// cmdDump streams every key/value pair to stdout as JSONL, the canonical interchange
// form load reads back. It uses the streaming record writer so a large database dumps in
// flat memory (spec 16 §4).
func cmdDump(args []string) int {
	fs := flag.NewFlagSet("dump", flag.ContinueOnError)
	e := encFlags(fs)
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv dump <db> [--hex | --base64]")
	}
	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()

	w := newRecordWriter(os.Stdout, fmtJSONL, e, false)
	dumpErr := d.View(func(txn *kv.Txn) error {
		it, err := txn.NewIterator(kv.IterOptions{})
		if err != nil {
			return err
		}
		defer it.Close()
		for it.First(); it.Valid(); it.Next() {
			v, err := it.Value()
			if err != nil {
				return err
			}
			if err := w.write(it.Key(), v, false); err != nil {
				return err
			}
		}
		return it.Error()
	})
	if dumpErr != nil {
		return fail(dumpErr)
	}
	if err := w.close(); err != nil {
		return fail(err)
	}
	return exitOK
}

// cmdLoad bulk-loads JSONL key/value records from a file or stdin through db.Load, the
// sorted fast path: on a fresh database it builds the tree bottom-up and makes it durable
// with one checkpoint, far faster than inserting key by key, and on a database that
// already holds data it falls back to chunked commits. Records stream one line at a time
// so a huge import never holds the whole input in memory (spec 16 §4, spec 15 §6).
//
// The fast path requires keys in strictly ascending order, which is exactly the order kv
// dump emits, so dump | load round-trips. Unsorted input loaded into a fresh database is
// reported as an error rather than silently mis-built.
func cmdLoad(args []string) int {
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	e := encFlags(fs)
	input := fs.String("input", "-", "read records from this file (- for stdin)")
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv load <db> [--input F] [--hex | --base64]")
	}
	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()

	var in io.Reader = os.Stdin
	if *input != "-" {
		f, err := os.Open(*input)
		if err != nil {
			return fail(err)
		}
		defer f.Close()
		in = f
	}

	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	line := 0
	// decodeErr carries a per-line decode failure out of the pull function, where db.Load
	// cannot return it directly. The first one stops the stream and is reported as a usage
	// error (bad input), distinct from a load fault.
	var decodeErr error
	next := func() (key, value []byte, ok bool) {
		for sc.Scan() {
			line++
			raw := sc.Bytes()
			if len(raw) == 0 {
				continue
			}
			var r record
			if err := json.Unmarshal(raw, &r); err != nil {
				decodeErr = fmt.Errorf("line %d: bad JSON: %v", line, err)
				return nil, nil, false
			}
			k, err := e.decode(r.Key)
			if err != nil {
				decodeErr = fmt.Errorf("line %d: bad key: %v", line, err)
				return nil, nil, false
			}
			val, err := e.decode(r.Value)
			if err != nil {
				decodeErr = fmt.Errorf("line %d: bad value: %v", line, err)
				return nil, nil, false
			}
			return k, val, true
		}
		return nil, nil, false
	}

	_, loadErr := d.Load(next)
	if decodeErr != nil {
		return usageErr("%v", decodeErr)
	}
	if err := sc.Err(); err != nil {
		return fail(err)
	}
	if loadErr != nil {
		return fail(loadErr)
	}
	return exitOK
}

// cmdCheckpoint folds the WAL into the main file and resets the log, the manual analog
// of SQLite's wal_checkpoint (spec 09).
func cmdCheckpoint(args []string) int {
	fs := flag.NewFlagSet("checkpoint", flag.ContinueOnError)
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv checkpoint <db>")
	}
	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()
	if err := d.Checkpoint(); err != nil {
		return fail(err)
	}
	return exitOK
}
