package main

import (
	"bufio"
	"encoding/json"
	"flag"
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

// cmdLoad bulk-loads JSONL key/value records from a file or stdin through the explicit
// WriteBatch builder, which buffers each record and commits in bounded chunks so a huge
// import never holds the whole stream in memory (spec 16 §4, spec 15 §6).
func cmdLoad(args []string) int {
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	e := encFlags(fs)
	input := fs.String("input", "-", "read records from this file (- for stdin)")
	batch := fs.Int("batch", 1000, "records per commit")
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv load <db> [--input F] [--batch N] [--hex | --base64]")
	}
	if *batch < 1 {
		return usageErr("--batch must be at least 1")
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

	wb := d.NewWriteBatch(*batch)
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	line := 0
	for sc.Scan() {
		line++
		raw := sc.Bytes()
		if len(raw) == 0 {
			continue
		}
		var r record
		if err := json.Unmarshal(raw, &r); err != nil {
			return usageErr("line %d: bad JSON: %v", line, err)
		}
		key, err := e.decode(r.Key)
		if err != nil {
			return usageErr("line %d: bad key: %v", line, err)
		}
		val, err := e.decode(r.Value)
		if err != nil {
			return usageErr("line %d: bad value: %v", line, err)
		}
		if err := wb.Set(key, val); err != nil {
			return fail(err)
		}
	}
	if err := sc.Err(); err != nil {
		return fail(err)
	}
	if err := wb.Close(); err != nil {
		return fail(err)
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
