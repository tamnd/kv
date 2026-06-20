package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/tamnd/kv"
)

// cmdVacuum reclaims space, in one of two modes (spec 09 §3, spec 16). The default is an
// incremental vacuum, the analog of SQLite's "PRAGMA incremental_vacuum": it folds the WAL
// with a checkpoint, then returns the run of free pages at the end of the file to the
// operating system so the file shrinks after large deletes. --full instead runs a full
// vacuum: it rebuilds the file from scratch into a fresh, maximally compact copy holding
// only the live data and swaps it in, reclaiming everything obsolete versions, tombstones,
// and freelist holes were holding.
//
// -n bounds how many pages a single incremental call reclaims (zero, the default, reclaims
// the whole trailing run); --incremental is accepted as a no-op spelling so the default
// reads the way the SQLite pragma does. The incremental mode prints how many pages were
// freed and the resulting page count; the full mode prints the page count before and after.
func cmdVacuum(args []string) int {
	fs := flag.NewFlagSet("vacuum", flag.ContinueOnError)
	budget := fs.Int("n", 0, "max pages to reclaim this round (0 = the whole trailing free run)")
	full := fs.Bool("full", false, "rebuild the file from scratch into a maximally compact copy")
	fs.Bool("incremental", false, "accepted spelling for the incremental vacuum; it is the default")
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv vacuum <db> [--full | -n pages | --incremental]")
	}
	if *full {
		return vacuumFull(fs.Arg(0))
	}

	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()

	freed, err := d.Vacuum(*budget)
	if err != nil {
		return fail(err)
	}
	fmt.Printf("freed %d page(s), %d page(s) remain\n", freed, d.Stats().PageCount)
	return exitOK
}

// vacuumFull rebuilds the database in place. It is offline, so it must open the file itself
// rather than reuse an open handle: it reads the page count before, runs kv.Compact (which
// opens, rebuilds, and atomically swaps), then reopens just to report the page count after.
func vacuumFull(path string) int {
	if _, err := os.Stat(path); err != nil {
		fmt.Fprintf(os.Stderr, "kv: cannot open %s: %v\n", path, err)
		return exitOpen
	}
	before, code := pageCount(path)
	if code != exitOK {
		return code
	}
	if err := kv.Compact(path); err != nil {
		return fail(err)
	}
	after, code := pageCount(path)
	if code != exitOK {
		return code
	}
	fmt.Printf("compacted %d page(s) to %d\n", before, after)
	return exitOK
}

// pageCount opens the database briefly to read its page count, the figure vacuum reports on
// each side of a full rebuild.
func pageCount(path string) (uint32, int) {
	d, code := openDB(path)
	if code != exitOK {
		return 0, code
	}
	defer d.Close()
	return d.Stats().PageCount, exitOK
}
