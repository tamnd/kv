package main

import (
	"flag"
	"fmt"
)

// cmdVacuum runs an incremental vacuum, returning trailing free pages to the operating
// system so the file shrinks after large deletes, the analog of SQLite's
// "PRAGMA incremental_vacuum" (spec 09 §3.1, spec 16). It folds the WAL with a
// checkpoint first, then truncates the run of free pages at the end of the file.
//
// -n bounds how many pages a single call reclaims (zero, the default, reclaims the whole
// trailing run); --incremental is accepted as a no-op spelling so the command reads the
// way the SQLite pragma does. It prints how many pages were freed and the resulting page
// count.
func cmdVacuum(args []string) int {
	fs := flag.NewFlagSet("vacuum", flag.ContinueOnError)
	budget := fs.Int("n", 0, "max pages to reclaim this round (0 = the whole trailing free run)")
	fs.Bool("incremental", false, "accepted spelling for the incremental vacuum; it is always incremental")
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv vacuum <db> [-n pages] [--incremental]")
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
