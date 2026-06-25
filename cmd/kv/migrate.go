package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/tamnd/kv"
)

// cmdMigrate upgrades a v0.2.0 database to the generation-2 Bε-tree format (spec 2059
// redesign, doc 06). The old on-disk format and the new one are different containers: a
// generation-2 file bumps the magic, so a v0.2.0 binary cannot open it and this command is
// the supported one-way conversion off the old files. It reads the source through the
// shipped core, rewrites the live key space into a fresh generation-2 file beside the
// destination, verifies that file holds the same keys and values, and only then swaps it
// into place, so a crash before the swap leaves the original intact.
//
// With one path it upgrades that file in place; with two it writes the upgrade to the
// second path and leaves the source untouched. It is offline: the file must not be open
// elsewhere, and the rewrite needs room on disk for a second copy of the live data. It
// refuses a file that is already generation 2.
func cmdMigrate(args []string) int {
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 && fs.NArg() != 2 {
		return usageErr("usage: kv migrate <db> [dst]")
	}
	src := fs.Arg(0)
	dst := src
	if fs.NArg() == 2 {
		dst = fs.Arg(1)
	}

	if _, err := os.Stat(src); err != nil {
		fmt.Fprintf(os.Stderr, "kv: cannot open %s: %v\n", src, err)
		return exitOpen
	}
	if err := kv.Migrate(src, dst); err != nil {
		return fail(err)
	}
	if dst == src {
		fmt.Printf("migrated %s to the generation-2 format\n", src)
	} else {
		fmt.Printf("migrated %s to %s in the generation-2 format\n", src, dst)
	}
	return exitOK
}
