package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/tamnd/kv"
)

// cmdShip streams the current WAL generation of a database to a file or stdout as a
// replication delta (spec 18 §4, spec 16 §4). It is the primary side of WAL shipping: pipe
// the output to kv replay on a follower to advance it to the same version. Shipping does
// not checkpoint, so it carries the committed tail the follower still needs; the captured
// commit version is printed to stderr so a script can track progress.
func cmdShip(args []string) int {
	fs := flag.NewFlagSet("ship", flag.ContinueOnError)
	output := fs.String("output", "-", "write the WAL delta to this file (- for stdout)")
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv ship <db> [--output F]")
	}
	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()

	var out io.Writer = os.Stdout
	if *output != "-" {
		f, err := os.Create(*output)
		if err != nil {
			return fail(err)
		}
		defer f.Close()
		out = f
	}
	bw := bufio.NewWriter(out)
	version, err := d.ShipWAL(bw)
	if err != nil {
		return fail(err)
	}
	if err := bw.Flush(); err != nil {
		return fail(err)
	}
	fmt.Fprintf(os.Stderr, "kv: shipped through version %d\n", version)
	return exitOK
}

// cmdReplay applies a WAL delta produced by ship onto a follower database, reading from a
// file or stdin (spec 18 §4). It opens the follower read-only and replays the shipped
// frames through the redo path, advancing it to the primary's version. A gap (the primary
// checkpointed away frames the follower never saw) is reported and the follower must be
// re-seeded from a full restore. With --until V it stops after version V, leaving later
// commits unapplied: restore a base, then replay archived deltas with the same --until to
// roll forward to an exact point in time (spec 18 §6). The new applied version is printed
// to stderr.
func cmdReplay(args []string) int {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	input := fs.String("input", "-", "read the WAL delta from this file (- for stdin)")
	until := fs.Uint64("until", 0, "stop after this commit version (0 replays the whole delta)")
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv replay <db> [--input F] [--until V]")
	}
	d, code := openDB(fs.Arg(0), kv.WithReadReplica())
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
	target := *until
	if target == 0 {
		target = ^uint64(0) // unbounded: replay the whole delta
	}
	version, err := d.ApplyWALUntil(bufio.NewReader(in), target)
	if err != nil {
		return fail(err)
	}
	fmt.Fprintf(os.Stderr, "kv: replayed to version %d\n", version)
	return exitOK
}
