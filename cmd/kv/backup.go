package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/tamnd/kv"
)

// cmdBackup streams a consistent physical image of a database to a file or stdout (spec 18
// §2, spec 16 §4). It folds the WAL into the main file first, so the image is self-contained,
// and writes it through a buffered writer so a large database backs up in flat memory. The
// captured commit version is printed to stderr so a script can record the backup point.
func cmdBackup(args []string) int {
	fs := flag.NewFlagSet("backup", flag.ContinueOnError)
	output := fs.String("output", "-", "write the backup to this file (- for stdout)")
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv backup <db> [--output F]")
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
	version, err := d.Backup(bw)
	if err != nil {
		return fail(err)
	}
	if err := bw.Flush(); err != nil {
		return fail(err)
	}
	fmt.Fprintf(os.Stderr, "kv: backed up version %d\n", version)
	return exitOK
}

// cmdRestore reconstructs a database from a stream produced by backup, reading from a file or
// stdin (spec 18 §2). It refuses to overwrite an existing file: restore creates, it never
// clobbers, so a mistaken target fails loudly. After it returns the database is ready to open.
func cmdRestore(args []string) int {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	input := fs.String("input", "-", "read the backup from this file (- for stdin)")
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv restore <db> [--input F]")
	}

	var in io.Reader = os.Stdin
	if *input != "-" {
		f, err := os.Open(*input)
		if err != nil {
			return fail(err)
		}
		defer f.Close()
		in = f
	}
	if err := kv.RestoreBackup(fs.Arg(0), bufio.NewReader(in)); err != nil {
		return fail(err)
	}
	fmt.Fprintf(os.Stderr, "kv: restored %s\n", fs.Arg(0))
	return exitOK
}
