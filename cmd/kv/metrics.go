package main

import (
	"bufio"
	"flag"
	"os"
)

// cmdMetrics prints the database's observability metrics as Prometheus text exposition (spec
// 19 §1), the form a scrape consumes. It is the embedded-process analog of the served
// /metrics endpoint: the same numbers, rendered by the same code in the kv package, so a
// dashboard built against one works against the other.
func cmdMetrics(args []string) int {
	fs := flag.NewFlagSet("metrics", flag.ContinueOnError)
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv metrics <db>")
	}
	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()

	bw := bufio.NewWriter(os.Stdout)
	if err := d.WriteMetrics(bw); err != nil {
		return fail(err)
	}
	if err := bw.Flush(); err != nil {
		return fail(err)
	}
	return exitOK
}
