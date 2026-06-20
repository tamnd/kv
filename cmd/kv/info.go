package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/tamnd/kv"
)

// statsJSON is the JSON shape info and stats emit. It mirrors kv.Stats with explicit
// field names and tags so the machine-readable output is a stable contract independent
// of the Go struct's field order.
type statsJSON struct {
	Engine        string  `json:"engine"`
	PageSize      int     `json:"page_size"`
	PageCount     uint32  `json:"page_count"`
	FreePages     int64   `json:"free_pages"`
	PhysicalBytes int64   `json:"physical_bytes"`
	LiveKeys      int64   `json:"live_keys"`
	LiveBytes     int64   `json:"live_bytes"`
	Amplification float64 `json:"amplification"`
	Version       uint64  `json:"version"`
	WALFrames     uint64  `json:"wal_frames"`
	WALBacklog    uint64  `json:"wal_backlog"`
	Syncs         uint64  `json:"syncs"`
}

func toJSON(s kv.Stats) statsJSON {
	return statsJSON{
		Engine:        s.Engine.String(),
		PageSize:      s.PageSize,
		PageCount:     s.PageCount,
		FreePages:     s.FreePages,
		PhysicalBytes: s.PhysicalBytes,
		LiveKeys:      s.LiveKeys,
		LiveBytes:     s.LiveBytes,
		Amplification: s.Amplification,
		Version:       s.Version,
		WALFrames:     s.WALFrames,
		WALBacklog:    s.WALBacklog,
		Syncs:         s.Syncs,
	}
}

// cmdInfo prints a human-readable summary of the database: its format, size, and
// durability backlog. It is the at-a-glance operational view (spec 09 §4, spec 19).
func cmdInfo(args []string) int {
	fs := flag.NewFlagSet("info", flag.ContinueOnError)
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv info <db>")
	}
	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()

	if err := writeInfo(os.Stdout, d.Stats()); err != nil {
		return fail(err)
	}
	return exitOK
}

// writeInfo renders the human-readable info table for a stats snapshot to w. It is shared
// by the info command and the shell's .info dot-command so both print the same summary.
func writeInfo(w io.Writer, s kv.Stats) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "engine\t%s\n", s.Engine)
	fmt.Fprintf(tw, "page size\t%d bytes\n", s.PageSize)
	fmt.Fprintf(tw, "page count\t%d\n", s.PageCount)
	fmt.Fprintf(tw, "file size\t%d bytes\n", int64(s.PageCount)*int64(s.PageSize))
	fmt.Fprintf(tw, "free pages\t%d\n", s.FreePages)
	fmt.Fprintf(tw, "physical bytes\t%d\n", s.PhysicalBytes)
	if s.LiveKeys > 0 || s.LiveBytes > 0 {
		fmt.Fprintf(tw, "live keys\t%d\n", s.LiveKeys)
		fmt.Fprintf(tw, "live bytes\t%d\n", s.LiveBytes)
	}
	fmt.Fprintf(tw, "amplification\t%.2f\n", s.Amplification)
	fmt.Fprintf(tw, "commit version\t%d\n", s.Version)
	fmt.Fprintf(tw, "wal frames\t%d\n", s.WALFrames)
	fmt.Fprintf(tw, "wal backlog\t%d frames\n", s.WALBacklog)
	fmt.Fprintf(tw, "syncs\t%d\n", s.Syncs)
	return tw.Flush()
}

// cmdStats prints the same accounting as machine-readable JSON, the form a monitoring
// script consumes (spec 19). -f selects json (default) or jsonl, which are identical for
// a single object but distinguished so it composes the same way scan does.
func cmdStats(args []string) int {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	format := fs.String("f", "json", "output format: json or jsonl")
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv stats <db> [-f json|jsonl]")
	}
	switch *format {
	case "json", "jsonl":
	default:
		return usageErr("unknown format %q (want json or jsonl)", *format)
	}
	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()

	bw := bufio.NewWriter(os.Stdout)
	enc := json.NewEncoder(bw)
	if *format == "json" {
		enc.SetIndent("", "  ")
	}
	if err := enc.Encode(toJSON(d.Stats())); err != nil {
		return fail(err)
	}
	if err := bw.Flush(); err != nil {
		return fail(err)
	}
	return exitOK
}

// flushErr flushes a tabwriter and maps a write failure to an exit code.
func flushErr(tw *tabwriter.Writer) int {
	if err := tw.Flush(); err != nil {
		return fail(err)
	}
	return exitOK
}
