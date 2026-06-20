package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/tamnd/kv"
)

// checkProblemJSON is the JSON shape for one problem, with explicit tags so the
// machine-readable output is a stable contract.
type checkProblemJSON struct {
	Class  string `json:"class"`
	Page   uint32 `json:"page"`
	Detail string `json:"detail"`
}

// checkJSON is the JSON shape `kv check -f json` emits: the summary counts and every
// problem, plus a top-level ok flag a CI script can branch on without parsing exit codes.
type checkJSON struct {
	OK           bool               `json:"ok"`
	PagesVisited int                `json:"pages_visited"`
	Keys         int64              `json:"keys"`
	FreePages    int                `json:"free_pages"`
	PageCount    uint32             `json:"page_count"`
	Problems     []checkProblemJSON `json:"problems"`
}

// cmdCheck runs a structural integrity check and reports its findings (spec 16 §4). It is
// what CI and cron run to catch corruption early: it exits 0 when the file is sound and
// exitCorrupt (4) on any violation, the signal a soundness gate keys on, independent of
// the output format. -f json emits the full report for a machine; the default is a human
// summary.
func cmdCheck(args []string) int {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	format := fs.String("f", "table", "output format: table or json")
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv check <db> [-f table|json]")
	}
	switch *format {
	case "table", "json":
	default:
		return usageErr("unknown format %q (want table or json)", *format)
	}

	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()

	rep, err := d.Check()
	if err != nil {
		return fail(err)
	}

	if *format == "json" {
		return emitCheckJSON(rep)
	}
	return emitCheckTable(rep)
}

// emitCheckJSON writes the report as a single JSON object and returns the soundness exit
// code.
func emitCheckJSON(rep *kv.CheckReport) int {
	out := checkJSON{
		OK:           rep.OK(),
		PagesVisited: rep.PagesVisited,
		Keys:         rep.Keys,
		FreePages:    rep.FreePages,
		PageCount:    rep.PageCount,
		Problems:     []checkProblemJSON{},
	}
	for _, p := range rep.Problems {
		out.Problems = append(out.Problems, checkProblemJSON{Class: p.Class, Page: p.Page, Detail: p.Detail})
	}
	bw := bufio.NewWriter(os.Stdout)
	enc := json.NewEncoder(bw)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		return fail(err)
	}
	if err := bw.Flush(); err != nil {
		return fail(err)
	}
	return checkCode(rep)
}

// emitCheckTable writes a human summary: the counts, then either "ok" or one line per
// problem grouped by the page it was found on.
func emitCheckTable(rep *kv.CheckReport) int {
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "pages visited\t%d\n", rep.PagesVisited)
	fmt.Fprintf(tw, "keys\t%d\n", rep.Keys)
	fmt.Fprintf(tw, "free pages\t%d\n", rep.FreePages)
	fmt.Fprintf(tw, "page count\t%d\n", rep.PageCount)
	if rep.OK() {
		fmt.Fprintf(tw, "result\tok\n")
		if code := flushErr(tw); code != exitOK {
			return code
		}
		return exitOK
	}
	fmt.Fprintf(tw, "result\t%d problem(s)\n", len(rep.Problems))
	if code := flushErr(tw); code != exitOK {
		return code
	}
	bw := bufio.NewWriter(os.Stderr)
	for _, p := range rep.Problems {
		if p.Page != 0 {
			fmt.Fprintf(bw, "  [%s] page %d: %s\n", p.Class, p.Page, p.Detail)
		} else {
			fmt.Fprintf(bw, "  [%s] %s\n", p.Class, p.Detail)
		}
	}
	if err := bw.Flush(); err != nil {
		return fail(err)
	}
	return checkCode(rep)
}

// checkCode is exitOK when the report is sound and exitCorrupt otherwise.
func checkCode(rep *kv.CheckReport) int {
	if rep.OK() {
		return exitOK
	}
	return exitCorrupt
}
