package bench

import (
	"fmt"
	"strings"
	"time"
)

// This file turns a measured Report into the two things spec 24's M5 exit asks for: a human
// readable results document, and a verdict on whether the spec 21 §6 targets and the
// B-tree/LSM tradeoff are demonstrated by the numbers rather than asserted in prose. The
// rendering is a pure function of a Report so it is unit-testable on a synthetic report
// without running the heavy suite; the runner in cmd/bench produces the real report and feeds
// it here.

// Finding is one falsifiable claim checked against the measured numbers. It pairs the spec
// target with what the run actually showed and whether the claim held, so the published
// document can say "demonstrated" with a number behind it or "not shown here" honestly.
type Finding struct {
	// Target names the spec 21 §6 target or the tradeoff direction being checked.
	Target string
	// Claim is the expectation the architecture predicts, in plain words.
	Claim string
	// Observed is the measured evidence, with the numbers that decide the verdict.
	Observed string
	// Holds is whether the measured numbers bear the claim out on this run. A false here is
	// not a test failure; it is a published fact about this machine and this size.
	Holds bool
}

// find returns the result for one engine and workload, or nil if the report has no such cell.
func (r Report) find(engine, workload string) *Result {
	for i := range r.Results {
		if r.Results[i].Engine == engine && r.Results[i].Workload == workload {
			return &r.Results[i]
		}
	}
	return nil
}

// microsecondClass is the ceiling for a "low-µs" cache-resident read tail. The §6 read target
// is stated in microseconds, not milliseconds: a B-tree point read that never leaves the
// buffer pool is CPU and cache work, so it stays microsecond-class on any machine. A regression
// that pushed it into the millisecond range (a stray page fault per read, a lock convoy) would
// cross this line. The exact figure is machine-dependent; the order of magnitude is not.
const microsecondClass = 100 * time.Microsecond

// cacheResidentReadIOs is the most extra page I/O a genuinely cache-resident read phase may
// charge per op before we stop calling it cache-resident. A truly warm read does zero page
// reads; this leaves slack for a cold-start page or two without letting a real miss stream by.
const cacheResidentReadIOs = 0.5

// Tradeoff evaluates the falsifiable facts this suite can actually adjudicate on a developer
// machine. The absolute latency and throughput targets in §6 are stated against an NVMe
// reference machine, so they are not asserted here. What is machine-independent in shape, and
// so genuinely demonstrable on any box, is: the B-tree serves cache-resident reads in the
// microsecond class with no extra I/O (the read-latency reference, §6); the LSM's log-structured
// write path writes less per op than the B-tree's in-place updates (the write-amplification
// corner of the RUM triangle); the LSM ingests at least as fast as the B-tree once batches are
// not pinned to per-op fsync; and the suite drops nothing. Those four are what this checks.
//
// Two facts that a single SyncFull, cache-resident, single-thread run cannot honestly speak to
// are deliberately left out. The cross-engine read race is regime-dependent: at a size where the
// LSM's data is memtable-resident a skiplist lookup beats a page walk, so "B-tree reads faster"
// is an out-of-cache phenomenon, not a cache-resident one, and asserting it here would be wrong.
// And the worst GC pause is a process-global number that folds in the benchmark driver's own
// per-op allocations, so it bounds the system, not the engine arena; it is disclosed by the
// renderer as context rather than scored as an engine pass or fail.
func Tradeoff(rep Report) []Finding {
	var out []Finding

	// The read-latency reference: on the read-only, cache-resident workload the B-tree reaches a
	// key entirely inside the buffer pool, so its read tail is microsecond-class and it issues no
	// extra page I/O. This is the "B-tree is the read-latency reference" claim (spec 05 §7, spec
	// 21 §6) made measurable in the form §6 actually states it: a low-µs cache-resident p99.
	if bt := rep.find("btree", "ycsb-c"); bt != nil {
		holds := bt.Reads.P99 > 0 && bt.Reads.P99 < microsecondClass &&
			bt.Amplification.Read >= 0 && bt.Amplification.Read < cacheResidentReadIOs
		out = append(out, Finding{
			Target: "Read latency reference (B-tree, YCSB-C cache-resident)",
			Claim:  "B-tree cache-resident read p99 is microsecond-class with no extra page I/O",
			Observed: fmt.Sprintf("btree p99 %s at %.2f read-ios/op",
				dur(bt.Reads.P99), bt.Amplification.Read),
			Holds: holds,
		})
	}

	// The write-amplification corner: on the write-saturated ingest the LSM turns random keys
	// into sequential segment appends, where the in-place B-tree pays a scattered page write per
	// update. The footprint write-factor captures that the LSM writes less storage per logical op.
	// This is the write side of the RUM triangle the LSM is built to win (spec 06, spec 21 §6),
	// and unlike raw throughput it is not masked when both engines sit behind the same fsync.
	if bt := rep.find("btree", "write-saturated"); bt != nil {
		if ls := rep.find("lsm", "write-saturated"); ls != nil {
			holds := ls.Amplification.Write <= bt.Amplification.Write
			out = append(out, Finding{
				Target: "Write amplification (LSM below B-tree, write-saturated)",
				Claim:  "LSM write-factor is at or below the B-tree's: log-structured writes cost less per op",
				Observed: fmt.Sprintf("lsm write-factor %s vs btree %s",
					amp(ls.Amplification.Write), amp(bt.Amplification.Write)),
				Holds: holds,
			})
		}
	}

	// The ingest-rate tradeoff, read where it is visible: the bulk load batches without an fsync
	// per op, so the engines are not both pinned to F_FULLFSYNC latency the way write-saturated
	// is. There the LSM's sequential appends should ingest at least as fast as the B-tree's
	// page-split path (spec 06). At SyncFull write-saturated this difference collapses into fsync
	// noise, which is why bulk-load is the honest place to read it.
	if bt := rep.find("btree", "bulk-load"); bt != nil {
		if ls := rep.find("lsm", "bulk-load"); ls != nil {
			holds := ls.Throughput >= bt.Throughput
			out = append(out, Finding{
				Target: "Bulk ingest (LSM at or above B-tree, un-fsync-pinned)",
				Claim:  "with batches not pinned to per-op fsync, LSM ingest is at or above the B-tree's",
				Observed: fmt.Sprintf("lsm %.0f ops/s vs btree %.0f ops/s",
					ls.Throughput, bt.Throughput),
				Holds: holds,
			})
		}
	}

	// Honesty target: a throughput number with hidden dropped work is a lie (spec 21 §3). The
	// whole suite must complete every offered op, on every engine and workload.
	dropped := int64(0)
	for _, r := range rep.Results {
		dropped += r.Dropped
	}
	out = append(out, Finding{
		Target:   "No silent drops (spec 21 §3)",
		Claim:    "every offered operation is accounted for across the whole suite",
		Observed: fmt.Sprintf("%d dropped operations across %d cells", dropped, len(rep.Results)),
		Holds:    dropped == 0,
	})

	return out
}

// worstGC returns the worst stop-the-world pause over every measured window and the cell it
// landed in. This is a process-global figure: it folds in the benchmark driver's own per-op
// key and value allocations, not just the engine, so it bounds the whole process and cannot be
// attributed to the engine arena. The renderer discloses it as context, with that caveat, rather
// than scoring it as an engine target (spec 20's sub-ms arena claim is checked by the engine's
// own allocation tests, not by this process-global counter).
func worstGC(rep Report) (time.Duration, string) {
	var worst time.Duration
	cell := ""
	for _, r := range rep.Results {
		if r.GC.MaxPause > worst {
			worst = r.GC.MaxPause
			cell = r.Engine + "/" + r.Workload
		}
	}
	return worst, cell
}

// RenderReport renders a Report as a Markdown results document: the disclosed setup, a
// throughput-and-latency table per workload with both engines side by side, the RUM
// amplification triple per cell, and the tradeoff findings. It is the published artifact spec
// 24's M5 exit calls for, the form in which "high throughput, low latency" stops being a
// slogan and becomes a table a reader can audit (spec 21 §3).
func RenderReport(rep Report) string {
	var b strings.Builder

	b.WriteString("# kv benchmark results\n\n")
	if rep.Label != "" {
		fmt.Fprintf(&b, "%s\n\n", rep.Label)
	}
	b.WriteString("These numbers come from the in-repo harness (`bench/`) running every workload on both engines.\n")
	b.WriteString("They are produced on the developer machine disclosed below, not the NVMe reference machine the spec 21 §6 absolute targets are stated against, so the figures to read here are the *shape* of the B-tree/LSM tradeoff and the honesty targets, both of which are machine-independent in direction.\n")
	b.WriteString("Regenerate them with `go run ./cmd/bench`.\n\n")

	writeSetup(&b, rep)
	writeWorkloadTables(&b, rep)
	writeTradeoff(&b, rep)
	writeGCContext(&b, rep)

	return b.String()
}

// writeSetup prints the disclosure block from the first result; the suite holds sizing fixed
// across cells, so one block describes the whole run (spec 21 §3).
func writeSetup(b *strings.Builder, rep Report) {
	if len(rep.Results) == 0 {
		b.WriteString("_No results._\n")
		return
	}
	s := rep.Results[0].Setup
	b.WriteString("## Setup\n\n")
	fmt.Fprintf(b, "- Machine: %s/%s, %d CPU, %s\n", s.GOOS, s.GOARCH, s.NumCPU, s.GoVersion)
	fmt.Fprintf(b, "- Keys: %d at %dB key / %dB value; ops: %d; concurrency: %d; batch: %d\n",
		s.KeyCount, s.KeyLen, s.ValLen, rep.Results[0].Ops, s.Concurrency, s.BatchSize)
	fmt.Fprintf(b, "- Durability: %s; seed: %d\n", s.Synchronous, s.Seed)
	if s.CacheBytes > 0 {
		fmt.Fprintf(b, "- Cache cap: %d bytes\n", s.CacheBytes)
	}
	b.WriteString("\n")
}

// writeWorkloadTables prints one row per (workload, engine) with throughput, the read and
// write p99, and the RUM amplification triple, so a reader compares the two engines on each
// workload at a glance.
func writeWorkloadTables(b *strings.Builder, rep Report) {
	b.WriteString("## Per-workload numbers\n\n")
	b.WriteString("| workload | engine | throughput (ops/s) | read p99 | write p99 | space-amp | write-factor | read-ios/op |\n")
	b.WriteString("|---|---|---:|---:|---:|---:|---:|---:|\n")
	for _, r := range rep.Results {
		fmt.Fprintf(b, "| %s | %s | %.0f | %s | %s | %s | %s | %s |\n",
			r.Workload, r.Engine, r.Throughput,
			dur(r.Reads.P99), dur(r.Writes.P99),
			amp(r.Amplification.Space), amp(r.Amplification.Write), ampRead(r.Amplification.Read))
	}
	b.WriteString("\n")
}

// writeTradeoff prints the findings as a checklist with the measured evidence beside each, so
// the tradeoff is read off the numbers rather than taken on faith (spec 24 M5 exit).
func writeTradeoff(b *strings.Builder, rep Report) {
	b.WriteString("## Tradeoff and targets\n\n")
	for _, f := range Tradeoff(rep) {
		mark := "yes"
		if !f.Holds {
			mark = "no"
		}
		fmt.Fprintf(b, "- **%s** — %s\n  - Claim: %s\n  - Observed: %s\n  - Holds here: %s\n",
			f.Target, mark, f.Claim, f.Observed, mark)
	}
	b.WriteString("\n")
}

// writeGCContext discloses the worst GC pause as process-global context, not an engine score.
// The pause folds in the benchmark driver's own per-op allocations, so it is reported with that
// caveat: it bounds the whole process, and the engine's sub-ms arena claim (spec 20) is checked
// by the engine's own allocation tests rather than by this counter.
func writeGCContext(b *strings.Builder, rep Report) {
	worst, cell := worstGC(rep)
	b.WriteString("## Garbage collection (process-global context)\n\n")
	fmt.Fprintf(b, "Worst stop-the-world pause over every window: %s (%s).\n", dur(worst), cell)
	b.WriteString("This is a process-global figure. It includes the benchmark driver's own per-op key and value allocations, not just the engine, so it bounds the whole process rather than isolating the engine arena.\n")
	b.WriteString("The engine's sub-millisecond arena claim (spec 20) is checked by the engine's own allocation tests, not by this counter.\n\n")
}

// dur formats a duration compactly for a table cell, choosing the unit by magnitude so a
// microsecond read and a millisecond pause are both legible.
func dur(d time.Duration) string {
	switch {
	case d == 0:
		return "n/a"
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fus", float64(d.Nanoseconds())/1e3)
	default:
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	}
}

// amp formats an amplification ratio; a zero is shown as such rather than blank.
func amp(v float64) string { return fmt.Sprintf("%.2f", v) }

// ampRead formats read amplification, showing the not-measured sentinel as a dash so a
// write-only cell does not print a misleading -1.
func ampRead(v float64) string {
	if v == readNotMeasured {
		return "-"
	}
	return fmt.Sprintf("%.2f", v)
}
