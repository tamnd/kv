package kv

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// This file renders a Stats snapshot as Prometheus text exposition, the first observability
// surface above the engine seam (spec 19 §1). It is a pure function of a Stats value so it is
// unit-testable without an open database and so the embedded path and the future served
// /metrics endpoint (spec 17, spec 19 §6) render the exact same numbers from the same code.
//
// The metric set here is the subset spec 19 names that today's Stats already measures honestly:
// space composition (§1.3), WAL and durability (§1.2), and cache traffic (§1.4). Per-op
// histograms, engine internals, and reader-age counters arrive in later slices as their
// counters land; this slice establishes the surface and the format.

// metricKind labels a metric as a monotonic counter or a point-in-time gauge, the two
// Prometheus types this surface uses.
type metricKind string

const (
	counter metricKind = "counter"
	gauge   metricKind = "gauge"
	summary metricKind = "summary"
)

// WriteMetrics renders a Stats snapshot as Prometheus text exposition to w. Every metric
// carries its HELP and TYPE lines, so a scrape is self-describing, and the engine is stamped
// on an info metric so a dashboard can tell a B-tree file from an LSM file. It is the shared
// renderer behind both DB.WriteMetrics and the served endpoint.
func WriteMetrics(w io.Writer, s Stats) error {
	bw := bufio.NewWriter(w)
	m := &metricWriter{w: bw}

	// Identity: a constant-1 info metric with the engine as a label, the idiom for exposing a
	// dimension that is not itself a number.
	m.help("kv_engine_info", "Storage engine the file was created with, as a label; value is always 1.")
	m.typ("kv_engine_info", gauge)
	m.line("kv_engine_info", `engine="`+s.Engine.String()+`"`, 1)

	// Space composition (spec 19 §1.3): what the file is made of and how much of it is live.
	m.gaugef("kv_page_size_bytes", "Page size of the file in bytes.", float64(s.PageSize))
	m.gaugef("kv_page_count", "File size in pages (high-water mark).", float64(s.PageCount))
	m.gaugef("kv_freelist_pages", "Freelist depth: pages reusable before the file grows.", float64(s.FreePages))
	m.gaugef("kv_live_pages", "Pages in use: page count minus the freelist.", float64(int64(s.PageCount)-s.FreePages))
	m.gaugef("kv_physical_bytes", "On-disk footprint in bytes, dead versions included.", float64(s.PhysicalBytes))
	if s.LiveKeys > 0 || s.LiveBytes > 0 {
		m.gaugef("kv_live_keys", "Live keys at the newest snapshot.", float64(s.LiveKeys))
		m.gaugef("kv_live_bytes", "Live value bytes at the newest snapshot.", float64(s.LiveBytes))
	}
	m.gaugef("kv_space_amplification", "Physical bytes over live bytes; the LSM's key space-health number.", s.Amplification)

	// Durability and WAL (spec 19 §1.2): commit progress, log growth, the checkpoint backlog,
	// and the fsync count. A rising backlog means checkpointing is losing to writes.
	m.gaugef("kv_commit_version", "Latest committed commit version.", float64(s.Version))
	m.gaugef("kv_wal_frames", "Frames the WAL has written since open.", float64(s.WALFrames))
	m.gaugef("kv_wal_checkpoint_backlog_frames", "Frames committed but not yet folded by a checkpoint.", float64(s.WALBacklog))
	m.counterf("kv_fsync_total", "fsyncs the WAL has performed since open.", float64(s.Syncs))

	// Cache traffic (spec 19 §1.4): the dominant latency lever. The hit ratio is derived from
	// the two cumulative counters, with the denominator guarded so an idle database reads 0
	// rather than NaN.
	m.counterf("kv_page_reads_total", "Physical page reads against the main file since open.", float64(s.PageReads))
	m.counterf("kv_cache_hits_total", "Gets served from a resident buffer-pool frame since open.", float64(s.CacheHits))
	m.gaugef("kv_cache_hit_ratio", "Cache hits over total page accesses since open; 0 when idle.", hitRatio(s))

	// Throughput (spec 19 §1.1): one counter name with an op label, the idiom for a family
	// of related totals, so a dashboard rates kv_ops_total by op without a metric per verb.
	m.help("kv_ops_total", "Logical operations served since open, by operation.")
	m.typ("kv_ops_total", counter)
	m.line("kv_ops_total", `op="get"`, float64(s.Gets))
	m.line("kv_ops_total", `op="set"`, float64(s.Sets))
	m.line("kv_ops_total", `op="delete"`, float64(s.Deletes))
	m.line("kv_ops_total", `op="merge"`, float64(s.Merges))
	m.line("kv_ops_total", `op="scan"`, float64(s.Scans))

	// Commit latency (spec 19 §1.1) as a summary's sum-and-count pair: total seconds of
	// durable-commit time over the count of durable commits. A scraper rates the sum over the
	// count to get the average fsync cost without the database keeping a histogram. The sum is
	// nanoseconds converted to seconds, the Prometheus base unit.
	m.help("kv_commit_latency_seconds", "Durable-commit latency, as a summary sum and count; sum/count is the average.")
	m.typ("kv_commit_latency_seconds", summary)
	m.line("kv_commit_latency_seconds_sum", "", float64(s.CommitNanos)/1e9)
	m.line("kv_commit_latency_seconds_count", "", float64(s.Commits))

	// Compaction backlog (spec 19 §1.5): the urgency of the most-pending compaction the
	// f2 core self-schedules, 1.0 at-trigger and 0 when idle.
	m.gaugef("kv_compaction_score", "Urgency of the most-pending compaction; 1.0 is at-trigger, 0 when idle.", s.CompactionScore)

	// Reader age (spec 19 §1.6): the wall-clock age of the longest-held live read
	// snapshot, in seconds. It reads 0 when no reader is live; a value that only climbs is
	// a snapshot or iterator that was never closed, pinning the GC watermark.
	m.gaugef("kv_oldest_snapshot_age_seconds", "Age of the longest-held live read snapshot in seconds; 0 when none is live.", float64(s.OldestSnapshotAgeNanos)/1e9)

	if err := m.err; err != nil {
		return wrap(err)
	}
	return wrap(bw.Flush())
}

// hitRatio is the buffer pool's hit rate over the database's lifetime: hits over hits plus
// physical reads. The denominator is guarded so a database that has served no reads reports a
// flat 0 rather than a divide-by-zero NaN that would poison a dashboard.
func hitRatio(s Stats) float64 {
	total := s.CacheHits + s.PageReads
	if total == 0 {
		return 0
	}
	return float64(s.CacheHits) / float64(total)
}

// WriteMetrics renders the database's current Stats as Prometheus text exposition (spec 19
// §1). It is cheap and lock-light, safe to wire into a scrape handler that polls.
func (kdb *DB) WriteMetrics(w io.Writer) error {
	return WriteMetrics(w, kdb.Stats())
}

// metricWriter accumulates exposition lines and the first write error, so the caller checks
// the error once at the end rather than after every line.
type metricWriter struct {
	w   *bufio.Writer
	err error
}

// help emits a HELP line for a metric.
func (m *metricWriter) help(name, text string) {
	m.writef("# HELP %s %s\n", name, text)
}

// typ emits a TYPE line for a metric.
func (m *metricWriter) typ(name string, k metricKind) {
	m.writef("# TYPE %s %s\n", name, k)
}

// line emits one sample. labels is the already-formatted label set without braces, or empty
// for an unlabeled metric.
func (m *metricWriter) line(name, labels string, v float64) {
	val := strconv.FormatFloat(v, 'g', -1, 64)
	if labels == "" {
		m.writef("%s %s\n", name, val)
		return
	}
	m.writef("%s{%s} %s\n", name, labels, val)
}

// gaugef emits a complete unlabeled gauge: HELP, TYPE, and the sample.
func (m *metricWriter) gaugef(name, help string, v float64) {
	m.help(name, help)
	m.typ(name, gauge)
	m.line(name, "", v)
}

// counterf emits a complete unlabeled counter: HELP, TYPE, and the sample.
func (m *metricWriter) counterf(name, help string, v float64) {
	m.help(name, help)
	m.typ(name, counter)
	m.line(name, "", v)
}

// writef writes a formatted line unless a previous write already failed, latching the first
// error.
func (m *metricWriter) writef(format string, args ...any) {
	if m.err != nil {
		return
	}
	_, m.err = fmt.Fprintf(m.w, format, args...)
}
