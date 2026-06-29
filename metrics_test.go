package kv_test

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"github.com/tamnd/kv"
)

// TestWriteMetricsRendersFromStats checks the pure renderer turns a Stats snapshot into
// well-formed Prometheus exposition: the engine label, the derived live-pages and hit-ratio,
// and the right HELP/TYPE pairing for a counter versus a gauge.
func TestWriteMetricsRendersFromStats(t *testing.T) {
	s := kv.Stats{
		Engine:        kv.LSM,
		PageSize:      4096,
		PageCount:     100,
		FreePages:     10,
		PhysicalBytes: 409600,
		LiveKeys:      42,
		LiveBytes:     1234,
		Amplification: 1.5,
		Version:       7,
		WALFrames:     20,
		WALBacklog:    3,
		Syncs:         9,
		PageReads:     25,
		CacheHits:     75,
		Gets:          400,
		Sets:          120,
		Deletes:       8,
		Merges:        5,
		Scans:         3,
		Commits:       50,
		CommitNanos:   2_000_000_000, // 2s over 50 commits -> 40ms average
	}
	var buf bytes.Buffer
	if err := kv.WriteMetrics(&buf, s); err != nil {
		t.Fatalf("WriteMetrics: %v", err)
	}
	out := buf.String()

	for _, want := range []string{
		`kv_engine_info{engine="lsm"} 1`,
		"# TYPE kv_fsync_total counter",
		"kv_fsync_total 9",
		"# TYPE kv_page_count gauge",
		"kv_page_count 100",
		"kv_live_pages 90",        // page count minus freelist
		"kv_cache_hit_ratio 0.75", // 75 hits over 100 accesses
		"kv_wal_checkpoint_backlog_frames 3",
		"kv_live_keys 42",
		"# HELP kv_space_amplification ",
		"# TYPE kv_ops_total counter",
		`kv_ops_total{op="get"} 400`,
		`kv_ops_total{op="set"} 120`,
		`kv_ops_total{op="delete"} 8`,
		`kv_ops_total{op="merge"} 5`,
		`kv_ops_total{op="scan"} 3`,
		"# TYPE kv_commit_latency_seconds summary",
		"kv_commit_latency_seconds_sum 2", // 2e9 ns rendered as 2 seconds
		"kv_commit_latency_seconds_count 50",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics output missing %q\n--- got ---\n%s", want, out)
		}
	}
}

// TestWriteMetricsRendersLSMInternals checks the per-level segment and byte gauges and the
// compaction score render with one labeled sample per level, and that the reader-age gauge
// turns nanoseconds into seconds.
func TestWriteMetricsRendersLSMInternals(t *testing.T) {
	s := kv.Stats{
		Engine: kv.LSM,
		Levels: []kv.LevelStats{
			{Segments: 4, Bytes: 8192},
			{Segments: 2, Bytes: 65536},
		},
		CompactionScore:        1.75,
		OldestSnapshotAgeNanos: 3_000_000_000, // 3s
	}
	var buf bytes.Buffer
	if err := kv.WriteMetrics(&buf, s); err != nil {
		t.Fatalf("WriteMetrics: %v", err)
	}
	out := buf.String()
	for _, want := range []string{
		"# TYPE kv_lsm_segments gauge",
		`kv_lsm_segments{level="0"} 4`,
		`kv_lsm_segments{level="1"} 2`,
		`kv_lsm_level_bytes{level="0"} 8192`,
		`kv_lsm_level_bytes{level="1"} 65536`,
		"kv_lsm_compaction_score 1.75",
		"kv_oldest_snapshot_age_seconds 3",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics output missing %q\n--- got ---\n%s", want, out)
		}
	}
}

// TestWriteMetricsOmitsLSMInternalsForBTree checks a B-tree file, which has no level
// structure, does not emit the per-level families at all rather than asserting a flat zero
// for a shape it does not have. The reader-age gauge still renders, since it is engine-agnostic.
func TestWriteMetricsOmitsLSMInternalsForBTree(t *testing.T) {
	var buf bytes.Buffer
	if err := kv.WriteMetrics(&buf, kv.Stats{Engine: kv.BTree}); err != nil {
		t.Fatalf("WriteMetrics: %v", err)
	}
	out := buf.String()
	for _, unwanted := range []string{"kv_lsm_segments", "kv_lsm_level_bytes", "kv_lsm_compaction_score"} {
		if strings.Contains(out, unwanted) {
			t.Errorf("%s should be omitted for a B-tree file, got:\n%s", unwanted, out)
		}
	}
	if !strings.Contains(out, "kv_oldest_snapshot_age_seconds 0") {
		t.Errorf("reader-age gauge should render for a B-tree file, got:\n%s", out)
	}
}

// TestWriteMetricsHitRatioGuard confirms an idle database (no reads, no hits) reports a flat
// zero hit ratio rather than a NaN that would poison a dashboard.
func TestWriteMetricsHitRatioGuard(t *testing.T) {
	var buf bytes.Buffer
	if err := kv.WriteMetrics(&buf, kv.Stats{Engine: kv.BTree}); err != nil {
		t.Fatalf("WriteMetrics: %v", err)
	}
	if !strings.Contains(buf.String(), "kv_cache_hit_ratio 0\n") {
		t.Errorf("idle database should report kv_cache_hit_ratio 0, got:\n%s", buf.String())
	}
}

// TestWriteMetricsOmitsEmptyLiveCounts checks the live-key and live-byte gauges are dropped
// when the engine does not compute them, so the surface does not assert a misleading zero.
func TestWriteMetricsOmitsEmptyLiveCounts(t *testing.T) {
	var buf bytes.Buffer
	if err := kv.WriteMetrics(&buf, kv.Stats{Engine: kv.BTree}); err != nil {
		t.Fatalf("WriteMetrics: %v", err)
	}
	if strings.Contains(buf.String(), "kv_live_keys") {
		t.Error("kv_live_keys should be omitted when LiveKeys is zero")
	}
}

// TestDBWriteMetricsIsWellFormed opens a real database, drives some traffic, and checks the
// rendered metrics parse as exposition: every sample line is "name value", the commit version
// advanced past zero, and the engine label names the engine the file was opened with.
func TestDBWriteMetricsIsWellFormed(t *testing.T) {
	d := open(t)
	for i := range 50 {
		k := []byte{byte(i)}
		if err := d.Update(func(txn *kv.Txn) error { return txn.Set(k, []byte("v")) }); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	if err := d.View(func(txn *kv.Txn) error {
		_, err := txn.Get([]byte{1})
		return err
	}); err != nil {
		t.Fatalf("get: %v", err)
	}

	var buf bytes.Buffer
	if err := d.WriteMetrics(&buf); err != nil {
		t.Fatalf("WriteMetrics: %v", err)
	}
	out := buf.String()

	sc := bufio.NewScanner(strings.NewReader(out))
	sawVersion := false
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// A sample line is "name value" or "name{labels} value": exactly two space-separated
		// fields once the optional label set is treated as part of the name.
		if fields := strings.Fields(line); len(fields) != 2 {
			t.Errorf("malformed sample line %q (want name and value)", line)
		}
		if strings.HasPrefix(line, "kv_commit_version ") && !strings.HasSuffix(line, " 0") {
			sawVersion = true
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !sawVersion {
		t.Error("commit version should have advanced past zero after 50 writes")
	}
	if !strings.Contains(buf.String(), `kv_engine_info{engine="btree"} 1`) {
		t.Error("metrics should carry the btree engine label for a default database")
	}
}

// TestDBMetricsCountOperations drives a known mix of reads and writes, then checks the
// per-op counters and the commit-latency pair reflect the work: 50 sets across 50 commits,
// one get, and a positive total commit latency.
func TestDBMetricsCountOperations(t *testing.T) {
	d := open(t)
	for i := range 50 {
		k := []byte{byte(i)}
		if err := d.Update(func(txn *kv.Txn) error { return txn.Set(k, []byte("v")) }); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	if err := d.View(func(txn *kv.Txn) error {
		if _, err := txn.Get([]byte{1}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("read txn: %v", err)
	}

	s := d.Stats()
	if s.Sets != 50 {
		t.Errorf("Sets = %d, want 50", s.Sets)
	}
	if s.Gets != 1 {
		t.Errorf("Gets = %d, want 1", s.Gets)
	}
	if s.Commits != 50 {
		t.Errorf("Commits = %d, want 50", s.Commits)
	}
	if s.CommitNanos == 0 {
		t.Error("CommitNanos should be positive after 50 durable commits")
	}
}
