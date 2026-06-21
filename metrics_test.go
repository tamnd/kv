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
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics output missing %q\n--- got ---\n%s", want, out)
		}
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
