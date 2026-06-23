package main

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// seedDB writes a small set of key/value pairs into a fresh database via the CLI.
func seedDB(t *testing.T, p string, pairs [][2]string) {
	t.Helper()
	for _, kv := range pairs {
		if code := run([]string{"set", p, kv[0], kv[1]}); code != exitOK {
			t.Fatalf("seed set %q: exit %d", kv[0], code)
		}
	}
}

// TestExportJSONLRoundTrip exports a database to JSONL and re-imports it into a
// second database, then verifies every key lands at the right value.
func TestExportJSONLRoundTrip(t *testing.T) {
	src := dbPath(t)
	pairs := [][2]string{{"alpha", "1"}, {"beta", "2"}, {"gamma", "3"}}
	seedDB(t, src, pairs)

	out := filepath.Join(t.TempDir(), "export.jsonl")
	if code := run([]string{"export", src, "--format", "jsonl", "--output", out}); code != exitOK {
		t.Fatalf("export jsonl: exit %d", code)
	}

	dst := filepath.Join(t.TempDir(), "dst.kv")
	if code := run([]string{"create", dst}); code != exitOK {
		t.Fatalf("create dst: exit %d", code)
	}
	if code := run([]string{"import", dst, "--format", "jsonl", "--input", out}); code != exitOK {
		t.Fatalf("import jsonl: exit %d", code)
	}

	for _, p := range pairs {
		got := strings.TrimSpace(capture(t, func() { run([]string{"get", dst, p[0]}) }))
		if got != p[1] {
			t.Fatalf("key %q: got %q, want %q", p[0], got, p[1])
		}
	}
}

// TestExportCSVRoundTrip exports a database as CSV and re-imports it.
func TestExportCSVRoundTrip(t *testing.T) {
	src := dbPath(t)
	pairs := [][2]string{{"a", "apple"}, {"b", "banana"}, {"c", "cherry"}}
	seedDB(t, src, pairs)

	out := filepath.Join(t.TempDir(), "export.csv")
	if code := run([]string{"export", src, "--format", "csv", "--output", out}); code != exitOK {
		t.Fatalf("export csv: exit %d", code)
	}

	dst := filepath.Join(t.TempDir(), "dst.kv")
	if code := run([]string{"create", dst}); code != exitOK {
		t.Fatalf("create dst: exit %d", code)
	}
	if code := run([]string{"import", dst, "--format", "csv", "--input", out}); code != exitOK {
		t.Fatalf("import csv: exit %d", code)
	}

	for _, p := range pairs {
		got := strings.TrimSpace(capture(t, func() { run([]string{"get", dst, p[0]}) }))
		if got != p[1] {
			t.Fatalf("key %q: got %q, want %q", p[0], got, p[1])
		}
	}
}

// TestExportTSVRoundTrip exports a database as TSV and re-imports it.
func TestExportTSVRoundTrip(t *testing.T) {
	src := dbPath(t)
	pairs := [][2]string{{"x", "10"}, {"y", "20"}}
	seedDB(t, src, pairs)

	out := filepath.Join(t.TempDir(), "export.tsv")
	if code := run([]string{"export", src, "--format", "tsv", "--output", out}); code != exitOK {
		t.Fatalf("export tsv: exit %d", code)
	}

	dst := filepath.Join(t.TempDir(), "dst.kv")
	if code := run([]string{"create", dst}); code != exitOK {
		t.Fatalf("create dst: exit %d", code)
	}
	if code := run([]string{"import", dst, "--format", "tsv", "--input", out}); code != exitOK {
		t.Fatalf("import tsv: exit %d", code)
	}

	for _, p := range pairs {
		got := strings.TrimSpace(capture(t, func() { run([]string{"get", dst, p[0]}) }))
		if got != p[1] {
			t.Fatalf("key %q: got %q, want %q", p[0], got, p[1])
		}
	}
}

// TestExportPrefix exports only keys with a given prefix.
func TestExportPrefix(t *testing.T) {
	src := dbPath(t)
	seedDB(t, src, [][2]string{{"foo/a", "1"}, {"foo/b", "2"}, {"bar/c", "3"}})

	out := capture(t, func() {
		if code := run([]string{"export", src, "--format", "jsonl", "--prefix", "foo/"}); code != exitOK {
			t.Fatalf("export with prefix: exit %d", code)
		}
	})

	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d:\n%s", len(lines), out)
	}
	for _, l := range lines {
		if !strings.Contains(l, "foo/") {
			t.Fatalf("unexpected line (no foo/ prefix): %s", l)
		}
	}
}

// TestExportUsageErrors covers format mismatches and missing db arguments.
func TestExportUsageErrors(t *testing.T) {
	p := dbPath(t)
	cases := []struct {
		name string
		args []string
	}{
		{"no-args", []string{"export"}},
		{"bad-format", []string{"export", p, "--format", "xml"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if code := run(tc.args); code != exitUsage {
				t.Fatalf("expected exitUsage, got %d", code)
			}
		})
	}
}

// TestImportUsageErrors covers bad format, negative column indices, and bad input.
func TestImportUsageErrors(t *testing.T) {
	p := dbPath(t)
	cases := []struct {
		name string
		args []string
	}{
		{"no-args", []string{"import"}},
		{"bad-format", []string{"import", p, "--format", "xml"}},
		{"bad-batch", []string{"import", p, "--batch", "0"}},
		{"neg-key-col", []string{"import", p, "--key-col", "-1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if code := run(tc.args); code != exitUsage {
				t.Fatalf("expected exitUsage, got %d", code)
			}
		})
	}
}

// TestImportBase64 checks that binary keys encoded as base64 survive the round-trip.
func TestImportBase64(t *testing.T) {
	src := dbPath(t)
	rawKey := []byte{0x00, 0x01, 0x02, 0x03}
	rawVal := []byte("bin-val")
	bk := base64.StdEncoding.EncodeToString(rawKey)
	bv := base64.StdEncoding.EncodeToString(rawVal)

	// Export as JSONL with base64 encoding.
	seedDB(t, src, [][2]string{{string(rawKey), string(rawVal)}})
	exported := capture(t, func() {
		if code := run([]string{"export", src, "--format", "jsonl", "--base64"}); code != exitOK {
			t.Fatalf("export base64: exit %d", code)
		}
	})

	// Exported line must contain base64-encoded key and value.
	if !strings.Contains(exported, bk) {
		t.Fatalf("exported line %q does not contain base64 key %q", exported, bk)
	}
	if !strings.Contains(exported, bv) {
		t.Fatalf("exported line %q does not contain base64 value %q", exported, bv)
	}

	// Re-import with base64 decoding into a new DB.
	dst := filepath.Join(t.TempDir(), "dst.kv")
	if code := run([]string{"create", dst}); code != exitOK {
		t.Fatalf("create dst: exit %d", code)
	}
	exportFile := filepath.Join(t.TempDir(), "data.jsonl")
	if code := run([]string{"export", src, "--format", "jsonl", "--base64", "--output", exportFile}); code != exitOK {
		t.Fatalf("export to file: exit %d", code)
	}
	if code := run([]string{"import", dst, "--format", "jsonl", "--base64", "--input", exportFile}); code != exitOK {
		t.Fatalf("import base64: exit %d", code)
	}

	// get --base64 decodes the key arg and encodes the returned value as base64.
	got := strings.TrimSpace(capture(t, func() {
		run([]string{"get", dst, "--base64", bk})
	}))
	wantB64Val := base64.StdEncoding.EncodeToString(rawVal)
	if got != wantB64Val {
		t.Fatalf("base64 import: got %q, want %q", got, wantB64Val)
	}
}

// TestImportCSVCustomColumns imports a CSV with key in column 1 and value in column 2.
func TestImportCSVCustomColumns(t *testing.T) {
	// Write a CSV with three columns: unused, key, value.
	csvData := "ignore,k1,v1\nignore,k2,v2\n"
	csvFile := filepath.Join(t.TempDir(), "data.csv")
	if err := writeFile(csvFile, csvData); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	dst := dbPath(t)
	if code := run([]string{"import", dst, "--format", "csv", "--input", csvFile, "--key-col", "1", "--val-col", "2"}); code != exitOK {
		t.Fatalf("import csv custom cols: exit %d", code)
	}

	for _, kv := range [][2]string{{"k1", "v1"}, {"k2", "v2"}} {
		got := strings.TrimSpace(capture(t, func() { run([]string{"get", dst, kv[0]}) }))
		if got != kv[1] {
			t.Fatalf("key %q: got %q, want %q", kv[0], got, kv[1])
		}
	}
}

// TestImportCSVBadColumn confirms an out-of-range column reports exitUsage.
func TestImportCSVBadColumn(t *testing.T) {
	csvFile := filepath.Join(t.TempDir(), "data.csv")
	if err := writeFile(csvFile, "only-one-col\n"); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	p := dbPath(t)
	code := run([]string{"import", p, "--format", "csv", "--input", csvFile, "--key-col", "0", "--val-col", "5"})
	if code != exitUsage {
		t.Fatalf("bad column: exit %d, want exitUsage (%d)", code, exitUsage)
	}
}

// writeFile is a test helper that creates a file with the given content.
func writeFile(path, content string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(content)
	return err
}
