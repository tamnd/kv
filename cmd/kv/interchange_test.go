package main

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestImportJSONL imports a JSONL file and verifies every key lands at the right value.
func TestImportJSONL(t *testing.T) {
	pairs := [][2]string{{"alpha", "1"}, {"beta", "2"}, {"gamma", "3"}}
	in := filepath.Join(t.TempDir(), "data.jsonl")
	jsonl := `{"key":"alpha","value":"1"}` + "\n" +
		`{"key":"beta","value":"2"}` + "\n" +
		`{"key":"gamma","value":"3"}` + "\n"
	if err := writeFile(in, jsonl); err != nil {
		t.Fatalf("write jsonl: %v", err)
	}

	dst := dbPath(t)
	if code := run([]string{"import", dst, "--format", "jsonl", "--input", in}); code != exitOK {
		t.Fatalf("import jsonl: exit %d", code)
	}

	for _, p := range pairs {
		got := strings.TrimSpace(capture(t, func() { run([]string{"get", dst, p[0]}) }))
		if got != p[1] {
			t.Fatalf("key %q: got %q, want %q", p[0], got, p[1])
		}
	}
}

// TestImportCSV imports a two-column CSV and verifies every key lands at the right value.
func TestImportCSV(t *testing.T) {
	pairs := [][2]string{{"a", "apple"}, {"b", "banana"}, {"c", "cherry"}}
	in := filepath.Join(t.TempDir(), "data.csv")
	if err := writeFile(in, "a,apple\nb,banana\nc,cherry\n"); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	dst := dbPath(t)
	if code := run([]string{"import", dst, "--format", "csv", "--input", in}); code != exitOK {
		t.Fatalf("import csv: exit %d", code)
	}

	for _, p := range pairs {
		got := strings.TrimSpace(capture(t, func() { run([]string{"get", dst, p[0]}) }))
		if got != p[1] {
			t.Fatalf("key %q: got %q, want %q", p[0], got, p[1])
		}
	}
}

// TestImportTSV imports a two-column TSV and verifies every key lands at the right value.
func TestImportTSV(t *testing.T) {
	pairs := [][2]string{{"x", "10"}, {"y", "20"}}
	in := filepath.Join(t.TempDir(), "data.tsv")
	if err := writeFile(in, "x\t10\ny\t20\n"); err != nil {
		t.Fatalf("write tsv: %v", err)
	}

	dst := dbPath(t)
	if code := run([]string{"import", dst, "--format", "tsv", "--input", in}); code != exitOK {
		t.Fatalf("import tsv: exit %d", code)
	}

	for _, p := range pairs {
		got := strings.TrimSpace(capture(t, func() { run([]string{"get", dst, p[0]}) }))
		if got != p[1] {
			t.Fatalf("key %q: got %q, want %q", p[0], got, p[1])
		}
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

// TestImportBase64 checks that binary keys encoded as base64 survive an import.
func TestImportBase64(t *testing.T) {
	rawKey := []byte{0x00, 0x01, 0x02, 0x03}
	rawVal := []byte("bin-val")
	bk := base64.StdEncoding.EncodeToString(rawKey)
	bv := base64.StdEncoding.EncodeToString(rawVal)

	// Build the JSONL input the way a base64 export would: both fields base64-encoded.
	importFile := filepath.Join(t.TempDir(), "data.jsonl")
	line := `{"key":"` + bk + `","value":"` + bv + `"}` + "\n"
	if err := writeFile(importFile, line); err != nil {
		t.Fatalf("write jsonl: %v", err)
	}

	// Import with base64 decoding into a fresh DB.
	dst := dbPath(t)
	if code := run([]string{"import", dst, "--format", "jsonl", "--base64", "--input", importFile}); code != exitOK {
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
