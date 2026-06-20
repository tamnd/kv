package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// capture runs fn with os.Stdout redirected to a pipe and returns what it wrote, so the
// command functions can be exercised exactly as main() invokes them.
func capture(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, r)
		done <- buf.String()
	}()
	fn()
	w.Close()
	os.Stdout = orig
	return <-done
}

// dbPath returns a fresh, created database path in a temp dir.
func dbPath(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "test.kv")
	if code := run([]string{"create", p}); code != exitOK {
		t.Fatalf("create: exit %d", code)
	}
	return p
}

func TestCLISetGet(t *testing.T) {
	p := dbPath(t)
	if code := run([]string{"set", p, "hello", "world"}); code != exitOK {
		t.Fatalf("set: exit %d", code)
	}
	out := capture(t, func() {
		if code := run([]string{"get", p, "hello"}); code != exitOK {
			t.Fatalf("get: exit %d", code)
		}
	})
	if strings.TrimSpace(out) != "world" {
		t.Fatalf("get = %q, want world", out)
	}
}

func TestCLIGetMissingExit1(t *testing.T) {
	p := dbPath(t)
	if code := run([]string{"get", p, "absent"}); code != exitNotFound {
		t.Fatalf("get absent = exit %d, want %d", code, exitNotFound)
	}
}

func TestCLIExists(t *testing.T) {
	p := dbPath(t)
	run([]string{"set", p, "k", "v"})
	if code := run([]string{"exists", p, "k"}); code != exitOK {
		t.Fatalf("exists present = exit %d, want 0", code)
	}
	if code := run([]string{"exists", p, "nope"}); code != exitNotFound {
		t.Fatalf("exists absent = exit %d, want 1", code)
	}
}

func TestCLIDelAndDelRange(t *testing.T) {
	p := dbPath(t)
	for _, k := range []string{"a", "b", "c", "d"} {
		run([]string{"set", p, k, k})
	}
	if code := run([]string{"del", p, "a"}); code != exitOK {
		t.Fatalf("del: exit %d", code)
	}
	// del-range [b, d) removes b and c, leaves d.
	if code := run([]string{"del-range", p, "b", "d"}); code != exitOK {
		t.Fatalf("del-range: exit %d", code)
	}
	out := capture(t, func() { run([]string{"count", p}) })
	if strings.TrimSpace(out) != "1" {
		t.Fatalf("count after deletes = %q, want 1", out)
	}
}

func TestCLIScanPrefixJSONL(t *testing.T) {
	p := dbPath(t)
	run([]string{"set", p, "user:1", "alice"})
	run([]string{"set", p, "user:2", "bob"})
	run([]string{"set", p, "other", "x"})
	out := capture(t, func() {
		run([]string{"scan", p, "--prefix", "user:", "-f", "jsonl"})
	})
	var keys []string
	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		var r record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("bad jsonl line %q: %v", sc.Text(), err)
		}
		keys = append(keys, r.Key)
	}
	if len(keys) != 2 || keys[0] != "user:1" || keys[1] != "user:2" {
		t.Fatalf("scanned keys = %v, want [user:1 user:2]", keys)
	}
}

func TestCLIScanReverseKeysOnly(t *testing.T) {
	p := dbPath(t)
	for _, k := range []string{"a", "b", "c"} {
		run([]string{"set", p, k, k})
	}
	out := capture(t, func() {
		run([]string{"scan", p, "--reverse", "--keys-only", "-f", "raw"})
	})
	got := strings.Fields(out)
	want := []string{"c", "b", "a"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("reverse keys = %v, want %v", got, want)
	}
}

func TestCLIDumpLoadRoundTrip(t *testing.T) {
	src := dbPath(t)
	run([]string{"set", src, "k1", "v1"})
	run([]string{"set", src, "k2", "v2"})
	dump := capture(t, func() { run([]string{"dump", src}) })

	dst := filepath.Join(t.TempDir(), "dst.kv")
	if code := run([]string{"create", dst}); code != exitOK {
		t.Fatalf("create dst: exit %d", code)
	}
	// Feed the dump back through load via a redirected stdin.
	withStdin(t, dump, func() {
		if code := run([]string{"load", dst}); code != exitOK {
			t.Fatalf("load: exit %d", code)
		}
	})
	out := capture(t, func() { run([]string{"scan", dst, "-f", "jsonl"}) })
	var got []string
	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		var r record
		json.Unmarshal(sc.Bytes(), &r)
		got = append(got, r.Key+"="+r.Value)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != "k1=v1,k2=v2" {
		t.Fatalf("loaded = %v, want [k1=v1 k2=v2]", got)
	}
}

func TestCLIBinaryBase64(t *testing.T) {
	p := dbPath(t)
	// key = bytes 00 01 02, value = byte ff
	if code := run([]string{"set", p, "--base64", "AAEC", "/w=="}); code != exitOK {
		t.Fatalf("set base64: exit %d", code)
	}
	out := capture(t, func() {
		run([]string{"get", p, "--base64", "AAEC"})
	})
	if strings.TrimSpace(out) != "/w==" {
		t.Fatalf("get base64 = %q, want /w==", out)
	}
}

func TestCLICreateRefusesExisting(t *testing.T) {
	p := dbPath(t)
	if code := run([]string{"create", p}); code != exitUsage {
		t.Fatalf("create existing = exit %d, want %d", code, exitUsage)
	}
}

func TestCLIUnknownCommand(t *testing.T) {
	if code := run([]string{"frobnicate"}); code != exitUsage {
		t.Fatalf("unknown command = exit %d, want %d", code, exitUsage)
	}
}

func TestCLIOpenMissingFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope.kv")
	if code := run([]string{"get", missing, "k"}); code != exitOpen {
		t.Fatalf("get on missing file = exit %d, want %d", code, exitOpen)
	}
}

func TestCLIInfo(t *testing.T) {
	p := dbPath(t)
	run([]string{"set", p, "k", "v"})
	out := capture(t, func() {
		if code := run([]string{"info", p}); code != exitOK {
			t.Fatalf("info: exit %d", code)
		}
	})
	if !strings.Contains(out, "engine") || !strings.Contains(out, "btree") {
		t.Fatalf("info output missing engine line:\n%s", out)
	}
	if !strings.Contains(out, "commit version") {
		t.Fatalf("info output missing version line:\n%s", out)
	}
}

func TestCLIStatsJSON(t *testing.T) {
	p := dbPath(t)
	run([]string{"set", p, "k", "v"})
	out := capture(t, func() {
		if code := run([]string{"stats", p, "-f", "jsonl"}); code != exitOK {
			t.Fatalf("stats: exit %d", code)
		}
	})
	var s statsJSON
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &s); err != nil {
		t.Fatalf("stats output not JSON: %v\n%s", err, out)
	}
	if s.Engine != "btree" {
		t.Fatalf("stats engine = %q, want btree", s.Engine)
	}
	if s.Version != 1 {
		t.Fatalf("stats version = %d, want 1", s.Version)
	}
}

// TestCLICheckSound writes a few keys, checkpoints so the data is folded into the main
// file, then runs check and expects exit 0 with an "ok" result.
func TestCLICheckSound(t *testing.T) {
	p := dbPath(t)
	for _, k := range []string{"a", "b", "c"} {
		if code := run([]string{"set", p, k, "v"}); code != exitOK {
			t.Fatalf("set %s: exit %d", k, code)
		}
	}
	if code := run([]string{"checkpoint", p}); code != exitOK {
		t.Fatalf("checkpoint: exit %d", code)
	}
	out := capture(t, func() {
		if code := run([]string{"check", p}); code != exitOK {
			t.Fatalf("check: exit %d, want 0", code)
		}
	})
	if !strings.Contains(out, "ok") {
		t.Fatalf("check output = %q, want it to contain ok", out)
	}
}

// TestCLICheckJSON confirms the machine-readable form reports ok=true for a sound file.
func TestCLICheckJSON(t *testing.T) {
	p := dbPath(t)
	if code := run([]string{"set", p, "k", "v"}); code != exitOK {
		t.Fatalf("set: exit %d", code)
	}
	out := capture(t, func() {
		if code := run([]string{"check", p, "-f", "json"}); code != exitOK {
			t.Fatalf("check -f json: exit %d", code)
		}
	})
	var got struct {
		OK        bool `json:"ok"`
		PageCount int  `json:"page_count"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if !got.OK {
		t.Fatalf("check json ok = false, want true: %q", out)
	}
	if got.PageCount == 0 {
		t.Fatalf("check json page_count = 0, want positive")
	}
}

// TestCLIVacuum confirms the vacuum command runs cleanly over a populated file, prints a
// freed-pages summary, and leaves the file sound. The tree core does not yet return pages
// to the freelist, so the run reclaims zero pages today; the command still must succeed
// and report its result (spec 09 §3.1, spec 16).
func TestCLIVacuum(t *testing.T) {
	p := dbPath(t)
	for _, k := range []string{"a", "b", "c", "d"} {
		if code := run([]string{"set", p, k, "v"}); code != exitOK {
			t.Fatalf("set %s: exit %d", k, code)
		}
	}
	out := capture(t, func() {
		if code := run([]string{"vacuum", p, "-n", "16"}); code != exitOK {
			t.Fatalf("vacuum: exit %d, want 0", code)
		}
	})
	if !strings.Contains(out, "freed") || !strings.Contains(out, "remain") {
		t.Fatalf("vacuum output = %q, want a freed/remain summary", out)
	}
	if code := run([]string{"check", p}); code != exitOK {
		t.Fatalf("check after vacuum: exit %d, want 0", code)
	}
}

// withStdin runs fn with os.Stdin replaced by a pipe carrying the given input.
func withStdin(t *testing.T, input string, fn func()) {
	t.Helper()
	orig := os.Stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdin = r
	go func() {
		io.WriteString(w, input)
		w.Close()
	}()
	fn()
	os.Stdin = orig
}
