package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
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

func TestCLIMetrics(t *testing.T) {
	p := dbPath(t)
	run([]string{"set", p, "hello", "world"})
	out := capture(t, func() {
		if code := run([]string{"metrics", p}); code != exitOK {
			t.Fatalf("metrics: exit %d", code)
		}
	})
	// The output must be valid Prometheus exposition: the engine label, a HELP/TYPE pair, and a
	// counter line are enough to prove the surface is wired and well-formed.
	for _, want := range []string{
		`kv_engine_info{engine="btree"} 1`,
		"# TYPE kv_fsync_total counter",
		"# HELP kv_page_count ",
		"kv_cache_hit_ratio ",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("kv metrics output missing %q\n--- got ---\n%s", want, out)
		}
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

func TestCLIBackupRestoreRoundTrip(t *testing.T) {
	src := dbPath(t)
	run([]string{"set", src, "k1", "v1"})
	run([]string{"set", src, "k2", "v2"})

	backupFile := filepath.Join(t.TempDir(), "snap.kvbak")
	if code := run([]string{"backup", src, "--output", backupFile}); code != exitOK {
		t.Fatalf("backup: exit %d", code)
	}

	dst := filepath.Join(t.TempDir(), "dst.kv")
	if code := run([]string{"restore", dst, "--input", backupFile}); code != exitOK {
		t.Fatalf("restore: exit %d", code)
	}
	// Restore must refuse to clobber an existing file.
	if code := run([]string{"restore", dst, "--input", backupFile}); code == exitOK {
		t.Fatal("restore over existing file should not exit 0")
	}

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
		t.Fatalf("restored = %v, want [k1=v1 k2=v2]", got)
	}
}

func TestCLIShipReplayRoundTrip(t *testing.T) {
	// Seed a primary and capture a base backup. backup checkpoints the primary, so writes
	// made after it land in a fresh WAL generation that ship carries to the follower.
	src := dbPath(t)
	run([]string{"set", src, "k1", "v1"})
	run([]string{"set", src, "k2", "v2"})

	baseFile := filepath.Join(t.TempDir(), "base.kvbak")
	if code := run([]string{"backup", src, "--output", baseFile}); code != exitOK {
		t.Fatalf("backup: exit %d", code)
	}
	follower := filepath.Join(t.TempDir(), "follower.kv")
	if code := run([]string{"restore", follower, "--input", baseFile}); code != exitOK {
		t.Fatalf("restore base: exit %d", code)
	}

	// A post-base write the follower must learn through shipping.
	run([]string{"set", src, "k3", "v3"})
	deltaFile := filepath.Join(t.TempDir(), "delta.kvship")
	if code := run([]string{"ship", src, "--output", deltaFile}); code != exitOK {
		t.Fatalf("ship: exit %d", code)
	}
	if code := run([]string{"replay", follower, "--input", deltaFile}); code != exitOK {
		t.Fatalf("replay: exit %d", code)
	}

	out := capture(t, func() { run([]string{"scan", follower, "-f", "jsonl"}) })
	var got []string
	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		var r record
		json.Unmarshal(sc.Bytes(), &r)
		got = append(got, r.Key+"="+r.Value)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != "k1=v1,k2=v2,k3=v3" {
		t.Fatalf("follower = %v, want [k1=v1 k2=v2 k3=v3]", got)
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

// TestCLICheckpointModes runs every checkpoint mode through the CLI and confirms each folds
// the data soundly, plus that an unknown mode is a usage error (spec 09 §1.2, spec 16).
func TestCLICheckpointModes(t *testing.T) {
	for _, mode := range []string{"passive", "full", "restart", "truncate"} {
		p := dbPath(t)
		for _, k := range []string{"a", "b", "c"} {
			if code := run([]string{"set", p, k, "v"}); code != exitOK {
				t.Fatalf("[%s] set %s: exit %d", mode, k, code)
			}
		}
		if code := run([]string{"checkpoint", p, "--mode", mode}); code != exitOK {
			t.Fatalf("checkpoint --mode %s: exit %d, want 0", mode, code)
		}
		if code := run([]string{"check", p}); code != exitOK {
			t.Fatalf("[%s] check after checkpoint: exit %d", mode, code)
		}
		if code := run([]string{"get", p, "b"}); code != exitOK {
			t.Fatalf("[%s] get after checkpoint: exit %d", mode, code)
		}
	}

	bad := dbPath(t)
	if code := run([]string{"checkpoint", bad, "--mode", "bogus"}); code != exitUsage {
		t.Fatalf("unknown mode = exit %d, want %d", code, exitUsage)
	}
}

// TestCLIPragmaWalCheckpoint drives a checkpoint through the pragma surface, both the bare
// read form (a passive checkpoint) and the value form selecting truncate.
func TestCLIPragmaWalCheckpoint(t *testing.T) {
	p := dbPath(t)
	for _, k := range []string{"a", "b"} {
		if code := run([]string{"set", p, k, "v"}); code != exitOK {
			t.Fatalf("set %s: exit %d", k, code)
		}
	}
	out := capture(t, func() {
		if code := run([]string{"pragma", p, "wal_checkpoint"}); code != exitOK {
			t.Fatalf("pragma wal_checkpoint: exit %d", code)
		}
	})
	if !strings.Contains(out, "checkpointed") {
		t.Fatalf("wal_checkpoint output = %q, want a checkpointed confirmation", out)
	}
	out = capture(t, func() {
		if code := run([]string{"pragma", p, "wal_checkpoint=truncate"}); code != exitOK {
			t.Fatalf("pragma wal_checkpoint=truncate: exit %d", code)
		}
	})
	if !strings.Contains(out, "truncate") {
		t.Fatalf("wal_checkpoint=truncate output = %q, want it to name the mode", out)
	}
	if code := run([]string{"check", p}); code != exitOK {
		t.Fatalf("check after pragma checkpoint: exit %d", code)
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

// TestCLICheckDetectsCorruption is the M3 exit-gate test at the command boundary: a
// populated file whose data page bit-rots on disk makes `kv check` exit exitCorrupt(4)
// and report the checksum class, while the same file before corruption exits 0. It drives
// the whole stack the way a release gate or a CI soundness check does, through the real OS
// filesystem (spec 02 §3.2, spec 23 §3, spec 24 M3).
func TestCLICheckDetectsCorruption(t *testing.T) {
	p := dbPath(t)
	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("k%02d", i)
		if code := run([]string{"set", p, k, "value"}); code != exitOK {
			t.Fatalf("set %s: exit %d", k, code)
		}
	}
	// Fold the writes into the main file so the data pages carry stamped checksums on disk.
	if code := run([]string{"checkpoint", p}); code != exitOK {
		t.Fatalf("checkpoint: exit %d", code)
	}
	// A sound file passes.
	if code := run([]string{"check", p}); code != exitOK {
		t.Fatalf("check on a sound file = exit %d, want %d", code, exitOK)
	}

	// Corrupt the first data page (page 2) directly on the OS file, behind the database.
	const pageSize = 4096
	corruptCLIPage(t, p, 2, pageSize)

	// check must now flag the file and exit with the corruption code.
	if code := run([]string{"check", p}); code != exitCorrupt {
		t.Fatalf("check on a bit-rotted file = exit %d, want %d", code, exitCorrupt)
	}
	// The machine-readable form reports ok=false so a script can gate on it.
	out := capture(t, func() {
		if code := run([]string{"check", p, "-f", "json"}); code != exitCorrupt {
			t.Fatalf("check -f json on a corrupt file = exit %d, want %d", code, exitCorrupt)
		}
	})
	var got struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	if got.OK {
		t.Fatalf("check json ok = true on a corrupt file: %q", out)
	}
}

// corruptCLIPage flips one content byte of a page directly in the on-disk file, modelling
// bit rot under the real OS filesystem the CLI runs on.
func corruptCLIPage(t *testing.T, path string, pgno int, pageSize int) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	buf := make([]byte, pageSize)
	off := int64(pgno-1) * int64(pageSize)
	if _, err := f.ReadAt(buf, off); err != nil {
		t.Fatalf("read page %d: %v", pgno, err)
	}
	buf[pageSize/2] ^= 0xFF
	if _, err := f.WriteAt(buf, off); err != nil {
		t.Fatalf("write page %d: %v", pgno, err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
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

// TestCLIVacuumFull confirms the full-vacuum command rebuilds a churned file, prints a
// before/after page summary, and leaves every live key readable through the swapped-in file
// while the deleted keys stay gone (spec 09 §3.2, spec 16).
func TestCLIVacuumFull(t *testing.T) {
	p := dbPath(t)
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("k%04d", i)
		if code := run([]string{"set", p, k, "v" + k}); code != exitOK {
			t.Fatalf("set %s: exit %d", k, code)
		}
	}
	for i := 0; i < 100; i++ {
		if code := run([]string{"del", p, fmt.Sprintf("k%04d", i)}); code != exitOK {
			t.Fatalf("del: exit %d", code)
		}
	}

	out := capture(t, func() {
		if code := run([]string{"vacuum", p, "--full"}); code != exitOK {
			t.Fatalf("vacuum --full: exit %d, want 0", code)
		}
	})
	if !strings.Contains(out, "compacted") {
		t.Fatalf("vacuum --full output = %q, want a compacted summary", out)
	}

	if code := run([]string{"check", p}); code != exitOK {
		t.Fatalf("check after full vacuum: exit %d, want 0", code)
	}
	if code := run([]string{"get", p, "k0000"}); code != exitNotFound {
		t.Fatalf("deleted key after full vacuum = exit %d, want %d", code, exitNotFound)
	}
	got := capture(t, func() {
		if code := run([]string{"get", p, "k0150"}); code != exitOK {
			t.Fatalf("get survivor: exit %d", code)
		}
	})
	if !strings.Contains(got, "vk0150") {
		t.Fatalf("survivor value = %q, want vk0150", got)
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

// shellSession runs the interactive shell on a fresh database, feeding it the script on
// stdin, and returns what it wrote to stdout. The shell's chrome (banner, prompt, notices)
// goes to stderr and is not captured, so the returned string is just command results.
func shellSession(t *testing.T, script string) string {
	t.Helper()
	p := dbPath(t)
	return capture(t, func() {
		withStdin(t, script, func() {
			if code := run([]string{p}); code != exitOK {
				t.Fatalf("shell: exit %d", code)
			}
		})
	})
}

// TestShellSetGetExists drives the core data verbs through the REPL and checks the results
// stream that reaches stdout.
func TestShellSetGetExists(t *testing.T) {
	out := shellSession(t, "set k v1\nget k\nexists k\nexists missing\n.exit\n")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	want := []string{"v1", "true", "false"}
	if len(lines) != len(want) {
		t.Fatalf("got %d result lines %q, want %d", len(lines), lines, len(want))
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Fatalf("line %d = %q, want %q", i, lines[i], want[i])
		}
	}
}

// TestShellQuotedValue confirms the shell tokenizer keeps a single-quoted value with
// spaces and embedded double quotes intact, the sqlite3-shell convention.
func TestShellQuotedValue(t *testing.T) {
	out := shellSession(t, "set doc '{\"name\": \"a b\"}'\nget doc\n.exit\n")
	if got := strings.TrimSpace(out); got != `{"name": "a b"}` {
		t.Fatalf("get doc = %q, want the quoted JSON intact", got)
	}
}

// TestShellScanAndCount checks a prefix scan and a count run against the open file, with
// the format dot-command selecting the rendering.
func TestShellScanAndCount(t *testing.T) {
	out := shellSession(t, ".format raw\nset user:1 a\nset user:2 b\nset other x\nscan --prefix user: --keys-only\ncount --prefix user:\n.exit\n")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	// raw key-only scan prints the two user keys, then count prints 2.
	want := []string{"user:1", "user:2", "2"}
	if len(lines) != len(want) {
		t.Fatalf("got %q, want %q", lines, want)
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Fatalf("line %d = %q, want %q", i, lines[i], want[i])
		}
	}
}

// TestShellTransactionRollback proves an explicit transaction holds writes until commit and
// drops them on rollback.
func TestShellTransactionRollback(t *testing.T) {
	out := shellSession(t, ".begin\nset tmp 1\nget tmp\n.rollback\nexists tmp\n.begin\nset keep 1\n.commit\nexists keep\n.exit\n")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	// get tmp (inside txn) -> 1, exists tmp (after rollback) -> false, exists keep -> true.
	want := []string{"1", "false", "true"}
	if len(lines) != len(want) {
		t.Fatalf("got %q, want %q", lines, want)
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Fatalf("line %d = %q, want %q", i, lines[i], want[i])
		}
	}
}

// TestShellDotCommands checks the meta-commands that print to stdout: .info, .check, and
// .stats all produce their expected shapes against a populated file.
func TestShellDotCommands(t *testing.T) {
	out := shellSession(t, "set a 1\nset b 2\n.checkpoint\n.info\n.check\n.stats\n.exit\n")
	if !strings.Contains(out, "engine") || !strings.Contains(out, "page count") {
		t.Fatalf(".info output missing expected fields:\n%s", out)
	}
	if !strings.Contains(out, "ok") {
		t.Fatalf(".check did not report ok:\n%s", out)
	}
	var stats struct {
		Engine    string `json:"engine"`
		PageCount int    `json:"page_count"`
	}
	// The .stats object is the last JSON document in the stream; find its opening brace.
	if i := strings.LastIndex(out, "{"); i >= 0 {
		if err := json.Unmarshal([]byte(out[i:]), &stats); err != nil {
			t.Fatalf("decode .stats: %v\n%s", err, out)
		}
	}
	if stats.Engine == "" {
		t.Fatalf(".stats engine empty:\n%s", out)
	}
}

// TestShellUnknownCommandContinues confirms a bad statement is reported but does not end
// the session: a following good statement still runs.
func TestShellUnknownCommandContinues(t *testing.T) {
	out := shellSession(t, "bogus arg\nset k v\nget k\n.exit\n")
	if got := strings.TrimSpace(out); got != "v" {
		t.Fatalf("session output = %q, want just v (the good statement after the bad one)", got)
	}
}

// TestShellOpensOnlyExistingFile confirms `kv <name>` for a path that does not exist is an
// unknown-command usage error, not a shell on a missing file.
func TestShellOpensOnlyExistingFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope.kv")
	if code := run([]string{missing}); code != exitUsage {
		t.Fatalf("run on missing file = exit %d, want %d (usage)", code, exitUsage)
	}
}

// TestPragmaReadAndPersistentSet drives the pragma command: reading a create-time knob, then
// setting a persistent header tag and reading it back through a fresh open (a new process
// would do the same), proving the value is durable.
func TestPragmaReadAndPersistentSet(t *testing.T) {
	p := dbPath(t)
	if got := strings.TrimSpace(capture(t, func() { run([]string{"pragma", p, "engine"}) })); got != "btree" {
		t.Fatalf("pragma engine = %q, want btree", got)
	}
	if code := run([]string{"pragma", p, "application_id=305419896"}); code != exitOK {
		t.Fatalf("set application_id: exit %d", code)
	}
	// A second invocation opens the file afresh and must see the persisted tag.
	if got := strings.TrimSpace(capture(t, func() { run([]string{"pragma", p, "application_id"}) })); got != "305419896" {
		t.Fatalf("application_id after reopen = %q, want 305419896", got)
	}
	// Hex input is accepted and echoed back in decimal.
	if got := strings.TrimSpace(capture(t, func() { run([]string{"pragma", p, "user_version=0x10"}) })); got != "16" {
		t.Fatalf("set user_version=0x10 echoed %q, want 16", got)
	}
}

// TestPragmaTierErrors confirms the wrong-tier and bad-input rejections all report a usage
// error (exit 2), not an IO or corruption code.
func TestPragmaTierErrors(t *testing.T) {
	p := dbPath(t)
	cases := []struct {
		name string
		expr string
	}{
		{"create-time", "page_size=8192"},
		{"read-only", "page_count=5"},
		{"unknown", "bogus_knob"},
		{"bad-value", "application_id=not-a-number"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if code := run([]string{"pragma", p, tc.expr}); code != exitUsage {
				t.Fatalf("pragma %s = exit %d, want %d (usage)", tc.expr, code, exitUsage)
			}
		})
	}
}

// TestPragmaIncrementalVacuum confirms the incremental_vacuum action is reachable through the
// pragma surface and reports how many pages it freed.
func TestPragmaIncrementalVacuum(t *testing.T) {
	p := dbPath(t)
	out := strings.TrimSpace(capture(t, func() {
		if code := run([]string{"pragma", p, "incremental_vacuum"}); code != exitOK {
			t.Fatalf("incremental_vacuum: exit %d", code)
		}
	}))
	if !strings.HasPrefix(out, "freed ") || !strings.HasSuffix(out, "page(s)") {
		t.Fatalf("incremental_vacuum output = %q, want a freed-N-page(s) line", out)
	}
}

// TestShellPragma drives the pragma surface through the shell: a persistent set then a read,
// both on the data stream.
func TestShellPragma(t *testing.T) {
	out := shellSession(t, ".pragma application_id=123\n.pragma application_id\n.exit\n")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	want := []string{"123", "123"}
	if len(lines) != len(want) {
		t.Fatalf("got %q, want %q", lines, want)
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Fatalf("line %d = %q, want %q", i, lines[i], want[i])
		}
	}
}
