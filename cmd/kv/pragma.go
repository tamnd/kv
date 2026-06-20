package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/tamnd/kv"
)

// pragmaUsageErr marks an error caused by bad pragma input (unknown name, wrong tier, a
// value that does not parse) rather than by the database. The CLI maps it to exit 2 (usage),
// keeping the IO/corruption codes for genuine library failures.
type pragmaUsageErr struct{ err error }

func (e pragmaUsageErr) Error() string { return e.err.Error() }

func usagePragma(format string, a ...any) error {
	return pragmaUsageErr{fmt.Errorf(format, a...)}
}

// A pragma is one configuration knob reachable by name, the kv analog of SQLite's PRAGMA
// (spec 22). Reading is `name`; setting is `name=value`. The same registry backs the
// `kv pragma` command and the shell's `.pragma` dot-command, so the one knob has one
// implementation behind both surfaces.
//
// kv's configuration model has three tiers (spec 22 §1): create-time settings fixed for the
// file's life, persistent-runtime settings remembered in the file, and session settings
// chosen per open. This registry exposes the knobs that have real backing state today: the
// read-only informational counters, the two persistent header tags (application_id,
// user_version), the create-time identity (engine, page_size) which read here but can only be
// set at `kv create`, and the incremental_vacuum action. Knobs for unbuilt subsystems (LSM,
// compression, encryption, server) are intentionally absent rather than stubbed.
type pragma struct {
	name    string
	summary string
	// get reads the current value as a display string.
	get func(d *kv.DB) (string, error)
	// set applies a value and returns a confirmation string. It is nil for a read-only
	// pragma; createTime distinguishes "set it at create" from "this can never be set".
	set        func(d *kv.DB, val string) (string, error)
	createTime bool
}

// pragmas is the registry, ordered for a stable help listing.
func pragmas() []pragma {
	stat := func(f func(s kv.Stats) string) func(*kv.DB) (string, error) {
		return func(d *kv.DB) (string, error) { return f(d.Stats()), nil }
	}
	return []pragma{
		{
			name:       "engine",
			summary:    "storage core (create-time)",
			get:        stat(func(s kv.Stats) string { return s.Engine.String() }),
			createTime: true,
		},
		{
			name:       "page_size",
			summary:    "page size in bytes (create-time)",
			get:        stat(func(s kv.Stats) string { return strconv.Itoa(s.PageSize) }),
			createTime: true,
		},
		{
			name:    "application_id",
			summary: "application-defined file tag (persistent)",
			get:     func(d *kv.DB) (string, error) { return strconv.FormatUint(uint64(d.ApplicationID()), 10), nil },
			set:     setU32(func(d *kv.DB, v uint32) error { return d.SetApplicationID(v) }),
		},
		{
			name:    "user_version",
			summary: "application-defined version counter (persistent)",
			get:     func(d *kv.DB) (string, error) { return strconv.FormatUint(uint64(d.UserVersion()), 10), nil },
			set:     setU32(func(d *kv.DB, v uint32) error { return d.SetUserVersion(v) }),
		},
		{
			name:    "page_count",
			summary: "file size in pages (read-only)",
			get:     stat(func(s kv.Stats) string { return strconv.FormatUint(uint64(s.PageCount), 10) }),
		},
		{
			name:    "freelist_count",
			summary: "pages on the freelist (read-only)",
			get:     stat(func(s kv.Stats) string { return strconv.FormatInt(s.FreePages, 10) }),
		},
		{
			name:    "physical_bytes",
			summary: "on-disk footprint in bytes (read-only)",
			get:     stat(func(s kv.Stats) string { return strconv.FormatInt(s.PhysicalBytes, 10) }),
		},
		{
			name:    "live_keys",
			summary: "live key count, zero if not tracked (read-only)",
			get:     stat(func(s kv.Stats) string { return strconv.FormatInt(s.LiveKeys, 10) }),
		},
		{
			name:    "live_bytes",
			summary: "live data bytes, zero if not tracked (read-only)",
			get:     stat(func(s kv.Stats) string { return strconv.FormatInt(s.LiveBytes, 10) }),
		},
		{
			name:    "amplification",
			summary: "space amplification, physical/live (read-only)",
			get:     stat(func(s kv.Stats) string { return strconv.FormatFloat(s.Amplification, 'f', 2, 64) }),
		},
		{
			name:    "commit_version",
			summary: "latest committed version (read-only)",
			get:     stat(func(s kv.Stats) string { return strconv.FormatUint(s.Version, 10) }),
		},
		{
			name:    "wal_frames",
			summary: "frames the WAL has written (read-only)",
			get:     stat(func(s kv.Stats) string { return strconv.FormatUint(s.WALFrames, 10) }),
		},
		{
			name:    "wal_backlog",
			summary: "frames committed but not yet checkpointed (read-only)",
			get:     stat(func(s kv.Stats) string { return strconv.FormatUint(s.WALBacklog, 10) }),
		},
		{
			name:    "syncs",
			summary: "fsyncs since open (read-only)",
			get:     stat(func(s kv.Stats) string { return strconv.FormatUint(s.Syncs, 10) }),
		},
		{
			name:    "incremental_vacuum",
			summary: "return up to N trailing free pages to the OS; 0 or empty means all",
			get: func(d *kv.DB) (string, error) {
				freed, err := d.Vacuum(0)
				if err != nil {
					return "", err
				}
				return fmt.Sprintf("freed %d page(s)", freed), nil
			},
			set: func(d *kv.DB, val string) (string, error) {
				n, err := strconv.Atoi(strings.TrimSpace(val))
				if err != nil {
					return "", usagePragma("incremental_vacuum wants a page count, got %q", val)
				}
				freed, err := d.Vacuum(n)
				if err != nil {
					return "", err
				}
				return fmt.Sprintf("freed %d page(s)", freed), nil
			},
		},
	}
}

// setU32 adapts a uint32 header setter into a pragma set func, parsing decimal or 0x-hex.
func setU32(apply func(d *kv.DB, v uint32) error) func(*kv.DB, string) (string, error) {
	return func(d *kv.DB, val string) (string, error) {
		n, err := strconv.ParseUint(strings.TrimSpace(val), 0, 32)
		if err != nil {
			return "", usagePragma("want an unsigned 32-bit integer, got %q", val)
		}
		if err := apply(d, uint32(n)); err != nil {
			return "", err
		}
		return strconv.FormatUint(n, 10), nil
	}
}

// lookupPragma finds a pragma by name, case-insensitively.
func lookupPragma(name string) (pragma, bool) {
	name = strings.ToLower(name)
	for _, p := range pragmas() {
		if p.name == name {
			return p, true
		}
	}
	return pragma{}, false
}

// applyPragma parses one pragma expression and runs it against the open database, returning
// the line to print. expr is `name`, `name=value`, or `name(value)` (the incremental_vacuum
// call form). A leading "pragma " keyword is tolerated for SQLite muscle memory. A read on a
// read-only or create-time knob succeeds; a write to one is a clear error.
func applyPragma(d *kv.DB, expr string) (string, error) {
	expr = strings.TrimSpace(expr)
	if rest, ok := cutKeyword(expr, "pragma"); ok {
		expr = strings.TrimSpace(rest)
	}
	name, val, hasVal := splitPragma(expr)
	if name == "" {
		return "", usagePragma("empty pragma")
	}
	p, ok := lookupPragma(name)
	if !ok {
		return "", usagePragma("unknown pragma %q (try: kv pragma <db> help)", name)
	}
	if !hasVal {
		return p.get(d)
	}
	if p.set == nil {
		if p.createTime {
			return "", usagePragma("%s is a create-time setting; choose it at `kv create`", name)
		}
		return "", usagePragma("%s is read-only", name)
	}
	return p.set(d, val)
}

// splitPragma breaks a pragma expression into its name and optional value. It accepts both
// `name=value` and the `name(value)` call form; hasVal is false for a bare `name`.
func splitPragma(expr string) (name, val string, hasVal bool) {
	if i := strings.IndexByte(expr, '='); i >= 0 {
		return strings.TrimSpace(expr[:i]), strings.TrimSpace(expr[i+1:]), true
	}
	if i := strings.IndexByte(expr, '('); i >= 0 && strings.HasSuffix(expr, ")") {
		return strings.TrimSpace(expr[:i]), strings.TrimSpace(expr[i+1 : len(expr)-1]), true
	}
	return strings.TrimSpace(expr), "", false
}

// cutKeyword strips a leading case-insensitive keyword followed by whitespace, reporting
// whether it was present.
func cutKeyword(s, kw string) (string, bool) {
	if len(s) > len(kw) && strings.EqualFold(s[:len(kw)], kw) && (s[len(kw)] == ' ' || s[len(kw)] == '\t') {
		return s[len(kw):], true
	}
	return s, false
}

// writePragmaHelp lists the known pragmas to w-style output via the caller's printer.
func pragmaHelpLines() []string {
	lines := make([]string, 0, len(pragmas()))
	for _, p := range pragmas() {
		lines = append(lines, fmt.Sprintf("  %-20s %s", p.name, p.summary))
	}
	return lines
}

// cmdPragma reads or sets one configuration knob on a database (spec 22, spec 16 §4):
//
//	kv pragma <db> <name>           print a knob's value
//	kv pragma <db> <name>=<value>   set a persistent knob
//	kv pragma <db> incremental_vacuum[=N]   run an incremental vacuum
//	kv pragma <db> help             list the known knobs
func cmdPragma(args []string) int {
	if len(args) >= 1 && (args[0] == "-h" || args[0] == "--help") {
		printPragmaUsage()
		return exitOK
	}
	if len(args) < 2 {
		return usageErr("usage: kv pragma <db> <name>[=<value>] (or: kv pragma <db> help)")
	}
	path := args[0]
	expr := strings.Join(args[1:], "")
	if strings.EqualFold(strings.TrimSpace(expr), "help") {
		printPragmaUsage()
		return exitOK
	}
	d, code := openDB(path)
	if code != exitOK {
		return code
	}
	defer d.Close()

	out, err := applyPragma(d, expr)
	if err != nil {
		var ue pragmaUsageErr
		if errors.As(err, &ue) {
			return usageErr("%v", err)
		}
		return fail(err)
	}
	fmt.Println(out)
	return exitOK
}

func printPragmaUsage() {
	fmt.Println("usage: kv pragma <db> <name>[=<value>]")
	fmt.Println()
	fmt.Println("Knobs:")
	for _, line := range pragmaHelpLines() {
		fmt.Println(line)
	}
}
