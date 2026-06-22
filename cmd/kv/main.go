// Command kv is the operator's and scripter's interface to a kv database file, the
// SQLite-shell analog (spec 16). It is a thin layer over the public library (spec 15):
// every command opens a *kv.DB and calls public API, so the CLI is the forcing function
// that keeps the library surface complete.
//
// The tool is a single static pure-Go binary with no runtime dependencies (spec 20):
// the command surface is built on the standard library's flag package and a small
// dispatcher rather than an external CLI framework, so the build stays dependency-free.
//
// Output is line-oriented and pipe-friendly; -f json|jsonl|table|raw selects the
// format, auto-picking table on a TTY and jsonl in a pipe, so the tool composes with
// jq, grep, and shell loops. Exit codes are meaningful and mirror the typed library
// errors (spec 16 §7).
package main

import (
	"fmt"
	"os"
)

// exit codes, mirroring the typed library errors so a script gets the same signal from
// the CLI as from the API (spec 16 §7).
const (
	exitOK       = 0
	exitNotFound = 1 // not found / false (get/exists on a missing key)
	exitUsage    = 2 // bad flags or arguments
	exitOpen     = 3 // file not found / cannot open
	exitCorrupt  = 4 // corruption detected
	exitLock     = 5 // needs recovery / lock held by another writer
	exitConflict = 6 // write could not commit after retries
	exitCrypto   = 7 // encryption error
	exitIO       = 8 // I/O / durability error
)

// command is one subcommand: a name, a one-line summary, and a run function that
// returns an exit code. Keeping run returning the code (not calling os.Exit) keeps the
// commands testable.
type command struct {
	name    string
	summary string
	run     func(args []string) int
}

func commands() []command {
	return []command{
		{"create", "create a new database file with create-time options", cmdCreate},
		{"get", "print the value for a key", cmdGet},
		{"set", "upsert a key to a value", cmdSet},
		{"del", "delete one key", cmdDel},
		{"del-range", "range-delete [lo, hi)", cmdDelRange},
		{"exists", "exit 0 if a key is present, 1 if absent", cmdExists},
		{"merge", "apply the registered merge operator to a key", cmdMerge},
		{"scan", "range or prefix scan", cmdScan},
		{"count", "count keys in a range or prefix", cmdCount},
		{"dump", "stream all key/value pairs as JSONL", cmdDump},
		{"load", "bulk-load key/value pairs from stdin or a file", cmdLoad},
		{"checkpoint", "fold the WAL into the main file", cmdCheckpoint},
		{"backup", "stream a consistent physical image to a file or stdout", cmdBackup},
		{"restore", "rebuild a database from a backup stream", cmdRestore},
		{"ship", "stream the current WAL generation as a replication delta", cmdShip},
		{"replay", "apply a shipped WAL delta onto a read-only follower", cmdReplay},
		{"vacuum", "return trailing free pages to the OS; shrink the file", cmdVacuum},
		{"pragma", "read or set a configuration knob (engine, application_id, ...)", cmdPragma},
		{"check", "verify structural integrity; exit 4 on any violation", cmdCheck},
		{"info", "print a human summary of the database", cmdInfo},
		{"stats", "print space and durability accounting as JSON", cmdStats},
		{"metrics", "print observability metrics in Prometheus text format", cmdMetrics},
		{"serve", "serve the database over HTTP/JSON", cmdServe},
	}
}

func main() {
	os.Exit(run(os.Args[1:]))
}

// run dispatches to a subcommand and returns its exit code.
func run(args []string) int {
	if len(args) == 0 {
		usage(os.Stderr)
		return exitUsage
	}
	switch args[0] {
	case "-h", "--help", "help":
		usage(os.Stdout)
		return exitOK
	}
	for _, c := range commands() {
		if c.name == args[0] {
			return c.run(args[1:])
		}
	}
	// `kv <file>` with no subcommand opens the interactive shell on that file, the
	// sqlite3-shell convention (spec 16 §1, §5). The single argument must be an existing
	// file, so a genuine mistyped command still gets the unknown-command error.
	if len(args) == 1 {
		if _, err := os.Stat(args[0]); err == nil {
			return runShell(args[0])
		}
	}
	fmt.Fprintf(os.Stderr, "kv: unknown command %q\n\n", args[0])
	usage(os.Stderr)
	return exitUsage
}

// usage prints the top-level help.
func usage(w *os.File) {
	fmt.Fprintln(w, "kv - embeddable ordered key/value database")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  kv <command> <db> [args] [flags]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Commands:")
	for _, c := range commands() {
		fmt.Fprintf(w, "  %-12s %s\n", c.name, c.summary)
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Run 'kv <command> -h' for command flags.")
}
