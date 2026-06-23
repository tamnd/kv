package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tamnd/kv"
)

// runShell opens the interactive REPL on path, the sqlite3-shell analog reached by
// invoking `kv <file>` with no subcommand (spec 16 §5). Bare data commands operate on the
// open file with auto-commit per statement; dot-commands are meta-operations. It returns a
// process exit code: exitOK on a clean .exit or end of input, or an open/IO code if the
// file cannot be opened or a final flush fails.
func runShell(path string) int {
	d, code := openDB(path)
	if code != exitOK {
		return code
	}
	defer d.Close()
	sh := &shell{
		db:          d,
		path:        path,
		in:          bufio.NewReader(os.Stdin),
		out:         os.Stdout,
		errOut:      os.Stderr,
		format:      fmtAuto,
		interactive: isTerminal(os.Stdin),
	}
	return sh.run()
}

// shell holds the REPL's state across statements: the open database, the output format
// and binary encoding selected by dot-commands, the optional explicit transaction held by
// .begin, and whether per-statement timing is on. Chrome (banner, prompt, notices, errors)
// goes to errOut; command results go to out, so the shell stays pipeable.
type shell struct {
	db   *kv.DB
	path string

	in     *bufio.Reader
	out    io.Writer
	errOut io.Writer

	format      outputFormat
	encoding    enc
	timer       bool
	interactive bool

	// txn is the explicit transaction held by .begin, or nil in auto-commit mode. When
	// set, every bare statement runs inside it until .commit or .rollback.
	txn *kv.Txn
}

// run is the read-eval-print loop. It prints the banner and a prompt when interactive,
// reads one statement per line, dispatches it, and prints any error without aborting the
// session, so a typo never drops the operator out of the shell. It returns when input ends
// or .exit is run.
func (sh *shell) run() int {
	if sh.interactive {
		fmt.Fprintf(sh.errOut, "kv %s  engine=%s  %s\n", Version, sh.db.Stats().Engine, sh.path)
		fmt.Fprintln(sh.errOut, `Enter ".help" for commands, ".exit" to quit.`)
	}
	for {
		if sh.interactive {
			fmt.Fprint(sh.errOut, sh.prompt())
		}
		line, err := sh.in.ReadString('\n')
		if len(line) == 0 && err != nil {
			// End of input (EOF) or a read error: leave the loop cleanly.
			break
		}
		stmt := strings.TrimSpace(line)
		if stmt == "" {
			if err != nil {
				break
			}
			continue
		}
		if quit := sh.dispatch(stmt); quit {
			break
		}
		if err != nil {
			break
		}
	}
	// A held transaction at end of session is rolled back: its writes were never committed.
	if sh.txn != nil {
		sh.txn.Discard()
		sh.txn = nil
	}
	if sh.interactive {
		fmt.Fprintln(sh.errOut, "")
	}
	return exitOK
}

// prompt is the input marker; it changes inside an explicit transaction so the operator
// can see uncommitted state is pending.
func (sh *shell) prompt() string {
	if sh.txn != nil {
		return "kv*> "
	}
	return "kv> "
}

// dispatch runs one statement and reports whether the loop should exit. A dot-prefixed
// statement is a meta-command; anything else is a bare data command. Errors are printed to
// errOut and swallowed so the session continues.
func (sh *shell) dispatch(stmt string) (quit bool) {
	start := time.Now()
	defer func() {
		if sh.timer {
			fmt.Fprintf(sh.errOut, "run time: %s\n", time.Since(start).Round(time.Microsecond))
		}
	}()

	toks, err := tokenize(stmt)
	if err != nil {
		fmt.Fprintf(sh.errOut, "kv: %v\n", err)
		return false
	}
	if len(toks) == 0 {
		return false
	}
	if strings.HasPrefix(toks[0], ".") {
		return sh.dot(toks)
	}
	if err := sh.bare(toks); err != nil {
		fmt.Fprintf(sh.errOut, "kv: %v\n", err)
	}
	return false
}

// read runs fn in the current read context: the held explicit transaction if one is open,
// else a fresh auto-commit read snapshot.
func (sh *shell) read(fn func(*kv.Txn) error) error {
	if sh.txn != nil {
		return fn(sh.txn)
	}
	return sh.db.View(fn)
}

// write runs fn in the current write context: the held explicit transaction if one is
// open, else a fresh auto-commit writable transaction.
func (sh *shell) write(fn func(*kv.Txn) error) error {
	if sh.txn != nil {
		return fn(sh.txn)
	}
	return sh.db.Update(fn)
}

// bare runs a non-dot data command against the open database. The verb set mirrors the
// top-level data commands, minus the database path argument the shell already holds.
func (sh *shell) bare(toks []string) error {
	verb, args := toks[0], toks[1:]
	switch verb {
	case "get":
		return sh.cmdGet(args)
	case "set":
		return sh.cmdSet(args)
	case "del":
		return sh.cmdDel(args)
	case "del-range":
		return sh.cmdDelRange(args)
	case "exists":
		return sh.cmdExists(args)
	case "merge":
		return sh.cmdMerge(args)
	case "scan":
		return sh.cmdScan(args)
	case "count":
		return sh.cmdCount(args)
	default:
		return fmt.Errorf("unknown command %q (try .help)", verb)
	}
}

// shellEnc binds the shared --hex/--base64 flags on top of the shell's current default
// encoding, so a statement can override the binary encoding for that one command.
func (sh *shell) shellEnc(fs *flag.FlagSet) *enc {
	e := &enc{hex: sh.encoding.hex, base64: sh.encoding.base64}
	fs.BoolVar(&e.hex, "hex", e.hex, "keys and values are hex-encoded")
	fs.BoolVar(&e.base64, "base64", e.base64, "keys and values are base64-encoded")
	return e
}

func (sh *shell) cmdGet(args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	e := sh.shellEnc(fs)
	if err := parseArgs(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: get <key>")
	}
	key, err := e.decode(fs.Arg(0))
	if err != nil {
		return fmt.Errorf("bad key: %w", err)
	}
	var val []byte
	rerr := sh.read(func(txn *kv.Txn) error {
		v, err := txn.GetCopy(key)
		val = v
		return err
	})
	if rerr == kv.ErrNotFound {
		fmt.Fprintln(sh.errOut, "(not found)")
		return nil
	}
	if rerr != nil {
		return rerr
	}
	fmt.Fprintln(sh.out, e.encode(val))
	return nil
}

func (sh *shell) cmdSet(args []string) error {
	fs := flag.NewFlagSet("set", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	e := sh.shellEnc(fs)
	if err := parseArgs(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return fmt.Errorf("usage: set <key> <value>")
	}
	key, err := e.decode(fs.Arg(0))
	if err != nil {
		return fmt.Errorf("bad key: %w", err)
	}
	value, err := e.decode(fs.Arg(1))
	if err != nil {
		return fmt.Errorf("bad value: %w", err)
	}
	return sh.write(func(txn *kv.Txn) error { return txn.Set(key, value) })
}

func (sh *shell) cmdDel(args []string) error {
	fs := flag.NewFlagSet("del", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	e := sh.shellEnc(fs)
	if err := parseArgs(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: del <key>")
	}
	key, err := e.decode(fs.Arg(0))
	if err != nil {
		return fmt.Errorf("bad key: %w", err)
	}
	return sh.write(func(txn *kv.Txn) error { return txn.Delete(key) })
}

func (sh *shell) cmdDelRange(args []string) error {
	fs := flag.NewFlagSet("del-range", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	e := sh.shellEnc(fs)
	if err := parseArgs(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return fmt.Errorf("usage: del-range <lo> <hi>")
	}
	lo, err := e.decode(fs.Arg(0))
	if err != nil {
		return fmt.Errorf("bad lo: %w", err)
	}
	hi, err := e.decode(fs.Arg(1))
	if err != nil {
		return fmt.Errorf("bad hi: %w", err)
	}
	return sh.write(func(txn *kv.Txn) error { return txn.DeleteRange(lo, hi) })
}

func (sh *shell) cmdExists(args []string) error {
	fs := flag.NewFlagSet("exists", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	e := sh.shellEnc(fs)
	if err := parseArgs(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: exists <key>")
	}
	key, err := e.decode(fs.Arg(0))
	if err != nil {
		return fmt.Errorf("bad key: %w", err)
	}
	var present bool
	if err := sh.read(func(txn *kv.Txn) error {
		ok, err := txn.Exists(key)
		present = ok
		return err
	}); err != nil {
		return err
	}
	fmt.Fprintln(sh.out, strconv.FormatBool(present))
	return nil
}

func (sh *shell) cmdMerge(args []string) error {
	fs := flag.NewFlagSet("merge", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	e := sh.shellEnc(fs)
	if err := parseArgs(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return fmt.Errorf("usage: merge <key> <operand>")
	}
	key, err := e.decode(fs.Arg(0))
	if err != nil {
		return fmt.Errorf("bad key: %w", err)
	}
	operand, err := e.decode(fs.Arg(1))
	if err != nil {
		return fmt.Errorf("bad operand: %w", err)
	}
	return sh.write(func(txn *kv.Txn) error { return txn.Merge(key, operand) })
}

func (sh *shell) cmdScan(args []string) error {
	fs := flag.NewFlagSet("scan", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	e := sh.shellEnc(fs)
	spec := bindScanFlags(fs)
	keysOnly := fs.Bool("keys-only", false, "print keys only, skip values")
	if err := parseArgs(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: scan [--prefix P | --from LO --to HI] [--reverse] [--limit N] [--keys-only]")
	}
	opts, err := spec.options(e, *keysOnly)
	if err != nil {
		return fmt.Errorf("bad bound: %w", err)
	}
	// In the shell the output device is the shell's out; auto resolves the same way the
	// scan command does, table when interactive and jsonl when piped.
	resolved := sh.format
	if resolved == fmtAuto {
		if sh.interactive {
			resolved = fmtTable
		} else {
			resolved = fmtJSONL
		}
	}
	w := newRecordWriter(sh.out, resolved, e, *keysOnly)
	scanErr := sh.read(func(txn *kv.Txn) error {
		it, err := txn.NewIterator(opts)
		if err != nil {
			return err
		}
		defer it.Close()
		n := 0
		for it.First(); it.Valid(); it.Next() {
			var val []byte
			if !*keysOnly {
				v, err := it.Value()
				if err != nil {
					return err
				}
				val = v
			}
			if err := w.write(it.Key(), val, *keysOnly); err != nil {
				return err
			}
			n++
			if spec.limit > 0 && n >= spec.limit {
				break
			}
		}
		return it.Error()
	})
	if scanErr != nil {
		return scanErr
	}
	return w.close()
}

func (sh *shell) cmdCount(args []string) error {
	fs := flag.NewFlagSet("count", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	e := sh.shellEnc(fs)
	spec := bindScanFlags(fs)
	if err := parseArgs(fs, args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: count [--prefix P | --from LO --to HI] [--limit N]")
	}
	opts, err := spec.options(e, true)
	if err != nil {
		return fmt.Errorf("bad bound: %w", err)
	}
	count := 0
	if err := sh.read(func(txn *kv.Txn) error {
		it, err := txn.NewIterator(opts)
		if err != nil {
			return err
		}
		defer it.Close()
		for it.First(); it.Valid(); it.Next() {
			count++
			if spec.limit > 0 && count >= spec.limit {
				break
			}
		}
		return it.Error()
	}); err != nil {
		return err
	}
	fmt.Fprintln(sh.out, count)
	return nil
}

// dot runs a meta-command and reports whether the loop should exit.
func (sh *shell) dot(toks []string) (quit bool) {
	switch toks[0] {
	case ".exit", ".quit", ".q":
		return true
	case ".help", ".h":
		sh.help()
	case ".info":
		if err := writeInfo(sh.out, sh.db.Stats()); err != nil {
			fmt.Fprintf(sh.errOut, "kv: %v\n", err)
		}
	case ".stats":
		enc := json.NewEncoder(sh.out)
		enc.SetIndent("", "  ")
		if err := enc.Encode(toJSON(sh.db.Stats())); err != nil {
			fmt.Fprintf(sh.errOut, "kv: %v\n", err)
		}
	case ".metrics":
		if err := sh.db.WriteMetrics(sh.out); err != nil {
			fmt.Fprintf(sh.errOut, "kv: %v\n", err)
		}
	case ".check":
		sh.doCheck()
	case ".checkpoint":
		if err := sh.db.Checkpoint(); err != nil {
			fmt.Fprintf(sh.errOut, "kv: %v\n", err)
		} else {
			fmt.Fprintln(sh.errOut, "checkpoint complete")
		}
	case ".vacuum":
		sh.doVacuum(toks[1:])
	case ".format", ".mode":
		sh.doFormat(toks[1:])
	case ".timer":
		sh.doToggle(toks[1:], &sh.timer, "timer")
	case ".encoding":
		sh.doEncoding(toks[1:])
	case ".begin":
		sh.doBegin()
	case ".commit":
		sh.doCommit()
	case ".rollback":
		sh.doRollback()
	case ".pragma":
		sh.doPragma(toks[1:])
	default:
		fmt.Fprintf(sh.errOut, "kv: unknown dot-command %q (try .help)\n", toks[0])
	}
	return false
}

func (sh *shell) doCheck() {
	rep, err := sh.db.Check()
	if err != nil {
		fmt.Fprintf(sh.errOut, "kv: %v\n", err)
		return
	}
	if rep.OK() {
		fmt.Fprintf(sh.out, "ok  %d pages, %d keys, %d free\n", rep.PagesVisited, rep.Keys, rep.FreePages)
		return
	}
	fmt.Fprintf(sh.out, "FAIL  %d problem(s)\n", len(rep.Problems))
	for _, p := range rep.Problems {
		fmt.Fprintf(sh.out, "  [%s] page %d: %s\n", p.Class, p.Page, p.Detail)
	}
}

func (sh *shell) doVacuum(args []string) {
	budget := 0
	if len(args) > 0 {
		n, err := strconv.Atoi(args[0])
		if err != nil {
			fmt.Fprintf(sh.errOut, "kv: .vacuum wants a page count, got %q\n", args[0])
			return
		}
		budget = n
	}
	freed, err := sh.db.Vacuum(budget)
	if err != nil {
		fmt.Fprintf(sh.errOut, "kv: %v\n", err)
		return
	}
	fmt.Fprintf(sh.errOut, "freed %d page(s), %d page(s) remain\n", freed, sh.db.Stats().PageCount)
}

// doPragma reads or sets one configuration knob through the shared pragma registry, the
// shell face of `kv pragma` (spec 22). With no argument it lists the known knobs; `help`
// does the same. A knob's value, or a set confirmation, prints to out; errors go to errOut.
func (sh *shell) doPragma(args []string) {
	expr := strings.Join(args, "")
	if expr == "" || strings.EqualFold(expr, "help") {
		fmt.Fprintln(sh.errOut, "usage: .pragma <name>[=<value>]")
		for _, line := range pragmaHelpLines() {
			fmt.Fprintln(sh.errOut, line)
		}
		return
	}
	out, err := applyPragma(sh.db, expr)
	if err != nil {
		fmt.Fprintf(sh.errOut, "kv: %v\n", err)
		return
	}
	fmt.Fprintln(sh.out, out)
}

func (sh *shell) doFormat(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(sh.errOut, "kv: usage: .format table|jsonl|json|raw|auto")
		return
	}
	f, err := parseFormat(args[0])
	if err != nil {
		fmt.Fprintf(sh.errOut, "kv: %v\n", err)
		return
	}
	sh.format = f
}

func (sh *shell) doEncoding(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(sh.errOut, "kv: usage: .encoding text|hex|base64")
		return
	}
	switch args[0] {
	case "text":
		sh.encoding = enc{}
	case "hex":
		sh.encoding = enc{hex: true}
	case "base64":
		sh.encoding = enc{base64: true}
	default:
		fmt.Fprintf(sh.errOut, "kv: unknown encoding %q (want text, hex, or base64)\n", args[0])
	}
}

// doToggle flips a boolean shell setting from an on/off argument, or reports its state when
// no argument is given.
func (sh *shell) doToggle(args []string, flag *bool, name string) {
	if len(args) == 0 {
		fmt.Fprintf(sh.errOut, "%s is %s\n", name, onoff(*flag))
		return
	}
	switch args[0] {
	case "on", "true", "1":
		*flag = true
	case "off", "false", "0":
		*flag = false
	default:
		fmt.Fprintf(sh.errOut, "kv: usage: .%s on|off\n", name)
	}
}

func (sh *shell) doBegin() {
	if sh.txn != nil {
		fmt.Fprintln(sh.errOut, "kv: a transaction is already open (.commit or .rollback first)")
		return
	}
	sh.txn = sh.db.Begin(true)
	fmt.Fprintln(sh.errOut, "transaction started")
}

func (sh *shell) doCommit() {
	if sh.txn == nil {
		fmt.Fprintln(sh.errOut, "kv: no transaction is open")
		return
	}
	err := sh.txn.Commit()
	sh.txn = nil
	if err != nil {
		fmt.Fprintf(sh.errOut, "kv: %v\n", err)
		return
	}
	fmt.Fprintln(sh.errOut, "committed")
}

func (sh *shell) doRollback() {
	if sh.txn == nil {
		fmt.Fprintln(sh.errOut, "kv: no transaction is open")
		return
	}
	sh.txn.Discard()
	sh.txn = nil
	fmt.Fprintln(sh.errOut, "rolled back")
}

func (sh *shell) help() {
	const text = `Data commands (operate on the open file):
  get <key>                 print the value for a key
  set <key> <value>         upsert a key to a value
  del <key>                 delete a key
  del-range <lo> <hi>       range-delete [lo, hi)
  exists <key>              print true or false
  merge <key> <operand>     record a merge operand
  scan [--prefix P | --from LO --to HI] [--reverse] [--limit N] [--keys-only]
  count [--prefix P | --from LO --to HI] [--limit N]
  (all accept --hex or --base64 for binary keys and values)

Meta-commands:
  .info                     print a summary of the database
  .stats                    print space and durability accounting as JSON
  .metrics                  print observability metrics in Prometheus text format
  .check                    verify structural integrity
  .checkpoint               fold the WAL into the main file
  .vacuum [N]               return up to N trailing free pages to the OS (0 = all)
  .pragma <name>[=<value>]  read or set a configuration knob (.pragma help lists them)
  .format <fmt>             set scan output: table, jsonl, json, raw, auto
  .encoding <enc>           set default key/value encoding: text, hex, base64
  .timer [on|off]           toggle per-statement timing
  .begin / .commit / .rollback   explicit multi-statement transaction
  .help                     show this help
  .exit                     leave the shell`
	fmt.Fprintln(sh.errOut, text)
}

// onoff renders a boolean as the on/off words the shell prints for toggles.
func onoff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

// tokenize splits a statement into words, honoring single and double quotes so a value
// with spaces can be passed as one argument (e.g. set k 'a b c'). Quotes group; they do
// not nest and do not process escapes, which is enough for shell-style data entry.
func tokenize(line string) ([]string, error) {
	var toks []string
	var cur strings.Builder
	inTok := false
	for i := 0; i < len(line); {
		c := line[i]
		switch {
		case c == ' ' || c == '\t':
			if inTok {
				toks = append(toks, cur.String())
				cur.Reset()
				inTok = false
			}
			i++
		case c == '\'' || c == '"':
			inTok = true
			quote := c
			i++
			for i < len(line) && line[i] != quote {
				cur.WriteByte(line[i])
				i++
			}
			if i >= len(line) {
				return nil, fmt.Errorf("unterminated %c quote", quote)
			}
			i++ // consume the closing quote
		default:
			inTok = true
			cur.WriteByte(c)
			i++
		}
	}
	if inTok {
		toks = append(toks, cur.String())
	}
	return toks, nil
}
