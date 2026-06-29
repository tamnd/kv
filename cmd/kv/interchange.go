package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/tamnd/kv"
)

// cmdImport loads key/value pairs from CSV, TSV, or JSONL into the database (spec 16 §3).
// For CSV/TSV the key is taken from --key-col (default 0) and the value from --val-col
// (default 1). For JSONL the standard {"key":"...","value":"..."} encoding is used, the
// same as kv load. Records are committed in batches of --batch rows.
func cmdImport(args []string) int {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	e := encFlags(fs)
	format := fs.String("format", "jsonl", "input format: csv, tsv, jsonl")
	input := fs.String("input", "-", "read from this file (- for stdin)")
	keyCol := fs.Int("key-col", 0, "CSV/TSV column index for the key (0-based)")
	valCol := fs.Int("val-col", 1, "CSV/TSV column index for the value (0-based)")
	batchSize := fs.Int("batch", 1000, "rows per commit batch (JSONL and CSV/TSV)")
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv import <db> [--format csv|tsv|jsonl] [--input F] [--key-col N] [--val-col N] [--batch N] [--base64]")
	}

	xfmt, err := parseXFormat(*format)
	if err != nil {
		return usageErr("%v", err)
	}
	if *keyCol < 0 {
		return usageErr("--key-col must be >= 0")
	}
	if *valCol < 0 {
		return usageErr("--val-col must be >= 0")
	}
	if *batchSize <= 0 {
		return usageErr("--batch must be positive")
	}

	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()

	var in io.Reader = os.Stdin
	if *input != "-" {
		f, ferr := os.Open(*input)
		if ferr != nil {
			return fail(ferr)
		}
		defer f.Close()
		in = f
	}

	var importErr error
	switch xfmt {
	case xfmtJSONL:
		importErr = importJSONL(d, in, e, *batchSize)
	case xfmtCSV:
		importErr = importDelimited(d, in, e, ',', *keyCol, *valCol, *batchSize)
	case xfmtTSV:
		importErr = importDelimited(d, in, e, '\t', *keyCol, *valCol, *batchSize)
	}

	if importErr != nil {
		if isUsageErr(importErr) {
			return usageErr("%v", importErr)
		}
		return fail(importErr)
	}
	return exitOK
}

// xFormat selects the interchange format for import/export, distinct from outputFormat
// which also includes table/raw/json modes not appropriate for data interchange.
type xFormat int

const (
	xfmtJSONL xFormat = iota
	xfmtCSV
	xfmtTSV
)

func parseXFormat(s string) (xFormat, error) {
	switch s {
	case "", "jsonl":
		return xfmtJSONL, nil
	case "csv":
		return xfmtCSV, nil
	case "tsv":
		return xfmtTSV, nil
	default:
		return xfmtJSONL, fmt.Errorf("unknown interchange format %q (want csv, tsv, or jsonl)", s)
	}
}

// importJSONL reads the standard {"key":"...","value":"..."} JSONL format (same as
// kv load) and loads it through a WriteBatch that auto-flushes every batchSize rows.
func importJSONL(d *kv.DB, r io.Reader, e *enc, batchSize int) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	line := 0
	wb := d.NewWriteBatch(batchSize)
	for sc.Scan() {
		line++
		raw := sc.Bytes()
		if len(raw) == 0 {
			continue
		}
		var rec record
		if err := json.Unmarshal(raw, &rec); err != nil {
			wb.Close() //nolint
			return xUsageErr("line %d: bad JSON: %v", line, err)
		}
		k, err := e.decode(rec.Key)
		if err != nil {
			wb.Close() //nolint
			return xUsageErr("line %d: bad key: %v", line, err)
		}
		v, err := e.decode(rec.Value)
		if err != nil {
			wb.Close() //nolint
			return xUsageErr("line %d: bad value: %v", line, err)
		}
		if err := wb.Set(k, v); err != nil {
			wb.Close() //nolint
			return err
		}
	}
	if err := sc.Err(); err != nil {
		wb.Close() //nolint
		return err
	}
	return wb.Close()
}

// importDelimited reads CSV or TSV rows and commits them through a WriteBatch.
func importDelimited(d *kv.DB, r io.Reader, e *enc, comma rune, keyCol, valCol, batchSize int) error {
	cr := csv.NewReader(r)
	cr.Comma = comma
	cr.FieldsPerRecord = -1 // allow variable-width rows; we validate per row
	cr.LazyQuotes = true

	need := keyCol
	if valCol > need {
		need = valCol
	}

	line := 0
	wb := d.NewWriteBatch(batchSize)
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			wb.Close() //nolint
			return xUsageErr("line %d: %v", line+1, err)
		}
		line++
		if len(row) <= need {
			wb.Close() //nolint
			return xUsageErr("line %d: need column %d but row has only %d column(s)", line, need, len(row))
		}
		k, err := e.decode(row[keyCol])
		if err != nil {
			wb.Close() //nolint
			return xUsageErr("line %d: bad key: %v", line, err)
		}
		v, err := e.decode(row[valCol])
		if err != nil {
			wb.Close() //nolint
			return xUsageErr("line %d: bad value: %v", line, err)
		}
		if err := wb.Set(k, v); err != nil {
			wb.Close() //nolint
			return err
		}
	}
	return wb.Close()
}

// xUsageError marks a parse-level error from bad input (as opposed to a kv failure),
// so the caller can map it to exit 2 rather than exit 5.
type xUsageError struct{ msg string }

func (e xUsageError) Error() string { return e.msg }

func xUsageErr(format string, a ...any) error {
	return xUsageError{fmt.Sprintf(format, a...)}
}

func isUsageErr(err error) bool {
	_, ok := err.(xUsageError)
	return ok
}
