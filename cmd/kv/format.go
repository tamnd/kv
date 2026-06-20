package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
)

// outputFormat selects how a scan/dump renders a stream of records.
type outputFormat int

const (
	// fmtAuto picks table on a TTY and jsonl in a pipe, so the tool is readable
	// interactively and parseable when redirected (spec 16 §3).
	fmtAuto outputFormat = iota
	fmtTable
	fmtJSONL
	fmtJSON
	fmtRaw
)

// parseFormat resolves a -f value to an outputFormat, defaulting to auto.
func parseFormat(s string) (outputFormat, error) {
	switch s {
	case "", "auto":
		return fmtAuto, nil
	case "table":
		return fmtTable, nil
	case "jsonl":
		return fmtJSONL, nil
	case "json":
		return fmtJSON, nil
	case "raw":
		return fmtRaw, nil
	default:
		return fmtAuto, fmt.Errorf("unknown format %q (want table, jsonl, json, or raw)", s)
	}
}

// resolve turns fmtAuto into a concrete format based on whether w is a terminal.
func (f outputFormat) resolve(w *os.File) outputFormat {
	if f != fmtAuto {
		return f
	}
	if isTerminal(w) {
		return fmtTable
	}
	return fmtJSONL
}

// isTerminal reports whether f is a character device (a TTY), without pulling in
// golang.org/x/term, to keep the binary dependency-free (spec 20).
func isTerminal(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

// record is one key/value pair rendered by a writer. Value is nil for a key-only scan.
type record struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// recordWriter renders a stream of key/value pairs in a chosen format. Writers stream:
// they emit each record as it arrives rather than buffering the whole scan, so a dump of
// a large database stays flat in memory (spec 16 §3).
type recordWriter interface {
	write(key, value []byte, keysOnly bool) error
	close() error
}

// newRecordWriter builds the writer for a resolved format. e controls how raw bytes are
// rendered into the string fields.
func newRecordWriter(w io.Writer, f outputFormat, e *enc, keysOnly bool) recordWriter {
	bw := bufio.NewWriter(w)
	switch f {
	case fmtTable:
		return &tableWriter{tw: tabwriter.NewWriter(bw, 0, 4, 2, ' ', 0), bw: bw, enc: e, keysOnly: keysOnly}
	case fmtJSON:
		return &jsonWriter{bw: bw, enc: e}
	case fmtRaw:
		return &rawWriter{bw: bw, enc: e, keysOnly: keysOnly}
	default:
		return &jsonlWriter{bw: bw, enc: e}
	}
}

// jsonlWriter emits one JSON object per line, the pipe-friendly default.
type jsonlWriter struct {
	bw  *bufio.Writer
	enc *enc
}

func (jw *jsonlWriter) write(key, value []byte, keysOnly bool) error {
	r := record{Key: jw.enc.encode(key)}
	if !keysOnly {
		r.Value = jw.enc.encode(value)
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	jw.bw.Write(b)
	return jw.bw.WriteByte('\n')
}

func (jw *jsonlWriter) close() error { return jw.bw.Flush() }

// jsonWriter accumulates the scan into one JSON array. It buffers, so it is meant for
// bounded result sets where a single document is wanted.
type jsonWriter struct {
	bw    *bufio.Writer
	enc   *enc
	began bool
}

func (jw *jsonWriter) write(key, value []byte, keysOnly bool) error {
	if !jw.began {
		jw.bw.WriteString("[")
		jw.began = true
	} else {
		jw.bw.WriteString(",")
	}
	r := record{Key: jw.enc.encode(key)}
	if !keysOnly {
		r.Value = jw.enc.encode(value)
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	jw.bw.Write(b)
	return nil
}

func (jw *jsonWriter) close() error {
	if !jw.began {
		jw.bw.WriteString("[]")
	} else {
		jw.bw.WriteString("]")
	}
	jw.bw.WriteByte('\n')
	return jw.bw.Flush()
}

// tableWriter renders an aligned two-column table for interactive reading.
type tableWriter struct {
	tw       *tabwriter.Writer
	bw       *bufio.Writer
	enc      *enc
	keysOnly bool
	began    bool
}

func (tw *tableWriter) write(key, value []byte, keysOnly bool) error {
	if !tw.began {
		if keysOnly {
			fmt.Fprintln(tw.tw, "KEY")
		} else {
			fmt.Fprintln(tw.tw, "KEY\tVALUE")
		}
		tw.began = true
	}
	if keysOnly {
		fmt.Fprintln(tw.tw, tw.enc.encode(key))
	} else {
		fmt.Fprintf(tw.tw, "%s\t%s\n", tw.enc.encode(key), tw.enc.encode(value))
	}
	return nil
}

func (tw *tableWriter) close() error {
	if err := tw.tw.Flush(); err != nil {
		return err
	}
	return tw.bw.Flush()
}

// rawWriter writes values (or keys for a key-only scan) verbatim with a trailing
// newline, for piping a single column into other tools.
type rawWriter struct {
	bw       *bufio.Writer
	enc      *enc
	keysOnly bool
}

func (rw *rawWriter) write(key, value []byte, keysOnly bool) error {
	if keysOnly {
		rw.bw.WriteString(rw.enc.encode(key))
	} else {
		rw.bw.WriteString(rw.enc.encode(value))
	}
	return rw.bw.WriteByte('\n')
}

func (rw *rawWriter) close() error { return rw.bw.Flush() }
