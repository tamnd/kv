package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tamnd/kv"
)

// watchContextOverride, when non-nil, is used by cmdWatch instead of building a
// signal-based context. Tests set it so they can cancel the watch without sending
// OS signals to the whole process.
var watchContextOverride context.Context

// changeRecord is one change event serialized to the JSONL stream.
type changeRecord struct {
	Version uint64 `json:"version"`
	Kind    string `json:"kind"`
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"`
}

// cmdWatch streams the change feed to stdout as JSONL (spec 16 §6). Each committed
// batch that touches a key under --prefix (or all keys when no prefix is given) emits
// one JSON object per mutation:
//
//	{"version":12,"kind":"set","key":"...","value":"..."}
//	{"version":12,"kind":"delete","key":"..."}
//
// The stream continues until the process receives SIGINT or SIGTERM.
func cmdWatch(args []string) int {
	fs := flag.NewFlagSet("watch", flag.ContinueOnError)
	e := encFlags(fs)
	prefix := fs.String("prefix", "", "watch only keys that start with this prefix")
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv watch <db> [--prefix P]")
	}
	path := fs.Arg(0)

	d, code := openDB(path)
	if code != exitOK {
		return code
	}
	defer d.Close()

	var pfx []byte
	if *prefix != "" {
		p, err := e.decode(*prefix)
		if err != nil {
			return usageErr("bad prefix: %v", err)
		}
		pfx = p
	}

	var ctx context.Context
	var cancel context.CancelFunc
	if watchContextOverride != nil {
		ctx = watchContextOverride
		cancel = func() {}
	} else {
		ctx, cancel = signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	}
	defer cancel()

	bw := bufio.NewWriter(os.Stdout)
	err := d.Subscribe(ctx, pfx, func(changes []kv.Change) error {
		for _, c := range changes {
			r := changeRecord{
				Version: c.Version,
				Kind:    changeKindName(c.Kind),
				Key:     e.encode(c.Key),
			}
			if c.Value != nil {
				r.Value = e.encode(c.Value)
			}
			b, err := json.Marshal(r)
			if err != nil {
				return err
			}
			bw.Write(b)
			if err := bw.WriteByte('\n'); err != nil {
				return err
			}
			if err := bw.Flush(); err != nil {
				return err
			}
		}
		return nil
	})

	if errors.Is(err, context.Canceled) {
		return exitOK
	}
	if errors.Is(err, kv.ErrSubscriberLagged) {
		fmt.Fprintln(os.Stderr, "kv watch: subscriber lagged; events were dropped")
		return exitOK
	}
	if err != nil {
		return fail(err)
	}
	return exitOK
}

// changeKindName maps a ChangeKind to its canonical lowercase string.
func changeKindName(k kv.ChangeKind) string {
	switch k {
	case kv.ChangeSet:
		return "set"
	case kv.ChangeDelete:
		return "delete"
	case kv.ChangeMerge:
		return "merge"
	case kv.ChangeRangeDelete:
		return "range-delete"
	default:
		return "unknown"
	}
}
