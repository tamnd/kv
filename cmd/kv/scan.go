package main

import (
	"flag"
	"os"

	"github.com/tamnd/kv"
)

// scanSpec captures the shared bound flags scan and count both parse, so the two
// commands describe the same key window the same way.
type scanSpec struct {
	prefix  string
	from    string
	to      string
	reverse bool
	limit   int
}

// bindScanFlags registers --prefix/--from/--to/--reverse/--limit on a flag set.
func bindScanFlags(fs *flag.FlagSet) *scanSpec {
	s := &scanSpec{}
	fs.StringVar(&s.prefix, "prefix", "", "restrict to keys with this prefix")
	fs.StringVar(&s.from, "from", "", "lower bound, inclusive (start of range)")
	fs.StringVar(&s.to, "to", "", "upper bound, exclusive (end of range)")
	fs.BoolVar(&s.reverse, "reverse", false, "iterate high to low")
	fs.IntVar(&s.limit, "limit", 0, "stop after this many keys (0 = no limit)")
	return s
}

// options builds the kv.IterOptions for a scan, decoding the bound strings per the
// active encoding.
func (s *scanSpec) options(e *enc, keysOnly bool) (kv.IterOptions, error) {
	var opts kv.IterOptions
	opts.Reverse = s.reverse
	opts.KeysOnly = keysOnly
	if s.prefix != "" {
		p, err := e.decode(s.prefix)
		if err != nil {
			return opts, err
		}
		opts.Prefix = p
	}
	if s.from != "" {
		lo, err := e.decode(s.from)
		if err != nil {
			return opts, err
		}
		opts.Lower = lo
	}
	if s.to != "" {
		hi, err := e.decode(s.to)
		if err != nil {
			return opts, err
		}
		opts.Upper = hi
	}
	return opts, nil
}

// cmdScan streams keys (and values) in a range or prefix in the chosen output format.
func cmdScan(args []string) int {
	fs := flag.NewFlagSet("scan", flag.ContinueOnError)
	e := encFlags(fs)
	spec := bindScanFlags(fs)
	keysOnly := fs.Bool("keys-only", false, "print keys only, skip values")
	format := fs.String("f", "auto", "output format: auto, table, jsonl, json, raw")
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv scan <db> [--prefix P | --from LO --to HI] [--reverse] [--limit N] [--keys-only] [-f fmt]")
	}
	outFmt, err := parseFormat(*format)
	if err != nil {
		return usageErr("%v", err)
	}
	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()

	opts, err := spec.options(e, *keysOnly)
	if err != nil {
		return usageErr("bad bound: %v", err)
	}

	w := newRecordWriter(os.Stdout, outFmt.resolve(os.Stdout), e, *keysOnly)
	scanErr := d.View(func(txn *kv.Txn) error {
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
		return fail(scanErr)
	}
	if err := w.close(); err != nil {
		return fail(err)
	}
	return exitOK
}

// cmdCount prints the number of keys in a range or prefix, never materializing values.
func cmdCount(args []string) int {
	fs := flag.NewFlagSet("count", flag.ContinueOnError)
	e := encFlags(fs)
	spec := bindScanFlags(fs)
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv count <db> [--prefix P | --from LO --to HI] [--limit N]")
	}
	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()

	opts, err := spec.options(e, true)
	if err != nil {
		return usageErr("bad bound: %v", err)
	}

	var count int
	if err := d.View(func(txn *kv.Txn) error {
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
		return fail(err)
	}
	os.Stdout.WriteString(itoa(count) + "\n")
	return exitOK
}

// itoa formats a non-negative int without pulling strconv into this file's imports for
// one call; small and allocation-free for the common case.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
