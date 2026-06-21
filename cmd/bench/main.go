// Command bench runs the in-repo benchmark suite across both engines and publishes the
// results: a machine-readable JSON report and a human-readable Markdown document with the
// spec 21 §6 targets and the B-tree/LSM tradeoff evaluated against the measured numbers. It
// is the M5 exit artifact (spec 24): the published benchmark that demonstrates the tradeoff
// rather than asserting it.
//
// Usage:
//
//	go run ./cmd/bench -keys 50000 -ops 50000 -out bench/results -label "ref run"
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tamnd/kv"
	"github.com/tamnd/kv/bench"
)

func main() {
	keys := flag.Int("keys", 50000, "number of distinct keys the load phase writes")
	ops := flag.Int("ops", 50000, "number of measured operations in the run phase")
	conc := flag.Int("concurrency", 1, "goroutines driving the run phase")
	seed := flag.Int64("seed", 1, "PRNG seed for a reproducible draw sequence")
	out := flag.String("out", "bench/results", "directory the JSON and Markdown are written to")
	label := flag.String("label", "", "free-form provenance stamped into the report")
	flag.Parse()

	if err := run(*keys, *ops, *conc, *seed, *out, *label); err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func run(keys, ops, conc int, seed int64, out, label string) error {
	work, err := os.MkdirTemp("", "kv-bench-*")
	if err != nil {
		return fmt.Errorf("scratch dir: %w", err)
	}
	defer os.RemoveAll(work)

	tmpl := bench.DefaultConfig(kv.BTree, work)
	tmpl.KeyCount = keys
	tmpl.Ops = ops
	tmpl.Concurrency = conc
	tmpl.Seed = seed

	fmt.Fprintf(os.Stderr, "running suite: %d keys, %d ops, concurrency %d, both engines...\n", keys, ops, conc)
	rep, err := bench.RunSuite(tmpl, []kv.EngineKind{kv.BTree, kv.LSM}, bench.Standard())
	if err != nil {
		return fmt.Errorf("run suite: %w", err)
	}
	rep.Label = label

	if err := os.MkdirAll(out, 0o755); err != nil {
		return fmt.Errorf("out dir %s: %w", out, err)
	}
	jsonPath := filepath.Join(out, "suite.json")
	if err := rep.WriteJSON(jsonPath); err != nil {
		return fmt.Errorf("write json: %w", err)
	}
	mdPath := filepath.Join(out, "RESULTS.md")
	if err := os.WriteFile(mdPath, []byte(bench.RenderReport(rep)), 0o644); err != nil {
		return fmt.Errorf("write markdown: %w", err)
	}

	fmt.Fprintf(os.Stderr, "wrote %s and %s\n", jsonPath, mdPath)
	for _, f := range bench.Tradeoff(rep) {
		mark := "ok"
		if !f.Holds {
			mark = "NO"
		}
		fmt.Fprintf(os.Stderr, "  [%s] %s: %s\n", mark, f.Target, f.Observed)
	}
	return nil
}
