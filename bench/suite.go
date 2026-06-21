package bench

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/tamnd/kv"
)

// Report is a whole suite run: every workload measured on every engine, gathered into one
// machine-readable document. Spec 21 §5 wants results emitted as JSON "for tracking over
// time"; a Report is that unit, the thing a regression gate diffs one release against the
// next. It carries no wall-clock timestamp of its own so two runs of the same suite on the
// same data produce byte-identical documents save for the measured numbers; a caller that
// wants a timestamp sets Label to carry it.
type Report struct {
	// Label is free-form provenance the caller stamps (a commit, a date, a machine name).
	// It is not interpreted, only carried, so the report stays reproducible by default.
	Label string `json:"label,omitempty"`
	// Results is one entry per (engine, workload), ordered deterministically by engine then
	// workload so two reports line up field for field in a diff.
	Results []Result `json:"results"`
}

// RunSuite runs every workload in workloads on every engine in engines and returns the
// gathered Report. Each run gets its own fresh directory under tmpl.Dir, because the harness
// measures a directory's whole footprint and two runs must not share a file. tmpl supplies
// the sizing (key count, ops, widths, seed, durability); its Engine and Dir are overridden
// per run.
func RunSuite(tmpl Config, engines []kv.EngineKind, workloads []Workload) (Report, error) {
	var rep Report
	for _, e := range engines {
		for _, w := range workloads {
			cfg := tmpl
			cfg.Engine = e
			dir := filepath.Join(tmpl.Dir, engineName(e)+"-"+w.Name)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return Report{}, fmt.Errorf("suite dir %s: %w", dir, err)
			}
			cfg.Dir = dir
			res, err := Run(cfg, w)
			if err != nil {
				return Report{}, fmt.Errorf("run %s/%s: %w", engineName(e), w.Name, err)
			}
			rep.Results = append(rep.Results, res)
		}
	}
	rep.sort()
	return rep, nil
}

// sort orders results by engine then workload so a report is deterministic and two reports
// align row for row when compared.
func (r *Report) sort() {
	sort.Slice(r.Results, func(i, j int) bool {
		if r.Results[i].Engine != r.Results[j].Engine {
			return r.Results[i].Engine < r.Results[j].Engine
		}
		return r.Results[i].Workload < r.Results[j].Workload
	})
}

// JSON renders the report as indented JSON for storage and diffing.
func (r Report) JSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

// WriteJSON writes the report's JSON to path, the durable record a later run compares
// against (spec 21 §5).
func (r Report) WriteJSON(path string) error {
	data, err := r.JSON()
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// ReadReport loads a report previously written by WriteJSON, so a regression gate can pull a
// stored baseline off disk and compare the current run against it.
func ReadReport(path string) (Report, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Report{}, err
	}
	var r Report
	if err := json.Unmarshal(data, &r); err != nil {
		return Report{}, fmt.Errorf("parse report %s: %w", path, err)
	}
	return r, nil
}
