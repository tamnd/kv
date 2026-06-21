package bench

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
)

// blockProfileRate and mutexProfileFraction set how densely the runtime samples blocking and
// mutex contention while a profile is enabled. One-in-one is the finest setting: in a short
// measured window the harness wants every contention event, not a sparse sample, since the
// point is to find the one hot lock a regression introduced (spec 21 §5). The cost is paid
// only when profiling is on; the regression gate and the microbenchmarks leave it off.
const (
	blockProfileRate     = 1
	mutexProfileFraction = 1
)

// ProfileSet selects which pprof profiles to capture around a run's measured window. Its zero
// value (empty Dir) disables profiling entirely, which is the default the regression gate and
// the microbenchmarks use, because profiling perturbs the very timings they measure. Set Dir
// and one or more flags to diagnose a regression: a CPU profile for where time goes, an
// allocation profile to catch a hot-path allocation the pool discipline (spec 20 §3) was
// meant to prevent, and block/mutex profiles for contention.
type ProfileSet struct {
	// Dir is the directory profiles are written into; empty disables all profiling. The
	// harness creates it. Files are named for the run so a suite writing many cells into one
	// directory does not clobber its own profiles.
	Dir string
	// CPU captures a CPU profile over the measured window.
	CPU bool
	// Heap captures the allocation profile (allocs), the cumulative bytes and objects
	// allocated, which is what surfaces a hot-path allocation.
	Heap bool
	// Block captures the blocking profile (time goroutines spend off-CPU waiting).
	Block bool
	// Mutex captures the mutex contention profile.
	Mutex bool
}

// enabled reports whether any profile is requested.
func (s ProfileSet) enabled() bool { return s.Dir != "" }

// profileSession is an in-flight capture: the CPU profile is streaming to its file while the
// window runs, and the sampled profiles are written when it stops.
type profileSession struct {
	set     ProfileSet
	label   string
	cpuFile *os.File
}

// startProfiling begins capturing the profiles ProfileSet selects, labelled so a suite's cells
// do not overwrite each other. Block and mutex sampling are enabled here, at the start of the
// measured window, and left off before it, so the samples accrue only over the window and not
// over the unmeasured load that preceded it. A disabled set returns a no-op session.
func startProfiling(set ProfileSet, label string) (*profileSession, error) {
	s := &profileSession{set: set, label: label}
	if !set.enabled() {
		return s, nil
	}
	if err := os.MkdirAll(set.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("profile dir %s: %w", set.Dir, err)
	}
	if set.Block {
		runtime.SetBlockProfileRate(blockProfileRate)
	}
	if set.Mutex {
		runtime.SetMutexProfileFraction(mutexProfileFraction)
	}
	if set.CPU {
		f, err := os.Create(s.path("cpu"))
		if err != nil {
			return nil, fmt.Errorf("create cpu profile: %w", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			f.Close()
			return nil, fmt.Errorf("start cpu profile: %w", err)
		}
		s.cpuFile = f
	}
	return s, nil
}

// stop ends the capture and writes every sampled profile, then disables block and mutex
// sampling again so a later unprofiled run pays nothing. It returns the first write error.
func (s *profileSession) stop() error {
	if !s.set.enabled() {
		return nil
	}
	if s.cpuFile != nil {
		pprof.StopCPUProfile()
		if err := s.cpuFile.Close(); err != nil {
			return fmt.Errorf("close cpu profile: %w", err)
		}
	}
	if s.set.Heap {
		// A GC before the snapshot settles the allocation accounting so the numbers reflect the
		// window rather than whatever the collector had not yet caught up on.
		runtime.GC()
		if err := s.writeLookup("allocs"); err != nil {
			return err
		}
	}
	if s.set.Block {
		if err := s.writeLookup("block"); err != nil {
			return err
		}
		runtime.SetBlockProfileRate(0)
	}
	if s.set.Mutex {
		if err := s.writeLookup("mutex"); err != nil {
			return err
		}
		runtime.SetMutexProfileFraction(0)
	}
	return nil
}

// writeLookup writes one named runtime profile to its file in the pprof binary format, the
// gzip-compressed protobuf that go tool pprof and the flamegraph tooling read.
func (s *profileSession) writeLookup(name string) error {
	p := pprof.Lookup(name)
	if p == nil {
		return fmt.Errorf("no %s profile registered", name)
	}
	f, err := os.Create(s.path(name))
	if err != nil {
		return fmt.Errorf("create %s profile: %w", name, err)
	}
	if err := p.WriteTo(f, 0); err != nil {
		f.Close()
		return fmt.Errorf("write %s profile: %w", name, err)
	}
	return f.Close()
}

// path is the file a profile kind is written to, prefixed with the run's label so the cells of
// a suite run do not collide in a shared directory.
func (s *profileSession) path(kind string) string {
	name := kind + ".pprof"
	if s.label != "" {
		name = s.label + "." + kind + ".pprof"
	}
	return filepath.Join(s.set.Dir, name)
}
