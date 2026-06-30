// Package commit is a frozen experiment: how should the write path make records durable?
// An append writes its bytes to the file, but the bytes are not on the platter until an
// fsync returns, and fsync is the expensive syscall, milliseconds on a spinning disk and
// tens of microseconds on an NVMe, orders of magnitude past the write itself. So the real
// question is not whether to fsync but how often: once per append, which is safe but pays
// the syscall on every write, or once per batch of appends, which amortizes the one fsync
// over many records at the cost of a bounded loss window.
//
// The three candidates bracket the choice. SyncEach fsyncs after every append, the safe
// ceiling on cost. NoSync never fsyncs, the throughput ceiling and the floor on safety,
// kept only to show how much fsync costs. GroupCommit fsyncs once per batchN appends, the
// real engine's policy: under load the flusher writes many committed records and one fsync
// covers them all, so the per-append fsync cost falls by the batch size while the loss
// window stays bounded to one batch.
//
// The board (impl note 181) is what settles the batch size: GroupCommit interpolates between
// the two ceilings as batchN grows, and the knee, where a larger batch stops buying much, is
// where the engine sits. The engine carries group commit at the flush-batch granularity, so
// the batch is whatever the flusher drained since the last fsync, which is large under load
// and one record when idle, exactly the right shape.
package commit

import (
	"os"
	"path/filepath"
	"sync/atomic"
)

// SyncEach fsyncs after every append: durable the instant Append returns, at the cost of one
// fsync syscall per record. This is the safety ceiling and the throughput floor.
type SyncEach struct {
	f   *os.File
	off int64
}

func openFile(dir, name string) (*os.File, error) {
	return os.Create(filepath.Join(dir, name))
}

func NewSyncEach(dir string) (*SyncEach, error) {
	f, err := openFile(dir, "synceach.log")
	if err != nil {
		return nil, err
	}
	return &SyncEach{f: f}, nil
}

func (s *SyncEach) Append(rec []byte) {
	s.f.WriteAt(rec, s.off)
	s.off += int64(len(rec))
	s.f.Sync()
}

func (s *SyncEach) Close() error { return s.f.Close() }

// NoSync writes but never fsyncs: the throughput ceiling and the safety floor, since a crash
// loses everything the OS has not flushed on its own. Kept only to bound how much fsync costs.
type NoSync struct {
	f   *os.File
	off int64
}

func NewNoSync(dir string) (*NoSync, error) {
	f, err := openFile(dir, "nosync.log")
	if err != nil {
		return nil, err
	}
	return &NoSync{f: f}, nil
}

func (n *NoSync) Append(rec []byte) {
	n.f.WriteAt(rec, n.off)
	n.off += int64(len(rec))
}

func (n *NoSync) Close() error { return n.f.Close() }

// GroupCommit is the winner the engine carries: it fsyncs once every batchN appends, so the
// fsync cost is shared across the batch and the loss window is bounded to at most batchN
// records. batchN of one degenerates to SyncEach; a large batchN approaches NoSync. The engine
// uses the flush batch as the natural group, so batchN tracks load instead of being fixed.
type GroupCommit struct {
	f       *os.File
	off     int64
	batchN  int64
	pending int64
	synced  atomic.Int64 // records fsynced so far, the durability watermark
	written int64
}

func NewGroupCommit(dir string, batchN int64) (*GroupCommit, error) {
	f, err := openFile(dir, "groupcommit.log")
	if err != nil {
		return nil, err
	}
	if batchN < 1 {
		batchN = 1
	}
	return &GroupCommit{f: f, batchN: batchN}, nil
}

func (g *GroupCommit) Append(rec []byte) {
	g.f.WriteAt(rec, g.off)
	g.off += int64(len(rec))
	g.written++
	g.pending++
	if g.pending >= g.batchN {
		g.f.Sync()
		g.synced.Store(g.written)
		g.pending = 0
	}
}

// Synced reports how many records are fsynced, the durability watermark a reader trusts.
func (g *GroupCommit) Synced() int64 { return g.synced.Load() }

func (g *GroupCommit) Close() error {
	g.f.Sync()
	return g.f.Close()
}
