package vfs

import (
	"fmt"
	"io"
	"os"
	"sync"
)

// Mem is an in-memory FS backend (spec 03 §5, 23). It is fast and deterministic,
// and it can simulate crashes: a Crash snapshot drops every byte that was
// written but not yet flushed past a Sync, modelling power loss. The WAL,
// recovery, and checkpoint code run against it unchanged, which is what makes
// crash testing cheap.
type Mem struct {
	mu    sync.Mutex
	files map[string]*memData
	shm   *shmStore
	// faultAfter, when >0, makes the Nth (and later) Sync calls fail, modelling
	// fsyncgate. 0 disables fault injection.
	faultAfter int
	syncs      int
	// freezeAt, when >0, captures the durable image of every file the moment the
	// freezeAt-th Sync completes; a later Crash reverts to that frozen image
	// instead of the latest durable bytes. This pins a crash to an exact fsync
	// boundary so a single continuous workload can be crashed at every sync point.
	freezeAt int
	frozen   map[string][]byte
}

// NewMem returns an empty in-memory filesystem.
func NewMem() *Mem {
	return &Mem{files: map[string]*memData{}, shm: newShmStore()}
}

// memData is the durable+volatile byte image of one file. durable holds bytes
// confirmed by a Sync; live holds the current contents. A simulated crash resets
// live to durable.
type memData struct {
	mu      sync.Mutex
	live    []byte
	durable []byte
}

// Open implements FS.
func (m *Mem) Open(path string, flags OpenFlags) (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.files[path]
	if !ok {
		if flags&OpenCreate == 0 {
			return nil, &os.PathError{Op: "open", Path: path, Err: os.ErrNotExist}
		}
		d = &memData{}
		m.files[path] = d
	} else if flags&OpenExclusive != 0 && flags&OpenCreate != 0 {
		return nil, &os.PathError{Op: "open", Path: path, Err: os.ErrExist}
	}
	return &memFile{fs: m, path: path, data: d}, nil
}

// Delete implements FS.
func (m *Mem) Delete(path string, syncDir bool) error {
	m.mu.Lock()
	delete(m.files, path)
	m.mu.Unlock()
	m.shm.drop(path)
	return nil
}

// Exists implements FS.
func (m *Mem) Exists(path string) (bool, error) {
	m.mu.Lock()
	_, ok := m.files[path]
	m.mu.Unlock()
	return ok, nil
}

// ShmMap implements FS using this instance's private region store.
func (m *Mem) ShmMap(path string, region int, create bool) ([]byte, error) {
	return m.shm.get(path, region, create)
}

// SetSyncFault makes the nth Sync (1-based) and every later Sync return an
// error, simulating an fsync failure. Pass 0 to disable.
func (m *Mem) SetSyncFault(nth int) {
	m.mu.Lock()
	m.faultAfter = nth
	m.syncs = 0
	m.mu.Unlock()
}

// CrashAfterSync arms the filesystem so the durable image of every file is frozen the
// moment the nth Sync (1-based, counted since open) completes. A later Crash then reverts
// to that frozen snapshot rather than the latest durable bytes, which lets a test crash a
// single continuous workload at an exact fsync boundary and prove the durable-prefix
// property (spec 08 §8, spec 23 §4). Syncs past n still succeed and advance the durable
// image, but they do not move the frozen point. Pass 0 to disable. A file created after the
// freeze point is absent (empty) in the snapshot, exactly as it would be after a crash at
// that point.
func (m *Mem) CrashAfterSync(n int) {
	m.mu.Lock()
	m.freezeAt = n
	m.frozen = nil
	m.mu.Unlock()
}

// SyncCount reports how many Sync calls have succeeded since open, so a crash harness can
// learn a workload's sync boundaries before sweeping a crash across each one.
func (m *Mem) SyncCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.syncs
}

// maybeFreeze snapshots the durable image of every file when the freeze boundary is hit.
// It runs after the nth Sync has updated its file's durable bytes, so the snapshot captures
// the exact committed-prefix state visible right after that fsync.
func (m *Mem) maybeFreeze(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.freezeAt <= 0 || n != m.freezeAt || m.frozen != nil {
		return
	}
	m.frozen = make(map[string][]byte, len(m.files))
	for path, d := range m.files {
		d.mu.Lock()
		m.frozen[path] = append([]byte(nil), d.durable...)
		d.mu.Unlock()
	}
}

// Crash discards all unsynced writes across every file, modelling a power
// failure: each file reverts to the bytes last made durable by a Sync. When a
// freeze point was armed with CrashAfterSync, each file reverts to its frozen
// snapshot instead. Shared memory (the wal-index) is also dropped, as it would
// be after a process exit.
func (m *Mem) Crash() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for path, d := range m.files {
		d.mu.Lock()
		if m.frozen != nil {
			d.live = append([]byte(nil), m.frozen[path]...)
		} else {
			d.live = append([]byte(nil), d.durable...)
		}
		d.mu.Unlock()
		m.shm.drop(path)
	}
}

type memFile struct {
	fs   *Mem
	path string
	data *memData
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	f.data.mu.Lock()
	defer f.data.mu.Unlock()
	if off >= int64(len(f.data.live)) {
		return 0, io.EOF
	}
	n := copy(p, f.data.live[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) WriteAt(p []byte, off int64) (int, error) {
	f.data.mu.Lock()
	defer f.data.mu.Unlock()
	end := int(off) + len(p)
	if end > len(f.data.live) {
		grown := make([]byte, end)
		copy(grown, f.data.live)
		f.data.live = grown
	}
	copy(f.data.live[off:], p)
	return len(p), nil
}

func (f *memFile) Sync(mode SyncMode) error {
	f.fs.mu.Lock()
	f.fs.syncs++
	n := f.fs.syncs
	fault := f.fs.faultAfter > 0 && n >= f.fs.faultAfter
	f.fs.mu.Unlock()
	if fault {
		return fmt.Errorf("kv/vfs: injected sync fault on %q", f.path)
	}
	f.data.mu.Lock()
	f.data.durable = append([]byte(nil), f.data.live...)
	f.data.mu.Unlock()
	f.fs.maybeFreeze(n)
	return nil
}

func (f *memFile) Truncate(size int64) error {
	f.data.mu.Lock()
	defer f.data.mu.Unlock()
	if size < int64(len(f.data.live)) {
		f.data.live = f.data.live[:size]
	} else {
		grown := make([]byte, size)
		copy(grown, f.data.live)
		f.data.live = grown
	}
	return nil
}

func (f *memFile) Size() (int64, error) {
	f.data.mu.Lock()
	defer f.data.mu.Unlock()
	return int64(len(f.data.live)), nil
}

func (f *memFile) Lock(level LockLevel) error   { return nil }
func (f *memFile) Unlock(level LockLevel) error { return nil }
func (f *memFile) Close() error                 { return nil }
