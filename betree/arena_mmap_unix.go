//go:build unix

package betree

import "syscall"

// mmapAnon maps size bytes of anonymous, private, read-write memory through the stdlib syscall
// package (no cgo, no dependency). MAP_ANON gives a region backed by no file, MAP_PRIVATE makes it
// copy-on-write-private to this process, and the kernel hands back zero-filled pages. The region is
// not part of the Go heap: the runtime never scans it, moves it, or counts it against the GC goal,
// which is the whole point of D10. The returned bool is true to mark this as real off-heap memory.
func mmapAnon(size int) ([]byte, bool, error) {
	b, err := syscall.Mmap(-1, 0, size,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// munmapAnon returns a mapped region to the operating system. A nil or empty slice is a no-op, so
// closing an already-closed or fallback arena is safe.
func munmapAnon(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return syscall.Munmap(b)
}
