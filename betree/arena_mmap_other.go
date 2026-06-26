//go:build !unix

package betree

// mmapAnon has no anonymous-mmap primitive on this platform through the stdlib alone, so it falls
// back to a plain Go byte slice. The fallback keeps the pointer-free property the arena depends on (a
// slice of bytes is a noscan allocation the GC does not walk for pointers), so the manual-reclamation
// and integer-offset discipline are unchanged. What it loses is the escape from the heap-size
// multiplier: a heap slice still counts against the GC goal, so an arena on this path does not remove
// the GOGC overhead the off-heap path removes. The returned bool is false to mark this as not real
// off-heap memory, so a diagnostic can tell the two apart.
func mmapAnon(size int) ([]byte, bool, error) {
	return make([]byte, size), false, nil
}

// munmapAnon drops the slice for the garbage collector; there is nothing to unmap. It is a no-op so
// the arena's close path is identical on both backings.
func munmapAnon(b []byte) error { return nil }
