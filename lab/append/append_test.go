package append

import "testing"

// Run with -cpu=1,2,4,8 and read the trend across cores, not a single row: the lock-free log
// should stay flat while the mutex climbs once the lock cache line starts bouncing.

var benchRec = []byte("a-typical-value-stands-in-here-for-the-append-benchmark-payload-x")

func BenchmarkAppendLockFree(b *testing.B) {
	l := NewLog(int64(b.N)*int64(hdrLen+len(benchRec)) + 1<<20)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Append(benchRec)
		}
	})
}

func BenchmarkAppendMutex(b *testing.B) {
	l := NewLockedLog(int64(b.N)*int64(hdrLen+len(benchRec)) + 1<<20)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Append(benchRec)
		}
	})
}
