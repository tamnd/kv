package kv

import (
	"fmt"
	"sync"
	"testing"
)

// TestAppendReadBack checks the core round-trip: every Append returns an address that
// reads back the exact bytes written, single-threaded.
func TestAppendReadBack(t *testing.T) {
	l := newRingLog(1 << 20)
	addrs := make([]int64, 1000)
	for i := range addrs {
		rec := fmt.Appendf(nil, "record-%04d-payload", i)
		addrs[i] = l.Append(rec)
	}
	for i, a := range addrs {
		want := fmt.Sprintf("record-%04d-payload", i)
		if got := string(l.At(a)); got != want {
			t.Fatalf("addr %d: got %q want %q", a, got, want)
		}
	}
}

// TestAppendConcurrentDisjoint is the lock-free correctness claim: many goroutines
// appending at once each get a disjoint span, and every record reads back intact with
// no torn or overlapping write. It records each appended address with its expected
// payload, then verifies all of them after the writers join.
func TestAppendConcurrentDisjoint(t *testing.T) {
	l := newRingLog(1 << 24)
	const writers = 8
	const each = 4000

	type rec struct {
		addr int64
		want string
	}
	out := make([][]rec, writers)
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			out[w] = make([]rec, each)
			for i := range each {
				payload := fmt.Sprintf("w%d-i%05d-data", w, i)
				a := l.Append([]byte(payload))
				out[w][i] = rec{addr: a, want: payload}
			}
		}(w)
	}
	wg.Wait()

	seen := make(map[int64]bool, writers*each)
	for w := range writers {
		for _, r := range out[w] {
			if seen[r.addr] {
				t.Fatalf("address %d handed to two appenders", r.addr)
			}
			seen[r.addr] = true
			if got := string(l.At(r.addr)); got != r.want {
				t.Fatalf("addr %d: got %q want %q", r.addr, got, r.want)
			}
		}
	}
	if len(seen) != writers*each {
		t.Fatalf("expected %d distinct records, saw %d", writers*each, len(seen))
	}
}
