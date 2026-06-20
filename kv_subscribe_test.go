package kv_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/tamnd/kv"
)

// subscribeAndWait starts a Subscribe on prefix in a goroutine and returns once the
// subscription is provably live, so the caller's later commits are guaranteed to land on
// the feed. A change feed only follows the tail, so a write that races ahead of
// registration is legitimately missed; the helper closes that race deterministically by
// committing a probe key under the prefix until the callback echoes one back, then draining
// the probe echoes so the channel is quiet before the real workload begins.
//
// The returned got channel carries every change the callback receives; errc carries the
// single error Subscribe returns when it ends.
func subscribeAndWait(t *testing.T, kdb *kv.DB, ctx context.Context, prefix string) (<-chan kv.Change, <-chan error) {
	t.Helper()
	got := make(chan kv.Change, 4096)
	errc := make(chan error, 1)
	go func() {
		errc <- kdb.Subscribe(ctx, []byte(prefix), func(batch []kv.Change) error {
			for _, c := range batch {
				got <- c
			}
			return nil
		})
	}()

	probe := prefix + "\x00probe"
	set := func(key, val string) {
		if err := kdb.Update(func(txn *kv.Txn) error { return txn.Set([]byte(key), []byte(val)) }); err != nil {
			t.Fatalf("probe commit: %v", err)
		}
	}
	deadline := time.After(5 * time.Second)
	for {
		set(probe, "x")
		select {
		case c := <-got:
			if string(c.Key) != probe {
				t.Fatalf("probe phase saw unexpected key %q", c.Key)
			}
			// Live. Drain any further probe echoes until the feed goes quiet.
			for {
				select {
				case <-got:
				case <-time.After(100 * time.Millisecond):
					return got, errc
				}
			}
		case <-time.After(10 * time.Millisecond):
		case <-deadline:
			t.Fatal("subscription never went live")
		}
	}
}

// TestSubscribeDeliversCommittedChanges checks the feed surfaces committed point writes in
// commit order, with the right kind, value, and a strictly increasing version per commit.
func TestSubscribeDeliversCommittedChanges(t *testing.T) {
	kdb := open(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got, errc := subscribeAndWait(t, kdb, ctx, "k")

	const n = 5
	for i := 0; i < n; i++ {
		key, val := fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i)
		if err := kdb.Update(func(txn *kv.Txn) error { return txn.Set([]byte(key), []byte(val)) }); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}

	var lastVersion uint64
	for i := 0; i < n; i++ {
		c := recvChange(t, got)
		wantKey, wantVal := fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i)
		if c.Kind != kv.ChangeSet {
			t.Fatalf("change %d kind = %d, want ChangeSet", i, c.Kind)
		}
		if string(c.Key) != wantKey || string(c.Value) != wantVal {
			t.Fatalf("change %d = %q=%q, want %q=%q", i, c.Key, c.Value, wantKey, wantVal)
		}
		if c.Version <= lastVersion {
			t.Fatalf("change %d version %d not greater than previous %d", i, c.Version, lastVersion)
		}
		lastVersion = c.Version
	}

	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe returned %v, want context.Canceled", err)
	}
}

// TestSubscribeDeliversDeletes checks a point delete arrives as a ChangeDelete with no value,
// so a feed consumer can replay a tombstone, not just upserts.
func TestSubscribeDeliversDeletes(t *testing.T) {
	kdb := open(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := kdb.Update(func(txn *kv.Txn) error { return txn.Set([]byte("kgone"), []byte("v")) }); err != nil {
		t.Fatalf("seed: %v", err)
	}

	got, errc := subscribeAndWait(t, kdb, ctx, "k")
	if err := kdb.Update(func(txn *kv.Txn) error { return txn.Delete([]byte("kgone")) }); err != nil {
		t.Fatalf("delete: %v", err)
	}

	c := recvChange(t, got)
	if c.Kind != kv.ChangeDelete {
		t.Fatalf("kind = %d, want ChangeDelete", c.Kind)
	}
	if string(c.Key) != "kgone" || c.Value != nil {
		t.Fatalf("delete change = %q=%q, want kgone with nil value", c.Key, c.Value)
	}

	cancel()
	<-errc
}

// TestSubscribeFiltersByPrefix checks a subscriber only sees keys under its prefix, never a
// sibling write outside it.
func TestSubscribeFiltersByPrefix(t *testing.T) {
	kdb := open(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got, errc := subscribeAndWait(t, kdb, ctx, "alpha:")

	// Interleave a non-matching write between two matching ones; only the matching pair
	// may appear, in order.
	commits := []struct{ key, val string }{
		{"alpha:1", "a1"},
		{"beta:1", "b1"},
		{"alpha:2", "a2"},
	}
	for _, c := range commits {
		key, val := c.key, c.val
		if err := kdb.Update(func(txn *kv.Txn) error { return txn.Set([]byte(key), []byte(val)) }); err != nil {
			t.Fatalf("commit %q: %v", key, err)
		}
	}

	for _, want := range []string{"alpha:1", "alpha:2"} {
		c := recvChange(t, got)
		if string(c.Key) != want {
			t.Fatalf("got key %q, want %q (a non-matching key leaked onto the feed)", c.Key, want)
		}
	}

	cancel()
	<-errc
}

// TestSubscribeStopsOnClose checks closing the database wakes a blocked subscriber promptly
// with ErrClosed, rather than hanging on a commit path that will never run again.
func TestSubscribeStopsOnClose(t *testing.T) {
	kdb := open(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, errc := subscribeAndWait(t, kdb, ctx, "")
	if err := kdb.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	select {
	case err := <-errc:
		if !errors.Is(err, kv.ErrClosed) {
			t.Fatalf("Subscribe returned %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe did not return after Close")
	}
}

// TestSubscribeDropsLaggingConsumer checks a callback that parks while commits pile up past
// the buffer is cut loose with ErrSubscriberLagged, proving a slow consumer never stalls the
// writers. SyncOff keeps the many commits cheap.
func TestSubscribeDropsLaggingConsumer(t *testing.T) {
	kdb := open(t, kv.WithSynchronous(kv.SyncOff))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := make(chan struct{})
	parked := make(chan struct{}, 1)
	errc := make(chan error, 1)
	go func() {
		first := true
		errc <- kdb.Subscribe(ctx, nil, func(batch []kv.Change) error {
			if first {
				first = false
				parked <- struct{}{}
				<-release // hold the feed so its buffer backs up
			}
			return nil
		})
	}()

	// Commit until the callback is parked inside its first batch, so the subscription is
	// provably live and the feed is now stuck.
	isParked := false
	for !isParked {
		if err := kdb.Update(func(txn *kv.Txn) error { return txn.Set([]byte("p"), []byte("x")) }); err != nil {
			t.Fatalf("park commit: %v", err)
		}
		select {
		case <-parked:
			isParked = true
		default:
		}
	}
	// Now flood far past the internal buffer so the non-blocking send must drop the sub.
	for i := 0; i < 4000; i++ {
		if err := kdb.Update(func(txn *kv.Txn) error { return txn.Set([]byte(fmt.Sprintf("q%04d", i)), []byte("x")) }); err != nil {
			t.Fatalf("flood commit %d: %v", i, err)
		}
	}
	close(release)

	select {
	case err := <-errc:
		if !errors.Is(err, kv.ErrSubscriberLagged) {
			t.Fatalf("Subscribe returned %v, want ErrSubscriberLagged", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("lagging Subscribe did not return")
	}
}

// recvChange reads one change from the feed or fails if none arrives promptly.
func recvChange(t *testing.T, got <-chan kv.Change) kv.Change {
	t.Helper()
	select {
	case c := <-got:
		return c
	case <-time.After(2 * time.Second):
		t.Fatal("expected a change on the feed, got none")
		return kv.Change{}
	}
}
