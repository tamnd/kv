package betree

// This file is the second slice of milestone M7, the cross-shard ordered scan (doc 05 section 6, D9).
// The partition function (partition.go) sends each key to one shard, which means adjacent keys land in
// different shards under hash partitioning by design. So a scan that wants keys in global sorted order
// cannot read one shard and be done: it has to read every shard's portion of the range, each already in
// order, and merge them back into one global order. That merge is what this file does.
//
// The merge is the cost sharding imposes on the scan path, and naming it honestly is part of the
// milestone. A single-structure scan streams one contiguous run at memory bandwidth; the sharded scan
// instead touches the head of N shard views and emits the global minimum at each step, which adds a
// log(N) comparison per emitted key and loses some of the pure-streaming property. Range partitioning
// buys this back for a scan confined to one band (that scan is single-shard and needs no merge), which
// is the reason both partitioners are offered; hash partitioning pays the merge on every ordered scan
// in exchange for even write spread. doc 07 holds the per-workload numbers to this, the ycsb-e
// short-scan tax in particular.
//
// What the merge operates on, and why it is a slice merge rather than a live cursor heap. The doc draws
// the merge as a heap over N live shard cursors, each the zero-copy block cursor from doc 04, pulling
// the next key from each as it emits. On the current substrate a read already resolves its range into a
// materialized sorted view (snapshotRange returns a []resolved; the streaming zero-copy cursor that
// would let the merge pull lazily is deferred to the same M6 arena integration that defers the
// single-domain zero-copy cursor), so a shard's "cursor" here is its resolved view, and the merge is a
// k-way merge of N sorted views into one. The heap is the same heap the doc names, sized to the shard
// count, and the result is a single sorted view the existing index-walking cursor walks in either
// direction, so the sharded reader gets full bidirectional iteration for free over the merged order.
// When the streaming cursor lands, this merge becomes a merge over live cursors with the identical
// heap; the ordering contract it proves now does not change.
//
// The correctness that makes the merge a clean interleave. Routing is by key, so every user key lives
// in exactly one shard: the shard views are disjoint, no user key appears in two of them. That means
// the merge never has to resolve the same key from two sources (which value wins) the way an LSM level
// merge does; it only has to interleave disjoint sorted runs into one sorted run. The invariant the
// gate asserts is exactly this: merging the per-shard partition of a single-domain sorted view
// reproduces that view, key for key and value for value.

// mergeShardViews performs a k-way ordered merge of the per-shard resolved views into one globally
// sorted view. Each input view must already be sorted ascending by user key (a shard's snapshotRange
// returns it that way), and the views are disjoint by user key (routing sends each key to one shard).
// The result is the union in ascending user-key order. It uses a binary min-heap over the view heads,
// so emitting a key costs one log(N) sift rather than an N-way linear scan, the cost the doc names.
//
// The merge copies nothing: each emitted resolved is the same struct (sharing the same key and value
// byte slices) the shard view held, since the shard view already owns caller-safe copies. A nil or
// empty view contributes nothing. Ties on equal user key cannot arise from real shard views because
// they are disjoint, but the heap breaks any tie by lower view index so the merge is deterministic even
// if a caller feeds overlapping views.
func mergeShardViews(views [][]resolved) []resolved {
	// Count the total so the result is allocated once, and seed the heap with the head of every
	// non-empty view. The heap holds view indices; pos tracks how far each view has been consumed.
	total := 0
	for _, v := range views {
		total += len(v)
	}
	if total == 0 {
		return nil
	}

	h := &viewHeap{views: views, pos: make([]int, len(views))}
	for i, v := range views {
		if len(v) > 0 {
			h.idx = append(h.idx, i)
		}
	}
	h.init()

	out := make([]resolved, 0, total)
	for len(h.idx) > 0 {
		// The heap root is the view whose current head is the global minimum. Emit it, advance that
		// view, and either re-sift the root to its new head or drop the view if it is exhausted.
		top := h.idx[0]
		out = append(out, views[top][h.pos[top]])
		h.pos[top]++
		if h.pos[top] < len(views[top]) {
			h.down(0)
		} else {
			h.removeRoot()
		}
	}
	return out
}

// viewHeap is a binary min-heap over view indices, ordered by each view's current head user key. It is
// the small heap the cross-shard merge keys on the next key from each cursor. idx is the heap array of
// live view indices; pos[i] is the next unconsumed position in view i; views is the backing data. It is
// hand-rolled rather than reaching for container/heap so the merge carries no interface boxing on the
// hot comparison and stands alone in the package, the same self-contained discipline the rest of the
// core keeps.
type viewHeap struct {
	views [][]resolved
	pos   []int
	idx   []int
}

// less orders two heap slots by the user key at each view's current head, breaking ties by lower view
// index so the merge is deterministic on the disjoint-view contract's edge.
func (h *viewHeap) less(a, b int) bool {
	va, vb := h.idx[a], h.idx[b]
	ka := h.views[va][h.pos[va]].uk
	kb := h.views[vb][h.pos[vb]].uk
	if c := compareBytes(ka, kb); c != 0 {
		return c < 0
	}
	return va < vb
}

func (h *viewHeap) swap(a, b int) { h.idx[a], h.idx[b] = h.idx[b], h.idx[a] }

// init heapifies the seeded idx slice bottom-up.
func (h *viewHeap) init() {
	for i := len(h.idx)/2 - 1; i >= 0; i-- {
		h.down(i)
	}
}

// down sifts the element at i toward the leaves until the heap order holds below it.
func (h *viewHeap) down(i int) {
	n := len(h.idx)
	for {
		l := 2*i + 1
		if l >= n {
			break
		}
		smallest := l
		if r := l + 1; r < n && h.less(r, l) {
			smallest = r
		}
		if !h.less(smallest, i) {
			break
		}
		h.swap(i, smallest)
		i = smallest
	}
}

// removeRoot drops the root view (exhausted) by moving the last live view into the root and sifting it
// down, shrinking the heap by one.
func (h *viewHeap) removeRoot() {
	n := len(h.idx) - 1
	h.swap(0, n)
	h.idx = h.idx[:n]
	if n > 0 {
		h.down(0)
	}
}

// compareBytes is the unsigned lexicographic byte comparison the merge orders by, the same order the
// engine keys in. It returns negative, zero, or positive like bytes.Compare; spelled out here so the
// merge stands alone alongside the partition function's bytesLess.
func compareBytes(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}
