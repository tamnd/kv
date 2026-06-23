package db

import "github.com/tamnd/kv/latch"

// rlatch is the DB-level distributed read latch (perf/10 R1): the read-mostly replacement for the
// DB's old sync.RWMutex, so concurrent point reads do not serialize on one shared reader-count
// word. The primitive lives in package latch because the pager shard reuses it (perf/10 R2); these
// aliases keep the db call sites reading as rlatch/newRlatch. See latch.RLatch for the contract.
type rlatch = latch.RLatch

func newRlatch() *rlatch { return latch.New() }
