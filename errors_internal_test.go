package kv

import (
	"errors"
	"fmt"
	"testing"

	"github.com/tamnd/kv/db"
)

// TestWrapMapsFatalSyncToNeedsRecovery pins the public contract that a fatal WAL
// durability fault surfaces as ErrNeedsRecovery (spec 07 §6), the sentinel a caller
// branches on to know it must reopen. The fence is produced deep in the db layer where a
// public fault cannot be injected (Open hard-wires the OS filesystem), so the mapping is
// verified directly on wrap, including the wrapped-with-context form the db layer returns.
func TestWrapMapsFatalSyncToNeedsRecovery(t *testing.T) {
	wrapped := fmt.Errorf("%w: injected sync fault", db.ErrFatalSync)
	if got := wrap(wrapped); !errors.Is(got, ErrNeedsRecovery) {
		t.Fatalf("wrap(%v) = %v, want ErrNeedsRecovery", wrapped, got)
	}
}
