package engine

// This file defines the structural-verification contract a storage core implements so
// `kv check` can confirm a real file is sound (spec 16 §4, spec 23 §3). Verification is
// engine-specific: a B-tree's invariants (balanced, ordered, every page reachable once)
// are not an LSM's (sorted non-overlapping runs, a valid MANIFEST), so the walk lives in
// the core. The host drives it under a read lock and reports the findings; the seam keeps
// the verifier, like every other engine concern, behind the four-verb boundary plus this
// one optional capability.

// VerifyProblem is one structural violation a Verify walk found. Class groups it into a
// corruption class so a caller can summarize what kind of damage a file has; Page is the
// offending page (zero when the problem is file-wide, like a space-accounting mismatch);
// Detail is a human-readable description.
type VerifyProblem struct {
	// Class is the corruption class, one of a small fixed vocabulary (spec 23 §3):
	// "structure" (bad page type or out-of-range child pointer), "order" (keys or
	// separators not strictly ascending), "bounds" (a key outside the subtree its
	// parent routes to it), "freelist" (a malformed freelist), "double-alloc" (a page
	// both reachable and free), or "space" (the page accounting does not balance).
	Class string
	// Page is the page the problem was found on, or zero for a file-wide problem.
	Page uint32
	// Detail is a human-readable description of the specific violation.
	Detail string
}

// VerifyReport is the outcome of a structural walk: what was inspected and every problem
// found. An empty Problems slice means the file is structurally sound to the extent the
// current format lets the verifier check (per-page checksum and AEAD verification activate
// when those features land; today the walk covers structure, ordering, and accounting).
type VerifyReport struct {
	// PagesVisited is how many pages the walk reached from the engine root.
	PagesVisited int
	// Keys is how many live key cells the walk saw across all leaves.
	Keys int64
	// FreePages is the freelist depth at the time of the walk.
	FreePages int
	// PageCount is the file's high-water page count.
	PageCount uint32
	// Problems is every violation found, in discovery order; empty means sound.
	Problems []VerifyProblem
}

// OK reports whether the walk found no problems.
func (r *VerifyReport) OK() bool { return len(r.Problems) == 0 }

// Add appends a problem to the report. Cores call it as they walk so a single run
// surfaces every violation rather than stopping at the first.
func (r *VerifyReport) Add(class string, page uint32, detail string) {
	r.Problems = append(r.Problems, VerifyProblem{Class: class, Page: page, Detail: detail})
}

// Verifier is the optional structural self-check a storage core implements (spec 23 §3).
// The host type-asserts the engine to it; a core that does not implement it reports that
// verification is unsupported rather than silently passing. Verify returns an error only
// for an I/O failure that prevents the walk; structural violations are problems in the
// report, not errors, so a corrupt file is fully diagnosed in one pass.
type Verifier interface {
	Verify() (*VerifyReport, error)
}
