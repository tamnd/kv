package format

// Op is one version of a single user key, as the MVCC fold sees it. A key's ops
// are consumed newest-first (version descending), which is the order the internal
// key encoding already lays them out in. Only set, delete, and merge versions
// belong here; range-delete markers are resolved through RangeDel, not as ops.
type Op struct {
	Version uint64
	Kind    Kind
	Value   []byte
}

// RangeDel is a committed range deletion [Lo, Hi) stamped at Version: every user
// key in [Lo, Hi) whose newest visible version is older than Version reads as
// absent. A range delete is stored as a single marker cell at Lo (kind
// KindRangeBegin, with Hi as the value), but resolution needs the interval, so
// each engine keeps the live set of intervals in memory and consults it during the
// fold (spec 11 §4).
type RangeDel struct {
	Lo      []byte
	Hi      []byte
	Version uint64
}

// NewestCoveringRangeDel returns the newest range-deletion version <= snap that
// covers key, or 0 if none does. Range tombstones are sparse, so a linear scan is
// adequate; a covering-interval index is a later optimization (spec 11 §4). The
// half-open interval [Lo, Hi) includes Lo and excludes Hi.
func NewestCoveringRangeDel(dels []RangeDel, key []byte, snap uint64) uint64 {
	var newest uint64
	for _, d := range dels {
		if d.Version > snap || d.Version <= newest {
			continue
		}
		if CompareUser(key, d.Lo) >= 0 && CompareUser(key, d.Hi) < 0 {
			newest = d.Version
		}
	}
	return newest
}

// Fold resolves one user key's version group to the value visible at snap. It is
// the single source of MVCC resolution shared by both engine cores and the
// conformance oracle (spec 10 §3) -- intentionally one function so the three folds
// cannot drift and the conformance check passes by construction.
//
// ops must be newest-first and hold only set, delete, and merge versions. rangeDel
// is the newest range-deletion version <= snap that covers this key, or 0 for
// none; it acts as a synthetic delete at that version. Because ops are newest-first,
// once a covering range delete is newer than the op under inspection it is newer
// than every remaining op, so it becomes the base: a covered key with no newer set
// or merge resolves absent, while merges committed above the range delete fold over
// an empty base and a set above it survives. merge folds an operand over the
// running value; a nil merge makes an operand replace. It returns the value and
// whether the key is present.
func Fold(ops []Op, snap, rangeDel uint64, merge func(existing, operand []byte) []byte) ([]byte, bool) {
	if rangeDel > snap {
		rangeDel = 0 // a range delete above the snapshot is invisible
	}
	var operands [][]byte // newest-first
	var baseVal []byte
	baseIsSet := false
	for _, op := range ops {
		if op.Version > snap {
			continue // not visible at this snapshot
		}
		if rangeDel > op.Version {
			// The covering range delete is newer than this op and all that follow
			// (newest-first), so it is the delete base; operands already collected
			// above it fold over nil.
			break
		}
		if op.Kind == KindMerge {
			operands = append(operands, op.Value)
			continue
		}
		if op.Kind == KindSet {
			baseVal = op.Value
			baseIsSet = true
		}
		break // a set or delete is the base; stop collecting
	}

	// Present if there is a set base or any merge operand. A delete base (real or
	// the synthetic range delete) with no operands shadows the key; merges above a
	// delete start fresh from the operands.
	if !baseIsSet && len(operands) == 0 {
		return nil, false
	}
	var val []byte
	if baseIsSet {
		val = baseVal
	}
	for k := len(operands) - 1; k >= 0; k-- { // fold oldest operand first
		if merge != nil {
			val = merge(val, operands[k])
		} else {
			val = operands[k]
		}
	}
	return val, true
}
