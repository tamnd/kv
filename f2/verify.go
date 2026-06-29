package f2

import "fmt"

// VerifyProblem is one structural fault a Verify walk found: a shard's index slot whose
// record does not decode, which means the log bytes the slot addresses are torn or have
// failed their CRC. Detail names the shard, slot, and address so a caller can locate it.
type VerifyProblem struct {
	Shard  int
	Slot   int
	Addr   int64
	Detail string
}

// VerifyResult is the outcome of a structural walk: how many live keys decoded cleanly,
// how many log pages the store spans, and every fault found. An empty Problems slice means
// every live record the index points at decoded and passed its CRC.
type VerifyResult struct {
	Keys     int64
	Pages    int
	Problems []VerifyProblem
}

// Verify walks every shard's index and re-reads the record each live slot addresses,
// confirming it decodes and passes its CRC. f2 has no key order to check, so the walk is
// not a tree traversal: it is an integrity pass over the live set, the records a reader can
// actually reach. A slot whose record fails to decode (a torn tail, a flipped byte, a
// truncated page) is reported rather than skipped, so bit rot in the durable log surfaces
// as a problem instead of a silently dropped key.
//
// The walk takes each shard's read lock for its pass, which excludes the writer, the
// evictor, and the compactor, so an evicted record read from the file is stable and a
// resident record is not recycled mid-read. It reads shards one at a time, so the lock is
// never held across the whole store.
func (s *Store) Verify() VerifyResult {
	var res VerifyResult
	for sid, sh := range s.shards {
		sh.mu.RLock()
		res.Pages += sh.log.npages
		idx := sh.index.Load()
		for i := range idx.slots {
			slot := idx.slots[i].Load()
			if slot == 0 || slot&slotTombstone != 0 {
				continue
			}
			addr := slotAddr(slot)
			key, _ := idx.log.read(addr)
			if key == nil {
				res.Problems = append(res.Problems, VerifyProblem{
					Shard:  sid,
					Slot:   i,
					Addr:   addr,
					Detail: fmt.Sprintf("shard %d slot %d: record at address %d does not decode (torn or failed CRC)", sid, i, addr),
				})
				continue
			}
			res.Keys++
		}
		sh.mu.RUnlock()
	}
	return res
}
