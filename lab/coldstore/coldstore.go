// Package coldstore is a frozen profiler probe: is the write path's cost the value copy
// landing on cold first-touch pages, or not? The Set profile showed runtime.memmove
// dominating, which looked like a cold-page tax that a bounded resident ring would avoid.
//
// Verdict: refuted. Streaming writes forward through a run-sized buffer is no slower than
// reusing a hot resident ring, and faster on amd64, because sequential full-line stores hit
// write-combining and the prefetcher while the ring pays a wrap branch. So the ring is
// justified by bounding memory for the larger-than-memory goal, not by making writes faster.
// The real write tax is the index scatter, found separately in impl note 177. This probe is
// why the ring's rationale is "bounds memory", not "speeds writes".
package coldstore

import "encoding/binary"

const hdrLen = 4
const coldPayloadLen = 108

// writeRecordAt frames a length prefix and touches every payload byte, so the store traffic
// matches a real record write without needing a source slice.
func writeRecordAt(buf []byte, off int, payload int) {
	binary.LittleEndian.PutUint32(buf[off:off+hdrLen], uint32(payload))
	dst := buf[off+hdrLen : off+hdrLen+payload]
	for i := range dst {
		dst[i] = byte(i)
	}
}
