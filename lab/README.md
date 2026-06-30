# lab: a library of technique-choice experiments

Each subdirectory is one small, self-contained experiment that settled a technique decision in
the clean-room engine with measured numbers rather than opinion. Both the winner and the loser
stay checked in, so every decision is reproducible: run the benchmark and read the board.

The experiments do not depend on the engine. They are frozen records of the comparison as it
was run, so they keep telling the truth even as the engine moves on. The engine carries only
the winners; the reasons live here and in the matching impl note under
`~/notes/Spec/2059/implementation/`.

| experiment | question | winner | note |
| --- | --- | --- | --- |
| [append](append) | lock-free fetch-add or mutex bump for the write path? | lock-free fetch-add | 173 |
| [hash](hash) | which key fingerprint for the index? | maphash.Bytes | 174 |
| [index](index) | how do a lookup and an insert coordinate on a slot? | lock-free open addressing | 175 |
| [coldstore](coldstore) | is the write tax the value copy on cold pages? | no, it is the index scatter | 177 |
| [mapping](mapping) | logical address to file offset: direct or block table? | direct mapping | 178 |
| [hotindex](hotindex) | does bounding the index to the working set pay off? | yes, 4x to 11x | 179 |
| [cache](cache) | how is a read-cache cell read and written concurrently? | atomic-pointer copy-on-write | 180 |
| [commit](commit) | fsync once per append or amortize over a batch? | group commit at the flush batch | 181 |
| [codec](codec) | which compression codec for the cold tier? | flate level 1 (space lever, not throughput) | 182 |
| [migrate](migrate) | how deep should the seal-to-migrate pipeline be? | depth four: overlap plus burst headroom | 184 |
| [flush](flush) | how often should a commit wake the flusher? | only past a byte threshold, with a ticker backstop | 185 |

## Running one

```
GOWORK=off go test -run '^$' -bench=. -benchmem -benchtime=2s ./lab/<name>/
```

Most are best read across cores, with `-cpu=1,2,4,8`, since the point of several is how the
candidates diverge as contention rises. The full boards, including the Linux and Windows fleet
numbers, are in the impl notes.
