# kv

A fast embedded key/value database for Go that looks and feels like SQLite: the
whole database is one file, durability comes from a write-ahead log, and you open
it with a path and a line of code. Underneath that familiar surface is a modern,
high-throughput, low-latency storage engine.

`kv` is not a SQLite clone and it is not SQL. It is a key/value engine — ordered
keys, byte-string values, range scans, transactions — wearing SQLite's
operational clothes: a single self-describing `.kv` file, an optional `-wal`
sidecar, a pager with a buffer pool, PRAGMA-style tuning knobs, a real CLI, and a
stable documented on-disk format.

The engine is pluggable. A single storage-engine seam (the Engine SPI) sits
between the durable substrate (file format, pager, WAL, MVCC, cache) and the
ordered map that actually stores keys. Two cores implement that seam:

- a **B-tree core** (default) — an in-place B-link B+tree tuned for low read
  latency and the truest SQLite feel; and
- an **LSM core** (opt-in) — a single-file log-structured merge tree with
  key/value separation, tuned for high write throughput and large values.

Both cores share the same file, the same WAL, the same transactions, the same
iterators, the same Go API, the same CLI, and the same server.

## Status

Early implementation. The design of record is the specification; the
implementation follows its roadmap milestone by milestone. See
[`docs/`](docs/) for the implementation notes that track what is built and how.

## Install

```
go get github.com/tamnd/kv
```

## Quick start

```go
db, err := kv.Open("data.kv")
if err != nil {
	log.Fatal(err)
}
defer db.Close()

err = db.Update(func(txn *kv.Txn) error {
	return txn.Set([]byte("hello"), []byte("world"))
})

err = db.View(func(txn *kv.Txn) error {
	v, err := txn.Get([]byte("hello"))
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", v)
	return nil
})
```

## License

Apache License 2.0. See [LICENSE](LICENSE).
