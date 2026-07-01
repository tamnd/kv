// Command hlog-server serves a single hlog store over the Redis wire protocol.
// It is the network face of the bare hash-log engine: open one file, size the
// tiers from the workload hints, and answer GET/SET/DEL on a unix socket or a TCP
// port until a signal arrives, then sync and close so the file shuts down
// coherently. It is the over-the-wire counterpart to the in-process engine, so a
// benchmark can measure the same store across a socket the way it measures redis
// or valkey, with the network round-trip in the number.
//
// The sizing flags mirror the in-process adapter: the engine keeps a resident key
// index sized to the cardinality, a hot tier sized to the value, and a resident
// cold window sized to the cache budget, so the served store is the same shape as
// the embedded one rather than a differently tuned second instance.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/tamnd/kv/hlog"
	"github.com/tamnd/kv/hlog/resp"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("hlog-server", flag.ContinueOnError)
	unixsocket := fs.String("unixsocket", "", "unix socket path to serve RESP on (preferred for a local benchmark)")
	addr := fs.String("addr", "", "TCP listen address for RESP, for example 127.0.0.1:6380 (empty disables TCP)")
	dir := fs.String("dir", ".", "data directory; the store lives at <dir>/hlog.db")
	synchronous := fs.String("synchronous", "default", "durability: off | normal | full | default")
	// Workload sizing hints. The harness knows the cell's cardinality and value
	// size, so it passes them and the server sizes the tiers the same way the
	// in-process adapter does, rather than guessing from defaults.
	cardinality := fs.Int("cardinality", 0, "expected distinct key count, sizes the resident cold index (0 uses the engine default)")
	valueBytes := fs.Int("value-bytes", 0, "value size hint, sizes the hot segment index (0 assumes a 1 KiB record)")
	cacheBytes := fs.Int64("cache-bytes", 0, "resident cold window in bytes (0 uses the engine default)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *unixsocket == "" && *addr == "" {
		fmt.Fprintln(os.Stderr, "hlog-server: one of --unixsocket or --addr is required")
		return 2
	}

	opts, forceSync := buildOptions(*cardinality, *valueBytes, *cacheBytes, *synchronous)
	dbPath := filepath.Join(*dir, "hlog.db")
	db, err := hlog.Open(dbPath, opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "hlog-server: open:", err)
		return 1
	}

	ln, err := listen(*unixsocket, *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "hlog-server: listen:", err)
		_ = db.Close()
		return 1
	}

	srv := resp.New(db, forceSync)
	// A signal closes the listener and drops the connections, so Serve returns and
	// the deferred sync and close run on the way out.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		_ = srv.Close()
	}()

	serveErr := srv.Serve(ln)
	if *unixsocket != "" {
		_ = os.Remove(*unixsocket)
	}
	// Force a final durability barrier before the file closes so a clean shutdown
	// leaves nothing in the hot tier unflushed, then close the store.
	_ = db.Sync()
	if cerr := db.Close(); cerr != nil && serveErr == nil {
		serveErr = cerr
	}
	if serveErr != nil {
		fmt.Fprintln(os.Stderr, "hlog-server:", serveErr)
		return 1
	}
	return 0
}

// listen binds the RESP listener. A unix socket wins when both are set; it is the
// fast local path the benchmark uses. A stale socket file from a crashed run is
// removed first so the bind does not fail on an address already in use.
func listen(unixsocket, addr string) (net.Listener, error) {
	if unixsocket != "" {
		_ = os.Remove(unixsocket)
		return net.Listen("unix", unixsocket)
	}
	return net.Listen("tcp", addr)
}

// buildOptions sizes the store from the workload hints, mirroring the in-process
// kvbench adapter so the served store is the same shape as the embedded one. It
// returns the options and whether a durability hook should force a Sync: every
// mode but off keeps the synced contract, off leaves the background flusher as the
// only durability.
func buildOptions(cardinality, valueBytes int, cacheBytes int64, synchronous string) (hlog.Options, bool) {
	const (
		hotRecords  = 32768
		maxHotBytes = int64(64 << 20)
		recordOver  = 32
		fallbackRec = 1056 // a 1 KiB value plus framing when the value size is unknown
	)
	recordBytes := int64(fallbackRec)
	if valueBytes > 0 {
		recordBytes = int64(valueBytes + recordOver)
	}
	hotBytes := min(int64(hotRecords)*recordBytes, maxHotBytes)
	opts := hlog.Options{
		KeyCapacity:    cardinality,
		HotBytes:       hotBytes,
		HotKeys:        hotRecords + hotRecords/4,
		ResidentBytes:  cacheBytes,
		ReadCacheCells: 4096,
	}
	forceSync := strings.ToLower(synchronous) != "off"
	return opts, forceSync
}
