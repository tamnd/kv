// Command kv serves a single kv store over the Redis wire protocol.
// It is the network face of the bare hash-log engine: open one file, size the
// tiers from the workload hints, and answer GET/SET/DEL on a TCP port, a unix
// socket, or both until a signal arrives, then sync and close so the file shuts
// down coherently. It is the over-the-wire counterpart to the in-process engine,
// so a benchmark can measure the same store across a socket the way it measures
// redis or valkey, with the network round-trip in the number.
//
// The flags follow redis-server so the binary is close to a drop-in: --port,
// --bind, --unixsocket, --dir, --dbfilename, --appendonly, --appendfsync, and
// --maxmemory carry their redis meaning. --appendfsync picks the durability
// contract: everysec (the default) acks a write from memory and fsyncs it a
// moment later, always waits for the group-commit fsync before the write returns.
// --cardinality and --value-bytes are kv-specific sizing hints with no redis
// equivalent; they let a benchmark shape the tiers the same way the in-process
// adapter does rather than guessing from defaults.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/tamnd/kv"
	"github.com/tamnd/kv/resp"
)

// Build metadata, stamped by the linker at release time via -X. The zero
// values are what a plain `go build` produces, so a from-source binary reports
// "dev" rather than a bare empty string.
var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("kv", flag.ContinueOnError)
	showVersion := fs.Bool("version", false, "print version and exit")
	// Redis-server-compatible flags. Bare `kv` starts on 127.0.0.1:6379 the way
	// redis-server with no config does.
	port := fs.Int("port", 6379, "TCP port to serve RESP on (0 disables the TCP listener)")
	bind := fs.String("bind", "127.0.0.1", "address to bind the TCP listener to")
	unixsocket := fs.String("unixsocket", "", "unix socket path to also serve RESP on (the fast local path)")
	dir := fs.String("dir", ".", "data directory")
	dbfilename := fs.String("dbfilename", "dump.kv", "store file name within --dir")
	appendonly := fs.String("appendonly", "yes", "keep the append log: yes | no (kv is always log-backed)")
	appendfsync := fs.String("appendfsync", "everysec", "fsync policy: no | everysec | always")
	maxmemory := fs.String("maxmemory", "0", "resident memory budget, a redis-style size like 512mb (0 uses the engine default)")
	// Workload sizing hints, kv-specific. The harness knows the cell's cardinality
	// and value size, so it passes them and the server sizes the tiers the same way
	// the in-process adapter does, rather than guessing from defaults.
	cardinality := fs.Int("cardinality", 0, "expected distinct key count, sizes the resident key index (0 uses the engine default)")
	valueBytes := fs.Int("value-bytes", 0, "value size hint, sizes the hot segment (0 assumes a 1 KiB record)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		fmt.Printf("kv %s (%s, built %s)\n", Version, Commit, Date)
		return 0
	}

	fsync := strings.ToLower(*appendfsync)
	switch fsync {
	case "no", "everysec", "always":
	default:
		fmt.Fprintln(os.Stderr, "kv: --appendfsync must be no, everysec, or always")
		return 2
	}
	aof := strings.ToLower(*appendonly)
	if aof != "yes" && aof != "no" {
		fmt.Fprintln(os.Stderr, "kv: --appendonly must be yes or no")
		return 2
	}
	maxmem, err := parseSize(*maxmemory)
	if err != nil {
		fmt.Fprintln(os.Stderr, "kv: --maxmemory:", err)
		return 2
	}
	if *port == 0 && *unixsocket == "" {
		fmt.Fprintln(os.Stderr, "kv: nothing to listen on; set --port or --unixsocket")
		return 2
	}

	opts := buildOptions(*cardinality, *valueBytes, maxmem, fsync)
	dbPath := filepath.Join(*dir, *dbfilename)
	db, err := kv.Open(dbPath, opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "kv: open:", err)
		return 1
	}

	lns, err := listen(*bind, *port, *unixsocket)
	if err != nil {
		fmt.Fprintln(os.Stderr, "kv: listen:", err)
		_ = db.Close()
		return 1
	}

	srv := resp.New(db, resp.Config{
		AppendOnly:  aof,
		AppendFsync: fsync,
		MaxMemory:   maxmem,
		Dir:         *dir,
		DBFilename:  *dbfilename,
	})
	// A signal closes the listeners and drops the connections, so every Serve
	// returns and the final sync and close run on the way out.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		_ = srv.Close()
	}()

	// One Serve goroutine per listener so a TCP port and a unix socket drive the
	// same store at once. The first real accept error wins and tears the rest down;
	// Serve returns nil after a Close, so a clean shutdown collects no error.
	var wg sync.WaitGroup
	var mu sync.Mutex
	var serveErr error
	for _, ln := range lns {
		wg.Add(1)
		go func(ln net.Listener) {
			defer wg.Done()
			if e := srv.Serve(ln); e != nil {
				mu.Lock()
				if serveErr == nil {
					serveErr = e
				}
				mu.Unlock()
				_ = srv.Close()
			}
		}(ln)
	}
	wg.Wait()

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
		fmt.Fprintln(os.Stderr, "kv:", serveErr)
		return 1
	}
	return 0
}

// listen binds the RESP listeners: a TCP listener when --port is non-zero, a unix
// listener when --unixsocket is set, or both, matching redis-server. A stale unix
// socket file from a crashed run is removed first so the bind does not fail on an
// address already in use. On error every listener already bound is closed.
func listen(bind string, port int, unixsocket string) ([]net.Listener, error) {
	var lns []net.Listener
	if port > 0 {
		l, err := net.Listen("tcp", net.JoinHostPort(bind, strconv.Itoa(port)))
		if err != nil {
			return nil, err
		}
		lns = append(lns, l)
	}
	if unixsocket != "" {
		_ = os.Remove(unixsocket)
		l, err := net.Listen("unix", unixsocket)
		if err != nil {
			for _, x := range lns {
				_ = x.Close()
			}
			return nil, err
		}
		lns = append(lns, l)
	}
	return lns, nil
}

// buildOptions sizes the store from the workload hints, mirroring the in-process
// kvbench adapter so the served store is the same shape as the embedded one. The
// fsync policy sets the durability contract: appendfsync always opens the store
// with SyncWrites, so a write waits for the group-commit fsync before it returns;
// everysec and no leave the background flusher as the durability path.
func buildOptions(cardinality, valueBytes int, maxmemory int64, appendfsync string) kv.Options {
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
	return kv.Options{
		KeyCapacity:    cardinality,
		HotBytes:       hotBytes,
		HotKeys:        hotRecords + hotRecords/4,
		ResidentBytes:  maxmemory,
		ReadCacheCells: 4096,
		SyncWrites:     appendfsync == "always",
	}
}

// parseSize reads a redis-style memory size: a plain byte count, or a number with
// a unit suffix. The kb/mb/gb suffixes are powers of 1024 and the bare k/m/g
// suffixes are powers of 1000, the same split redis uses. An empty string is 0.
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return 0, nil
	}
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "kb"):
		mult, s = 1024, s[:len(s)-2]
	case strings.HasSuffix(s, "mb"):
		mult, s = 1024*1024, s[:len(s)-2]
	case strings.HasSuffix(s, "gb"):
		mult, s = 1024*1024*1024, s[:len(s)-2]
	case strings.HasSuffix(s, "k"):
		mult, s = 1000, s[:len(s)-1]
	case strings.HasSuffix(s, "m"):
		mult, s = 1000*1000, s[:len(s)-1]
	case strings.HasSuffix(s, "g"):
		mult, s = 1000*1000*1000, s[:len(s)-1]
	case strings.HasSuffix(s, "b"):
		mult, s = 1, s[:len(s)-1]
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return n * mult, nil
}
