package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/tamnd/kv/server"
)

// cmdServe opens a database and serves it over the network (spec 17): the CLI's job is to
// open the file, hand the writer to a server.Server, and run the listener until a signal
// arrives, then shut down cleanly so served work drains and the file closes coherently. The
// served surface is the same operation set the library and the rest of the CLI expose, on a
// socket instead of in process, so a database can be shared across processes or hosts.
func cmdServe(args []string) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	addr := fs.String("addr", ":8480", "listen address for the HTTP surface")
	binaryAddr := fs.String("binary-addr", "", "listen address for the binary protocol (empty disables it)")
	if err := parseArgs(fs, args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		return usageErr("usage: kv serve <db> [-addr host:port] [-binary-addr host:port]")
	}

	d, code := openDB(fs.Arg(0))
	if code != exitOK {
		return code
	}
	defer d.Close()

	// Bind the listener before announcing readiness so the printed address is the real one,
	// including the OS-assigned port when -addr ends in :0, and so a bind failure is reported
	// before any traffic is promised.
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return fail(err)
	}
	srv := server.New(d, server.Options{Addr: ln.Addr().String()})

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	fmt.Fprintf(os.Stderr, "kv: serving %s on http://%s\n", fs.Arg(0), ln.Addr().String())

	// The binary protocol is opt-in: when -binary-addr is set, bind a second listener and serve
	// the efficient wire on it alongside HTTP. The same Service backs both, so the two faces
	// agree on every operation. A closed listener on shutdown ends ServeBinary without error.
	if *binaryAddr != "" {
		bln, err := net.Listen("tcp", *binaryAddr)
		if err != nil {
			return fail(err)
		}
		go func() { errc <- srv.ServeBinary(bln) }()
		fmt.Fprintf(os.Stderr, "kv: serving %s binary on kv://%s\n", fs.Arg(0), bln.Addr().String())
	}

	// Run until the listener fails or an interrupt/terminate signal arrives, then drain
	// in-flight requests with a bounded shutdown before the deferred Close folds the file.
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errc:
		if err != nil && err != http.ErrServerClosed {
			return fail(err)
		}
		return exitOK
	case sig := <-sigc:
		fmt.Fprintf(os.Stderr, "kv: %s, shutting down\n", sig)
		if err := srv.Shutdown(context.Background()); err != nil {
			return fail(err)
		}
		return exitOK
	}
}
