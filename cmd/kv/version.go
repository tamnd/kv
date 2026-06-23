package main

import (
	"fmt"
	"runtime"

	"github.com/tamnd/kv"
)

// Build identity, stamped by the release pipeline through -ldflags -X. The values are
// cosmetic: they identify the binary but do not affect on-disk compatibility, which is
// governed by the file format and kv.Version. A `go install`ed or `go build`ed binary
// keeps the defaults, so the tool still reports something sensible off a release.
var (
	// Version is the release tag (for example "0.1.0"), or "dev" for an untagged build.
	Version = "dev"
	// Commit is the short git SHA the binary was built from.
	Commit = "none"
	// Date is the commit timestamp of the build.
	Date = "unknown"
)

// cmdVersion prints the CLI build identity and the library version it links. The library
// version (kv.Version) is the contract that matters for data: two binaries that report
// the same kv.Version read and write the same on-disk format.
func cmdVersion(args []string) int {
	fmt.Printf("kv %s\n", Version)
	fmt.Printf("  library  %s\n", kv.Version)
	fmt.Printf("  commit   %s\n", Commit)
	fmt.Printf("  built    %s\n", Date)
	fmt.Printf("  go       %s %s/%s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH)
	return exitOK
}
