//go:build race

package betree

// raceEnabled reports whether the test binary was built with the race detector. The runtime's
// sync.Pool deliberately does not recycle objects under -race (it drops them to help surface
// use-after-put races), so the pool tests that assert recycling and zero-allocation only hold in a
// non-race build and skip themselves under -race. Split across two build-tagged files so the constant
// is a compile-time value with no runtime cost.

const raceEnabled = true
