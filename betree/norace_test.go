//go:build !race

package betree

// raceEnabled is false in a non-race build. See race_test.go for why the pool recycling and
// allocation tests gate on it.

const raceEnabled = false
