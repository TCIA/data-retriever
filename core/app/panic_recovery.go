package app

import (
	"runtime/debug"
)

// recoverPanic should be deferred at the top of every goroutine this
// package launches that isn't already covered by a caller's own recover.
// A panic in a goroutine only unwinds that goroutine's own stack: unlike an
// error return, it is never caught by a recover() deferred somewhere in the
// goroutine that merely launched it with `go`. Left unrecovered, it crashes
// the entire process immediately, taking down every other in-flight
// download with it and leaving nothing but Go's raw stack dump on stderr —
// which is invisible in the GUI and easy to miss in the CLI. label
// identifies which goroutine panicked; onRecover (optional) lets the caller
// react, e.g. to mark whatever work that goroutine was doing as failed
// instead of leaving it stuck forever (a channel never closed, a
// WaitGroup never decremented).
func recoverPanic(label string, onRecover func(recovered any)) {
	if r := recover(); r != nil {
		logger.Errorf("panic in %s: %v\n%s", label, r, debug.Stack())
		if onRecover != nil {
			onRecover(r)
		}
	}
}
