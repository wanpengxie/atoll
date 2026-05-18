//go:build e2e

package e2e

import (
	"fmt"
	"sync/atomic"
	"time"
)

// uniqSuffix returns a process-unique string fragment safe to append
// to emails / workspace names / channel names. Tests don't share a
// stack, but they do share the same server.db within a stack (when
// future tests opt in to stack-sharing), so collision-free names
// keep assertions deterministic.
var uniqCounter uint64

func uniqSuffix() string {
	n := atomic.AddUint64(&uniqCounter, 1)
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), n)
}
