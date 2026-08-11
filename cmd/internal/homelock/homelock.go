// Package homelock pins the home model's process rule: one home, at most one
// live process. Two processes sharing a home is the double-identity accident
// in every role — two carriers presenting one device credential, two engines
// mutating one installation — so the second starter must fail LOUDLY at the door
// with a human answer, not deep in a driver with a yamux or sqlite growl.
//
// Mechanism: flock(LOCK_EX|LOCK_NB) on <home>/.lock, held for the process
// lifetime. Kernel-owned, so a crash or kill -9 releases it instantly — no
// stale-pidfile sweeping, no false "already running" after a power cut.
package homelock

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// Acquire takes the exclusive lock for home (creating the directory if
// needed). The returned release func unlocks; letting the process exit works
// exactly as well (the kernel drops flocks with the fd). A held lock returns
// an error that names the home and says what to do.
func Acquire(home, role string) (release func(), err error) {
	if err := os.MkdirAll(home, 0o755); err != nil {
		return nil, fmt.Errorf("%s home %s: %w", role, home, err)
	}
	f, err := os.OpenFile(filepath.Join(home, ".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("%s home %s: %w", role, home, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if err == syscall.EWOULDBLOCK {
			return nil, fmt.Errorf(
				"%s home %s is already in use by a running atoll process — stop it first, or point this one at a different home",
				role, home)
		}
		return nil, fmt.Errorf("%s home %s: lock: %w", role, home, err)
	}
	return func() { f.Close() }, nil
}
