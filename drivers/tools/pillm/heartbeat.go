package pillm

import (
	"context"
	"time"
)

// Callers join done before replying, so liveness cannot follow their terminal.
func heartbeat(ctx context.Context, interval time.Duration, emit func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if ctx.Err() != nil {
					return
				}
				emit()
			}
		}
	}()
	return done
}
