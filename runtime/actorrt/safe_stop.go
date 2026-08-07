package actorrt

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// stopBudget is cooperative: it bounds the context offered to Stop, not the
// goroutine itself. A Stop implementation that ignores ctx keeps Unit.Done
// open and is reported by its host as a physical leak.
const stopBudget = 5 * time.Second

// safeStop is the one invocation site for actor cleanup. It contains a panic so
// one faulty actor cannot tear down the process, while preserving the honest
// Done contract: the caller only continues after Stop actually returns.
func safeStop(logger *slog.Logger, id actor.ActorID, stopper Stopper) (err error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithTimeout(context.Background(), stopBudget)
	defer cancel()
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("actorrt: actor %s Stop panicked: %v", id, recovered)
			logger.Error(
				"actorrt.cell.stop_panicked",
				"actor", string(id),
				"panic", recovered,
				"stack", string(debug.Stack()),
			)
		}
	}()
	if err = stopper.Stop(ctx); err != nil {
		logger.Error("actorrt.cell.stop_abandoned", "actor", string(id), "err", err)
	}
	return err
}
