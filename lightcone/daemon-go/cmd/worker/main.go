// Command worker is the Go re-implementation of the v4 worker process
// (replacing the bash CLI + hand-written worker from M1.2).
//
// T0 scope: placeholder entrypoint. It logs a startup banner via log/slog,
// runs a go-kimi integration smoke (echo provider — no API key needed),
// then exits with status 0. The real worker runtime is wired up in T10
// (go-kimi Agent + v4 ABI adapter) and T11 (built-in tool actor wrappers).
//
// The presence of this entrypoint is what proves go-kimi successfully
// links into the daemon-go module: if the import chain breaks, the
// binary fails to build and CI catches it.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/coagent-ai/daemon-go/pkg/kimismoke"
)

// Version is the worker placeholder version. Bumped per ticket.
const Version = "0.0.0-t0"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	slog.Info("worker starting",
		"event", "worker.start",
		"version", Version,
		"pid", os.Getpid(),
	)

	// T0 placeholder: run the go-kimi integration smoke so the binary
	// has a concrete code path that exercises NewAgent + Run + Close.
	// T10/T11 replace this with the real v4 ABI adapter loop.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := kimismoke.Run(ctx, kimismoke.Options{Prompt: "hello from daemon-go worker"})
	if err != nil {
		slog.Error("worker smoke failed",
			"event", "worker.smoke.error",
			"err", err.Error(),
		)
		os.Exit(1)
	}

	slog.Info("worker smoke ok",
		"event", "worker.smoke.ok",
		"reply", res.Reply,
		"model", res.Model,
	)
}
