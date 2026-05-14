// Command coagent is the v4 agent-facing CLI. It exposes three
// subcommands matching the v4 message ADT classes (L2 §3.1):
//
//	coagent emit     # kind=event   — declare a fact
//	coagent ask      # kind=request — ask / call a tool
//	coagent answer   # kind=response — reply to a prior request
//
// The binary is a thin wrapper over pkg/coagent.Run — all subcommand
// logic, flag parsing, envelope normalization, and binding dispatch
// live in the package so worker processes (in-process callers) can
// drive the same code path by injecting their own Binding.
//
// Binary mode defaults to the daemon_rpc binding (L2 §3.4.1):
//
//   - DAEMON_URL              daemon HTTP RPC endpoint (required)
//   - COAGENT_AUTH_TOKEN      bearer token for Authorization header
//   - COAGENT_CHANNEL_ID      current channel id
//   - COAGENT_SELF_ID         caller actor id (envelope.sender.id default)
//   - COAGENT_TRIGGER_CORRELATION_ID  trigger correlation_id (propagation default)
//   - COAGENT_FENCING_TOKEN   worker lease token (when running in a worker)
//
// Worker processes invoke pkg/coagent.Run directly with a pre-built
// in_worker_bus Binding — see pkg/coagent.NewInWorkerBusBinding.
//
// Exit codes (stable contract for shell callers):
//
//	0 success
//	2 usage / unknown subcommand / bad args
//	3 harness or client-side reject (RejectError on stderr)
//	4 infrastructure error (transport, sql, ctx cancel)
//	5 no binding wired
//	6 flag value failed to parse (json, duration, ...)
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/coagent-ai/daemon-go/pkg/coagent"
)

// Version is the coagent CLI placeholder version. Bumped per ticket.
const Version = "0.0.0-t12"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	exit := coagent.Run(ctx, coagent.Config{
		Args:   os.Args[1:],
		Env:    os.Getenv,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	})
	os.Exit(exit)
}
