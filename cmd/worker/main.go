// Package main wires the coagent worker subprocess binary.
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T3 (worker IPC-only
// constraint — codex review #9).
//
// The worker binary is a thin shell: it constructs runtime/worker.Runtime
// with os.Stdin / os.Stdout as the IPC stream and calls Run. The agent
// loop itself (go-kimi or replacement) is wired by composing a
// worker.Bridge implementation here so that runtime/worker stays
// vendor-light (no go-kimi import inside runtime/worker).
//
// M1.5-T3 ships a no-op bridge — concrete agent loops come in T4/T5.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/coagent-ai/coagent/runtime/worker"
)

func main() {
	leaseID := flag.String("lease-id", os.Getenv("COAGENT_WORKER_LEASE_ID"),
		"lease id assigned by daemon (also via COAGENT_WORKER_LEASE_ID)")
	flag.Parse()

	if *leaseID == "" {
		fmt.Fprintln(os.Stderr, "worker: --lease-id required (or COAGENT_WORKER_LEASE_ID env)")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rt, err := worker.New(worker.Config{
		LeaseID: *leaseID,
		In:      os.Stdin,
		Out:     os.Stdout,
		// Bridge nil — M1.5-T3 placeholder. T4/T5 plug the go-kimi loop.
	})
	if err != nil {
		log.Fatalf("worker: assemble: %v", err)
	}
	if err := rt.Run(ctx); err != nil {
		log.Fatalf("worker: exit: %v", err)
	}
}
