// Package adapter implements the M1.3 daemon-side Adapter Framework
// described by L1 §11 + L2 §8 (Must subset F1/F2/F3/F5/F6 per L2 §8.0).
//
// The framework owns the boilerplate every protocol-edge adapter would
// otherwise re-implement:
//
//   - F1 Adapter interface: declares / init / handle / shutdown / onExternalCallback.
//   - F2 Correlation Tracker: request_id ↔ external_id persistent map with 5-minute GC grace.
//   - F3 Error Policy (subset): Timeout(request_id, after_ms, reason) + FailTerminal(request_id, reason, detail).
//   - F5 ctx.Respond: builds a valid kind=response envelope with the deterministic id "response:<request_id>:<canonical_payload_hash>" and routes through the harness.
//   - F6 Install lifecycle: idempotent install + boot timer recovery (replay pending tool requests after a daemon crash).
//
// Out of scope for M1.3 (deferred per L2 §8.0):
//
//   - F3 retry / logSystemEvent helpers
//   - F4 State Store helpers
//   - F7 Observability hooks
//   - F8 Cross-adapter shared helpers (http / ws / credentials / rate limit)
//   - F6 graceful shutdown beyond cancelling in-flight timers
//
// Adapter implementations register themselves via the package-level
// Register pattern; M1.3 chooses compile-time register over Go plugin
// .so loading (M1.3 tickets §T13 "关键技术决策").
//
// Wiring example (the daemon Manager is invoked at boot):
//
//	mgr, err := adapter.NewManager(adapter.ManagerConfig{
//	    DB:      db,
//	    Deps:    harnessDeps,
//	    Modules: map[string]adapter.Module{"xhs": xhs.New()},
//	})
//	if err != nil { return err }
//	if err := mgr.Install(ctx); err != nil { return err }
//	if err := mgr.BootRecoverTimers(ctx); err != nil { return err }
//	go mgr.RunGC(ctx)
//
//	// trigger gateway dispatches a request envelope
//	mgr.Dispatch(ctx, env)
//
//	// external callback arrives (HTTP endpoint / WS message)
//	mgr.OnExternalCallback(ctx, "xhs", payload)
//
// Authoritative spec references:
//
//   - L1 §11      Adapter contract (Ad-1..Ad-4)
//   - L2 §8.1     AdapterModule interface
//   - L2 §8.2     Correlation Tracker
//   - L2 §8.3     Error Policy
//   - L2 §8.5     Harness integration glue (ctx.respond)
//   - L2 §8.6     Lifecycle (install + boot timer recovery)
//   - L2 §1.4.10.2 deterministic response id derivation
package adapter
