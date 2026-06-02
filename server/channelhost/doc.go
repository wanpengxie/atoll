// Package channelhost is the v2 channel-home: it COMPOSES the deployment-agnostic
// core (runtime/store + runtime/harness + lib/channelkit) into a process that
// HOLDS channel truth. This is the v2 truth-flip — truth physically lives at the
// server, not the daemon; the server is no longer a view-cache.
//
// Compose blueprint (port from framework/multiuser/runtime/daemon.go — the v1
// god-object that composed everything; v2 splits truth→here, business cells→
// daemon/host):
//  1. store.OpenChannel(dbPath) → *sql.DB (append-only truth + projections).
//  2. build runtime/store.{Messages(harness.MessageLog), ActorRegistry,
//     TypeRegistry, RequestLookup} over the db.
//  3. harness.New(Deps{Log: Messages, Registry, TypeReg, ...}) → *harness.Chain
//     (the 9-step write path; INVARIANT-13 — all writes go through it).
//  4. sysactor.New(Deps{Registry, Chain, Lookup}) → channel system cell.
//  5. channelkit.New(Config{System}) → assembles cells + policy + supervision.
//  6. spawn固有 cells (sysactor) on the home; channel genesis is server-local
//     (no placement-CAS/reclaim — proto-v2-physical-revision §3).
//
// Depends on runtime + lib + wire + internal (T5). MUST NOT import daemon or
// concrete adapters (cmd injects those).
package channelhost
