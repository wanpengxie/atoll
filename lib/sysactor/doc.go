// Package sysactor is the channel's cross-cutting physical-state actor. It is
// the single owner of the channel's ephemeral PRESENCE state (compute lease:
// who is physically online) and answers the channel-wide directory query
// (actor.list) as a composed view (membership ∧ presence, the formula owned by
// introspect.QueryList). It is ADVISORY — never a dispatch gate (P15/P16):
// reachability authority is send→terminal, and the dispatch path never reads
// this actor's view. It runs as a channel-intrinsic cell, spawned once per
// channel at channel creation time.
//
// Two-axis model (runtime/storespec.Record): membership is durable registry
// truth; PRESENCE is volatile and AUTHORITY-OWNED (the component holding the
// compute leases/connections). The system actor does NOT keep a presence copy —
// it READS presence via an injected seam when composing actor.list, exactly as
// it reads membership from the Registry. Presence is never a message and never
// truth — a volatile state read. readiness is NOT a third axis — whether an
// actor can service a request is the OUTCOME of send→terminal, not a stored
// state the system actor projects or composes.
package sysactor
