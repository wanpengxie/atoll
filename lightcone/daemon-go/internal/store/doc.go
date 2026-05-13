// Package store will host the channel-local + daemon-level SQLite layer
// (modernc.org/sqlite, WAL mode) introduced by ticket T1.
//
// T0 keeps this package empty so the directory exists for downstream
// imports while leaving the schema work — 6 channel-local tables +
// bootstrap_registry — to T1 per .dalek/pm/m1.3-tickets.md §T1.
package store
