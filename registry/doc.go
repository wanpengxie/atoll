// Package registry is the fat-daemon actor self-registration point: each actor
// package's init() registers its hosting Decl here (driver-registration pattern,
// like database/sql's sql.Register / image.RegisterFormat), so a daemon
// composition root just blank-imports the actor packages and the registry fills
// itself — zero hand-maintained actor list, zero main.go edit to add an adapter.
package registry
