// Package sdk will provide the Go client library for actors (daemon-hosted
// or standalone) to interact with the atoll server: send messages, read
// channel logs, and manage actor lifecycle — the programmatic equivalent of
// the HTTP/WS gateway, shaped for actor authors rather than HTTP callers.
//
// Not yet implemented; the previous client.go was a CLI-only consumer with
// no real actor use case driving it. Will be rebuilt when a concrete actor
// needs a typed Go client instead of raw HTTP.
package sdk
