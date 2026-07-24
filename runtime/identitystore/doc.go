// Package identitystore owns the physical storage-home split for actor
// identities. Callers observe one ActorControlRow namespace; only this package
// knows whether a row lives in the durable backend or the current process.
package identitystore
