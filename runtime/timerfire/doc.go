// Package timerfire is the substrate glue between the scheduler's fire exit
// and the message organ: a due timer's message enters the ledger through the
// ordinary author-welded pen path, guarded by the author-membership verdict.
// The whole loop — schedule exit → Controller admission → harness write — is
// substrate behaviour and lives in runtime; Platform only constructs this sink
// at assembly time and plugs it into schedule.Deps.
package timerfire
