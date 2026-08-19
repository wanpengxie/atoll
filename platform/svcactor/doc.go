// Package svcactor implements two deliberately separate faces. Its ordinary
// actor mailbox serves svcactor.set/get and is described by actor.describe.
// Its process-local peer port serves call frames plus Describe{from}->Card;
// peer describe never enters the mailbox or writes the channel log.
//
// One private state value atomically stores the service table and materialized
// card. The table maps accepted words to complete member ids; ordinary channels
// have no system-word table entries.
//
// Dispatch is structural: replies and cancellation return to pending calls;
// requests from c0 may reach membrane words, c0 itself routes space words to
// the fixed registrar target, explicit endpoint words use the private table,
// and agent.ask uses the configured service agent. The actor preserves the
// peer frame's caller with CallFor and maps local progress and terminal state
// back into peer protocol frames.
package svcactor
