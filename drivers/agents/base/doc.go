// Package base owns the provider-independent agent mailbox specialization.
// One arbitration loop merges mailbox intake, provider EventPort callbacks,
// request closures, control deadlines, and watchdog expiry. Its explicit
// bounded buffer, committing set, workspace owner, control slot, and settling
// fence keep requests paired while provider engines remain asynchronous. Base
// alone writes progress, activity, terminal envelopes, and resume state.
package base
