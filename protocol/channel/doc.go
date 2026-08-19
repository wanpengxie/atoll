// Package channel defines channel identity and the data-only peer call,
// progress, result, cancel, describe, and card frames used between channels.
// A binding owns the target, so frames carry an authenticated from but no to.
// It is a protocol leaf: runtime identities, storage, transport bindings, and
// provisioning commands do not belong here.
package channel
