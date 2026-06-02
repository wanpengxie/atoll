// Package fencing declares the minimal write-fence identity carried by
// channel-local mutation paths.
package fence

// FencingToken is the opaque guard value that gates writes to a channel
// store. Equality is the only protocol-level comparison.
type FencingToken string

// String returns the wire form.
func (f FencingToken) String() string { return string(f) }

// DaemonEpoch identifies one daemon process lifetime. It changes across
// restarts and is paired with FencingToken when validating writes.
type DaemonEpoch int64
