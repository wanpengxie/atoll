// Package channel defines the channel-id type. Allocation strategy is outside
// the kernel's scope; the kernel treats IDs as opaque stable strings (no
// validation, normalization, or allocation).
package channel

// ID is the channel identifier. It is equivalent to envelope `channel_id`
// and is opaque to the kernel.
type ID string

// String returns the wire form.
func (c ID) String() string { return string(c) }
