// Package channel defines the channel-id type. Allocation strategy belongs to
// the embedding framework; the kernel treats IDs as opaque stable strings.
package channel

// ID is the channel identifier. It is equivalent to envelope `channel_id`
// and is opaque to the kernel.
type ID string

// String returns the wire form.
func (c ID) String() string { return string(c) }
