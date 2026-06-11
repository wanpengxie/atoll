package channel

// ID is the channel identifier. It is equivalent to envelope `channel_id`
// and is opaque to the kernel.
type ID string

// String returns the wire form.
func (c ID) String() string { return string(c) }
