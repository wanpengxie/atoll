// Package channel defines the channel-id type. Allocation strategy is outside
// the kernel's scope; the kernel treats IDs as opaque stable strings (no
// validation, normalization, or allocation).
package channel
