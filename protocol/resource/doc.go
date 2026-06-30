// Package resource defines the resource-id type — the passive object of the
// access plane. Allocation strategy is outside the kernel's scope; the kernel
// treats IDs as opaque stable strings (no validation, normalization, or
// allocation).
package resource
