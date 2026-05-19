// Package channel defines the channel-id type and its forward-compatible
// federation reference (ChannelRef). Channel-id is server-allocated and
// globally unique within a server registry per L1 §3.1.
package channel

import (
	"database/sql/driver"
	"fmt"
)

// ID is the channel identifier — server-allocated, globally unique
// within the server registry per L1 §3.1. Equivalent to envelope
// `channel_id` (L0 §2.1 — same namespace).
type ID string

// String returns the wire form.
func (c ID) String() string { return string(c) }

// Value implements driver.Valuer for SQL TEXT boundaries.
func (c ID) Value() (driver.Value, error) { return string(c), nil }

// Scan implements sql.Scanner for SQL TEXT boundaries.
func (c *ID) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*c = ""
		return nil
	case string:
		*c = ID(v)
		return nil
	case []byte:
		*c = ID(string(v))
		return nil
	default:
		return fmt.Errorf("channel.ID: scan unsupported %T", src)
	}
}
