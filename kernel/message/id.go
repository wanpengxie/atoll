package message

import (
	"database/sql/driver"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

// ID is the envelope.message identifier wire form.
type ID string

// String returns the wire form.
func (id ID) String() string { return string(id) }

// Value implements driver.Valuer for SQL TEXT boundaries.
func (id ID) Value() (driver.Value, error) { return string(id), nil }

// Scan implements sql.Scanner for SQL TEXT boundaries.
func (id *ID) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*id = ""
		return nil
	case string:
		*id = ID(v)
		return nil
	case []byte:
		*id = ID(string(v))
		return nil
	default:
		return fmt.Errorf("message.ID: scan unsupported %T", src)
	}
}

// Audience is the envelope audience list. The "*" entry is the channel
// wildcard; other entries are channel-local actor ids.
type Audience []actor.ActorID

const AudienceWildcard actor.ActorID = "*"

// IsWildcard reports whether this audience contains the channel wildcard.
func (a Audience) IsWildcard() bool {
	for _, id := range a {
		if id == AudienceWildcard {
			return true
		}
	}
	return false
}

// Strings returns a copy of the wire string values.
func (a Audience) Strings() []string {
	out := make([]string, len(a))
	for i, id := range a {
		out[i] = string(id)
	}
	return out
}
