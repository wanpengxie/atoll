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

// Audience is the envelope audience list. Each entry is a channel-local
// actor id; the wildcard `"*"` is removed from the closed set — broadcast
// must be expressed by enumerating the explicit receiver actor_id list.
// Self-scheduling is expressed by including the sender's own actor_id.
type Audience []actor.ActorID

// Strings returns a copy of the wire string values.
func (a Audience) Strings() []string {
	out := make([]string, len(a))
	for i, id := range a {
		out[i] = string(id)
	}
	return out
}
