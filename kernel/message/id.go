package message

import (
	"github.com/wanpengxie/ActOS/kernel/actor"
)

// ID is the envelope.message identifier wire form.
type ID string

// String returns the wire form.
func (id ID) String() string { return string(id) }

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
