package message

import (
	"github.com/wanpengxie/atoll/protocol/actor"
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

// Contains reports whether id is an explicit member of the audience. Membership
// is exact id equality only — the audience carries no wildcard or multicast
// interpretation (the `"*"` broadcast form is removed from the closed set; see
// the type doc), so a receiver decides "am I addressed" by this literal check.
func (a Audience) Contains(id actor.ActorID) bool {
	for _, member := range a {
		if member == id {
			return true
		}
	}
	return false
}
