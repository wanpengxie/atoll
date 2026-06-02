package actor

import (
	"fmt"
)

// Kind is the actor physical-position classifier (L0 §2.3 normative):
// the 4-value closed set human / agent / system / tool.
type Kind string

const (
	KindHuman  Kind = "human"
	KindAgent  Kind = "agent"
	KindSystem Kind = "system"
	KindTool   Kind = "tool"
)

// AllKinds enumerates every valid actor kind value, in spec order.
var AllKinds = []Kind{KindHuman, KindAgent, KindSystem, KindTool}

// String returns the wire form.
func (k Kind) String() string { return string(k) }

// Scan implements sql.Scanner for SQL TEXT boundaries.
func (k *Kind) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		*k = ""
		return nil
	case string:
		*k = Kind(v)
		return nil
	case []byte:
		*k = Kind(string(v))
		return nil
	default:
		return fmt.Errorf("actor.Kind: scan unsupported %T", src)
	}
}
