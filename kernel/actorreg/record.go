package actorreg

import "github.com/wanpengxie/ActOS/kernel/actor"

// Record is the channel-local actor row exposed via the registry query
// API (L1 §12.2 minimum field set).
//
// `Binding` is empty string for human / system actors per L1 §12.2. The
// SQL CHECK in L2 §1.4.6 keeps the column NULL for those rows; this
// kernel-level interface uses zero-value (empty string) to mean the same.
type Record struct {
	ID             actor.ActorID
	Kind           actor.Kind
	Binding        actor.Binding // empty for human / system
	DisplayName    string        // optional; informative only (L1 §12.2 fields optional)
	CreatedAt      int64
	DeregisteredAt int64 // 0 = active; non-zero = soft-deregister timestamp
}

// IsActive reports whether the actor is still active per L1 §12.2.
func (r Record) IsActive() bool { return r.DeregisteredAt == 0 }
