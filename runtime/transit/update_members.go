package transit

import "github.com/wanpengxie/ActOS/kernel/daemonbus"

// UpdateMembersBody is the daemonbus `control.update_members` payload.
type UpdateMembersBody = daemonbus.UpdateMembersBody

// UpdateMembersAckBody is the daemon -> server reply for
// `control.update_members`.
type UpdateMembersAckBody = daemonbus.UpdateMembersAckBody
