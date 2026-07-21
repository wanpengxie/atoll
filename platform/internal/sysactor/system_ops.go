package sysactor

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// SystemOps is the direct realm-to-system transport adapter. The implementation
// is the unexported Home OpEntry; callers receive this interface only through
// ChannelHost assembly.
type SystemOps interface {
	Admit(context.Context, channel.AdmitRequest) (channel.AdmitResult, error)
	Introduce(context.Context, channel.IntroduceRequest) (channel.IntroduceResult, error)
	AttachDaemon(context.Context, channel.DaemonRequest) (channel.BindingResult, error)
	DetachDaemon(context.Context, channel.DaemonRequest) (channel.BindingResult, error)
	ApplyDeclVersion(context.Context, channel.ApplyDeclVersionRequest) (channel.ApplyDeclVersionResult, error)
	RevokeDeclTargets(context.Context, channel.RevokeDeclRequest) (channel.RevokeResult, error)
	RevokeDaemon(context.Context, channel.DaemonRequest) (channel.RevokeResult, error)
	// DeclaredBySourceSerialized answers "which live instances did this decl
	// introduce" from INSIDE the operation serial section: an absent verdict
	// taken here can never interleave with an in-flight member introduce — the
	// judge and the birth share one queue (判断段与落账段恒同区).
	DeclaredBySourceSerialized(context.Context, string) ([]storespec.ActorControlRow, error)
}
