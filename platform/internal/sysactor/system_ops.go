package sysactor

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/channel"
)

// SystemOps is the direct realm-to-system transport adapter. The implementation
// is the unexported Home OpEntry; callers receive this interface only through
// ChannelHost assembly.
type SystemOps interface {
	Admit(context.Context, channel.AdmitRequest) (channel.AdmitResult, error)
	Introduce(context.Context, channel.IntroduceRequest) (channel.IntroduceResult, error)
	Remove(context.Context, channel.RemoveRequest) (channel.RemoveResult, error)
	AttachDaemon(context.Context, channel.DaemonRequest) (channel.BindingResult, error)
	DetachDaemon(context.Context, channel.DaemonRequest) (channel.BindingResult, error)
	ApplyDeclVersion(context.Context, channel.ApplyDeclVersionRequest) (channel.ApplyDeclVersionResult, error)
	RevokeDeclTargets(context.Context, channel.RevokeDeclRequest) (channel.RevokeResult, error)
	RevokeDaemon(context.Context, channel.DaemonRequest) (channel.RevokeResult, error)
}
