package sysactor

import (
	"context"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// SystemOps is the direct realm-to-system transport adapter. The implementation
// is the unexported Home OpEntry; callers receive this interface only through
// ChannelHost assembly.
type SystemOps interface {
	Admit(context.Context, channelspec.AdmitRequest) (channel.AdmitResult, error)
	Introduce(context.Context, channelspec.IntroduceRequest) (channel.IntroduceResult, error)
	Remove(context.Context, channelspec.RemoveRequest) (channel.RemoveResult, error)
	AttachDaemon(context.Context, channelspec.DaemonRequest) (channelspec.BindingResult, error)
	DetachDaemon(context.Context, channelspec.DaemonRequest) (channelspec.BindingResult, error)
}
