package cli

import (
	"context"

	"github.com/coagent-ai/xhs-cli/internal/xhs"
	"github.com/spf13/cobra"
)

// newSyncCookieCmd 注册 `coagent-xhs sync-cookie`（v4 type:
// xhs.cookie.sync）。
//
// L4 §2.1.2 把旧 `publish-status` 命令从业务 type 表里删除，并新增
// `xhs.cookie.sync` 类型；CLI surface 同步把 publish-status 替换成
// sync-cookie。
func newSyncCookieCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sync-cookie",
		Short: "同步 xhs 帐号 cookie（real 模式 dispatch，mock 模式同步返回）",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			runWithProvider(cmd, func(ctx context.Context, p xhs.Provider) (any, error) {
				return p.SyncCookie(ctx, xhs.SyncCookieArgs{})
			})
			return nil
		},
	}
	return cmd
}
