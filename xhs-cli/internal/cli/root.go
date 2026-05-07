// Package cli 实现 coagent-xhs binary 的 cobra command 层。
//
// 5 个子命令在 `root.go` 注册，各自薄壳：解析 flag → 调 Provider → WriteOK/WriteErr。
// 输出统一 JSON envelope（见 output.go）。
package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

// NewRootCommand 构造 coagent-xhs root cobra command。
func NewRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:   "coagent-xhs",
		Short: "coagent xhs CLI",
		Long: fmt.Sprintf(`coagent-xhs：xhs 业务命令行入口。

5 个子命令：publish / search / get-my-recent / get-note / publish-status

provider 切换 env：%s（mock|real，默认 mock）
real 模式必填 env：%s / %s / %s`,
			"COAGENT_XHS_BACKEND",
			"COAGENT_DAEMON_HTTP", "COAGENT_DAEMON_TOKEN", "COAGENT_CHANNEL_ID"),
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.AddCommand(
		newPublishCmd(),
		newSearchCmd(),
		newGetMyRecentCmd(),
		newGetNoteCmd(),
		newPublishStatusCmd(),
	)

	return root
}
