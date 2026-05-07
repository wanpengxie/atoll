// Package cli 实现 coagent-xhs binary 的 cobra command 层。
//
// 5 个子命令在 `root.go` 注册，各自薄壳：解析 flag → 调 Provider → WriteOK/WriteErr。
// 输出统一 JSON envelope（见 output.go）。
package cli

import (
	"errors"
	"fmt"
	"io"

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

// RunCLI 是 coagent-xhs binary 的可测入口。
//
// 把 cobra 解析失败 / RunE 返回错误统一转成 stdout JSON envelope（永不再写 stderr）：
//   - RunE 返回 *CLIError → envelope 用其 Code/Message
//   - cobra parse / unknown command / 其他 plain error → envelope code="usage_error"
//   - 成功 → 返回 ExitOK
//
// 注意：runWithProvider 内部走 os.Exit；那条路径不会回到这里。本函数只负责入口/解析层。
func RunCLI(args []string, stdout io.Writer) int {
	root := NewRootCommand()
	root.SetOut(stdout)
	root.SetErr(stdout)
	root.SetArgs(args)
	if err := root.Execute(); err != nil {
		var ce *CLIError
		if errors.As(err, &ce) && ce != nil {
			_ = WriteErr(stdout, ce.Code, ce.Message)
		} else {
			_ = WriteErr(stdout, "usage_error", err.Error())
		}
		return ExitUsageError
	}
	return ExitOK
}
