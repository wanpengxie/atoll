// coagent-xhs binary entrypoint.
//
// 提供 5 个 xhs 子命令（publish / search / get-my-recent / get-note /
// publish-status），按 COAGENT_XHS_BACKEND env 切换 mock/real provider。
package main

import (
	"fmt"
	"os"

	"github.com/coagent-ai/xhs-cli/internal/cli"
)

func main() {
	root := cli.NewRootCommand()
	if err := root.Execute(); err != nil {
		// cobra 已经在 RunE 里通过 cli.WriteErr 打了 envelope；
		// 这里只兜底处理 cobra 自身解析阶段错误。
		fmt.Fprintln(os.Stderr, err)
		os.Exit(cli.ExitUsageError)
	}
}
