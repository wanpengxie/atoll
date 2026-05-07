package cli

import (
	"context"
	"os"
	"strings"

	"github.com/coagent-ai/xhs-cli/internal/xhs"
	"github.com/spf13/cobra"
)

// newPublishCmd 注册 `coagent-xhs publish` 子命令。
//
//	coagent-xhs publish --title <T> --content <path> [--images <paths>] [--tags <tags>]
//
// `--content` 是 markdown 文件路径；mock/real 行为：
//   - mock：读文件为 string 后塞 PublishArgs.Content，note_id 用新 ulid
//   - real：把文件路径塞 PublishArgs.ContentPath，daemon 端去读
//
// 注：因为目前 daemon 端 RPC 还没实现（T3），real 路径仅 dispatch 一个 ack。
func newPublishCmd() *cobra.Command {
	var (
		title       string
		contentPath string
		imagesCSV   string
		tagsCSV     string
	)

	cmd := &cobra.Command{
		Use:   "publish",
		Short: "发布一篇笔记（real 模式 dispatch，mock 模式同步返回）",
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(title) == "" {
				return NewCLIError("invalid_argument", "--title is required")
			}
			if strings.TrimSpace(contentPath) == "" {
				return NewCLIError("invalid_argument", "--content is required")
			}

			content, err := os.ReadFile(contentPath)
			if err != nil {
				return NewCLIError("content_read_failed", "read content: %s", err)
			}

			pubArgs := xhs.PublishArgs{
				Title:       title,
				ContentPath: contentPath,
				Content:     string(content),
				Images:      splitCSV(imagesCSV),
				Tags:        splitCSV(tagsCSV),
			}

			runWithProvider(cmd, func(ctx context.Context, p xhs.Provider) (any, error) {
				return p.Publish(ctx, pubArgs)
			})
			return nil
		},
	}

	cmd.Flags().StringVarP(&title, "title", "t", "", "笔记标题（必填）")
	cmd.Flags().StringVarP(&contentPath, "content", "c", "", "笔记正文文件路径（必填）")
	cmd.Flags().StringVar(&imagesCSV, "images", "", "图片路径（逗号分隔）")
	cmd.Flags().StringVar(&tagsCSV, "tags", "", "标签（逗号分隔）")

	return cmd
}

// splitCSV 把 "a,b , c" 切成 ["a","b","c"]，过滤空串。
func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}
