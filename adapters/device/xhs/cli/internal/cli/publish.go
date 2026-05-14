package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/coagent-ai/coagent/adapters/device/xhs/cli/internal/xhs"
	"github.com/spf13/cobra"
)

// newPublishCmd 注册 `coagent-xhs publish` 子命令。
//
//	coagent-xhs publish --title <T> --content <path> [--images <paths>] [--tags <tags>]
//
// `--content` 是 markdown 文件路径；mock/real 行为：
//   - mock：CLI 端读文件为 string 后塞 PublishArgs.Content；Images 用 string paths（mock 不消费）。
//   - real：CLI 端不读 content 文件，把 content 路径解析成 absolute 后塞 PublishArgs.ContentPath；
//     Images 逐个 base64 编码归一化为 ImageData = [{type:data, value:data:..., fileName}]，
//     daemon 端按 abs path 读盘 + 把 images 透传给 extension（spec §5.1.5）。
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

			images := splitCSV(imagesCSV)
			tags := splitCSV(tagsCSV)

			runWithProvider(cmd, func(ctx context.Context, p xhs.Provider) (any, error) {
				pubArgs := xhs.PublishArgs{
					Title: title,
					Tags:  tags,
				}

				if p.Name() == "real" {
					// real 模式：abs path + image data 归一化；不发 inline content。
					abs, err := filepath.Abs(contentPath)
					if err != nil {
						return nil, NewCLIError("invalid_argument", "resolve absolute content path %q: %s", contentPath, err)
					}
					pubArgs.ContentPath = abs

					normalized, err := normalizeImagesForRPC(images)
					if err != nil {
						return nil, err
					}
					pubArgs.ImageData = normalized
				} else {
					// mock 模式：CLI 读文件 + 直接传 paths（mock 不消费 images）。
					content, err := os.ReadFile(contentPath)
					if err != nil {
						return nil, NewCLIError("content_read_failed", "read content: %s", err)
					}
					pubArgs.ContentPath = contentPath
					pubArgs.Content = string(content)
					pubArgs.Images = images
				}

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
