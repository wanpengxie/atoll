package cli

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
)

// normalizeImagesForRPC 把 CLI 收到的 image 文件路径数组转成 daemon RPC 期望的对象数组。
//
// 输出 shape 对齐 devices/xhs-extension publish-content.ts 的 createFileFromResource：
//
//	{ "type": "data", "value": "data:<mime>;base64,<b64>", "fileName": "<basename>" }
//
// real 模式 publish 用此 helper 把 --images <paths> 在 CLI 端归一化好再塞 RPC，
// daemon 端不再做文件读取（agent cwd != daemon cwd 也安全）。mock 模式不需调用。
func normalizeImagesForRPC(paths []string) ([]map[string]any, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	out := make([]map[string]any, 0, len(paths))
	for i, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, NewCLIError("image_read_failed", "read image[%d] %q: %s", i, p, err)
		}
		// http.DetectContentType 看前 512 字节嗅探 MIME；图片基本都能识别。
		mt := http.DetectContentType(data)
		b64 := base64.StdEncoding.EncodeToString(data)
		out = append(out, map[string]any{
			"type":     "data",
			"value":    fmt.Sprintf("data:%s;base64,%s", mt, b64),
			"fileName": filepath.Base(p),
		})
	}
	return out, nil
}
