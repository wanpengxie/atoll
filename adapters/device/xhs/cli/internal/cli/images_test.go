package cli

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// minimal valid PNG header so http.DetectContentType returns "image/png".
// 8 bytes signature + IHDR chunk start.
var fixturePNG = []byte{
	0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, // PNG signature
	0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52, // IHDR length + type
	0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, // 1x1 pixel
	0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4, // bit depth+color+...
	0x89, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, // IEND
	0x44, 0xAE, 0x42, 0x60, 0x82,
}

// minimal SOI + APP0 (JFIF) so http.DetectContentType returns "image/jpeg".
var fixtureJPEG = []byte{
	0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00,
	0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0xFF, 0xD9,
}

// TestPublishRealMode_ImageDataNormalized（fix-spec.md §Fix-T1.2）：
// CLI 在 real 模式下把 --images <paths> 逐个 base64 编码 + 包成
// {type:"data", value:"data:<mime>;base64,...", fileName:"<basename>"}
// 与 chrome-extension publish-content.ts 的 createFileFromResource 期望对齐。
func TestPublishRealMode_ImageDataNormalized(t *testing.T) {
	dir := t.TempDir()
	pngPath := filepath.Join(dir, "cover.png")
	jpgPath := filepath.Join(dir, "shot.jpg")
	if err := os.WriteFile(pngPath, fixturePNG, 0o644); err != nil {
		t.Fatalf("write png: %v", err)
	}
	if err := os.WriteFile(jpgPath, fixtureJPEG, 0o644); err != nil {
		t.Fatalf("write jpg: %v", err)
	}

	out, err := normalizeImagesForRPC([]string{pngPath, jpgPath})
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(out))
	}

	checkEntry := func(idx int, wantPrefix, wantName string) {
		t.Helper()
		entry := out[idx]
		if entry["type"] != "data" {
			t.Fatalf("entry[%d].type want 'data', got %v", idx, entry["type"])
		}
		val, ok := entry["value"].(string)
		if !ok {
			t.Fatalf("entry[%d].value not string: %T", idx, entry["value"])
		}
		if !strings.HasPrefix(val, wantPrefix) {
			t.Fatalf("entry[%d].value should have prefix %q, got %q", idx, wantPrefix, val)
		}
		if entry["fileName"] != wantName {
			t.Fatalf("entry[%d].fileName want %q, got %v", idx, wantName, entry["fileName"])
		}
	}
	checkEntry(0, "data:image/png;base64,", "cover.png")
	checkEntry(1, "data:image/jpeg;base64,", "shot.jpg")
}

func TestNormalizeImagesForRPC_FileMissing(t *testing.T) {
	_, err := normalizeImagesForRPC([]string{"/no/such/file.png"})
	if err == nil {
		t.Fatal("expected error for missing file")
	}
	var ce *CLIError
	if !errors.As(err, &ce) || ce.Code != "image_read_failed" {
		t.Fatalf("expected image_read_failed CLIError, got %v", err)
	}
}

func TestNormalizeImagesForRPC_EmptyInput(t *testing.T) {
	out, err := normalizeImagesForRPC(nil)
	if err != nil {
		t.Fatalf("normalize nil: %v", err)
	}
	if out != nil {
		t.Fatalf("expected nil for empty input, got %v", out)
	}
}
