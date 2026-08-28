package boundedjson

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestProjectPreservesSmallFactsAndCutsLargeValues(t *testing.T) {
	raw, err := json.Marshal(map[string]any{
		"status": "completed",
		"path":   "/channel/image.png",
		"image":  strings.Repeat("a", 20000),
		"audio":  strings.Repeat("b", 12000),
	})
	if err != nil {
		t.Fatal(err)
	}
	projected, meta, err := Project(raw, 10<<10)
	if err != nil {
		t.Fatal(err)
	}
	if len(projected) > 10<<10 || !json.Valid(projected) {
		t.Fatalf("projection bytes=%d valid=%v", len(projected), json.Valid(projected))
	}
	var got map[string]any
	if err := json.Unmarshal(projected, &got); err != nil {
		t.Fatal(err)
	}
	if got["status"] != "completed" || got["path"] != "/channel/image.png" {
		t.Fatalf("small facts lost: %s", projected)
	}
	if !meta.Projected || meta.OriginalBytes != len(raw) || len(meta.SHA256) != 64 {
		t.Fatalf("metadata = %+v", meta)
	}
}

func TestProjectPreservesSmallJSONByteForByte(t *testing.T) {
	raw := []byte(`{ "status": "ok", "n": 1 }`)
	got, meta, err := Project(raw, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(raw) || meta.Projected {
		t.Fatalf("got=%q meta=%+v", got, meta)
	}
}

func TestProjectBoundsHugeArrayObjectKeysAndUnicode(t *testing.T) {
	value := map[string]any{"状态": "完成", strings.Repeat("键", 2000): strings.Repeat("值", 8000)}
	items := make([]any, 2000)
	for i := range items {
		items[i] = value
	}
	raw, _ := json.Marshal(map[string]any{"ok": true, "items": items})
	got, _, err := Project(raw, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) > 4096 || !json.Valid(got) {
		t.Fatalf("projection bytes=%d valid=%v", len(got), json.Valid(got))
	}
	if !strings.Contains(string(got), `"ok":true`) {
		t.Fatalf("small sibling lost: %s", got)
	}
}

func TestProjectRejectsInvalidJSONAndImpossibleBudget(t *testing.T) {
	if _, _, err := Project([]byte(`{"x":`), 1024); err == nil {
		t.Fatal("invalid JSON accepted")
	}
	if _, _, err := Project([]byte(`{"x":"long"}`), 1); err == nil {
		t.Fatal("impossible budget accepted")
	}
}

func TestProjectAlwaysKeepsTerminalStatusAheadOfManySmallKeys(t *testing.T) {
	value := map[string]any{"status": "completed", "text": strings.Repeat("x", 10000)}
	for i := 0; i < 2000; i++ {
		value[strings.Repeat("a", i%20)+string(rune(0x1000+i))] = i
	}
	raw, _ := json.Marshal(value)
	got, _, err := Project(raw, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var projected map[string]any
	if err := json.Unmarshal(got, &projected); err != nil {
		t.Fatal(err)
	}
	if projected["status"] != "completed" {
		t.Fatalf("terminal status lost: %s", got)
	}
}
