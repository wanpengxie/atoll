package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

func openKVRegistry(t *testing.T) *resourceRegistry {
	t.Helper()
	db, err := openSqlite(t.Context(), filepath.Join(t.TempDir(), "resources.sqlite"), OpenOptions{}, ChannelLocalDDL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return newResourceRegistry(db)
}

func createKVRow(t *testing.T, reg *resourceRegistry, id resource.ResourceID, value []byte) {
	t.Helper()
	if err := reg.Create(t.Context(), id, resourcespec.KindKV, actor.ActorID("agent:a"), value); err != nil {
		t.Fatalf("Create(%q): %v", id, err)
	}
}

func TestKVRegistryListPaginatesWithoutRepeatsOrGaps(t *testing.T) {
	reg := openKVRegistry(t)
	reg.nowMs = func() int64 { return 1000 }
	want := []resource.ResourceID{"kv:a", "kv:b", "kv:c", "kv:d", "kv:e"}
	for _, id := range want {
		createKVRow(t, reg, id, nil)
	}

	var got []resource.ResourceID
	cursor := ""
	for page := 0; page < 10; page++ {
		rows, next, err := reg.List(t.Context(), "kv:", 2, cursor)
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		for _, row := range rows {
			got = append(got, row.ID)
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if len(got) != len(want) {
		t.Fatalf("listed ids = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("listed ids = %v, want %v", got, want)
		}
	}
}

func TestKVRegistryListEscapesPrefixMetacharacters(t *testing.T) {
	reg := openKVRegistry(t)
	for _, id := range []resource.ResourceID{
		"kv:100%off", "kv:100Xoff", "kv:a_b", "kv:axb", `kv:a\\b`, "kv:ab",
	} {
		createKVRow(t, reg, id, nil)
	}
	tests := []struct {
		prefix resource.ResourceID
		want   resource.ResourceID
	}{
		{"kv:100%", "kv:100%off"},
		{"kv:a_", "kv:a_b"},
		{`kv:a\`, `kv:a\\b`},
	}
	for _, test := range tests {
		rows, _, err := reg.List(t.Context(), string(test.prefix), 10, "")
		if err != nil {
			t.Fatalf("prefix %q: %v", test.prefix, err)
		}
		if len(rows) != 1 || rows[0].ID != test.want {
			t.Fatalf("prefix %q rows = %+v, want only %q", test.prefix, rows, test.want)
		}
	}
}

func TestKVRegistryListRejectsMalformedCursor(t *testing.T) {
	reg := openKVRegistry(t)
	_, _, err := reg.List(t.Context(), "", 50, "not-a-valid-cursor!!")
	if !errors.Is(err, resourcespec.ErrMalformedCursor) {
		t.Fatalf("List error = %v, want ErrMalformedCursor", err)
	}
}

func TestKVDriverDistinguishesNullEmptyAndMissingValues(t *testing.T) {
	reg := openKVRegistry(t)
	driver := newKVDriver(reg.db)
	createKVRow(t, reg, "kv:null", nil)
	createKVRow(t, reg, "kv:empty", []byte{})

	if value, found, err := driver.Read(t.Context(), "kv:null"); err != nil || found || value != nil {
		t.Fatalf("null read = (%q, %v, %v), want (nil, false, nil)", value, found, err)
	}
	if value, found, err := driver.Read(t.Context(), "kv:empty"); err != nil || !found || value == nil || len(value) != 0 {
		t.Fatalf("empty read = (%q, %v, %v), want (non-nil empty, true, nil)", value, found, err)
	}
	if value, found, err := driver.Read(t.Context(), "kv:missing"); err != nil || found || value != nil {
		t.Fatalf("missing read = (%q, %v, %v), want (nil, false, nil)", value, found, err)
	}
}

func TestKVDriverWriteToMissingRowFails(t *testing.T) {
	reg := openKVRegistry(t)
	err := newKVDriver(reg.db).Write(context.Background(), "kv:missing", []byte("value"))
	if err == nil {
		t.Fatal("write to missing row succeeded")
	}
}
