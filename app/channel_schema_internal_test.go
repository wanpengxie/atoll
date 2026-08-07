package app

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestChannelDirectorySchemaIsRealmScoped(t *testing.T) {
	db, err := openTestAppDB(t, filepath.Join(t.TempDir(), "app.db"))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`PRAGMA table_info(channels)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notnull, &defaultValue, &pk); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, name)
	}
	want := []string{"id", "name", "type", "status", "owner_principal", "spec_json", "created_at", "parent_id"}
	if !reflect.DeepEqual(columns, want) {
		t.Fatalf("channels columns=%v want %v", columns, want)
	}
	for _, fragmented := range []string{"work" + "spaces", "work" + "space_members"} {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, fragmented).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("retired table %q still exists", fragmented)
		}
	}
}
