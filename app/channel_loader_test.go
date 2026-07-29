package app

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelhost"
)

func TestStart_ConvergesMissingChannelImageFromDesiredValue(t *testing.T) {
	dir := t.TempDir()
	db, err := openTestAppDB(t, filepath.Join(dir, "app.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, stmt := range []string{
		`INSERT INTO users(id,email,password,created_at) VALUES ('u','u@x','x',1)`,
		`INSERT INTO channels(id,name,type,status,owner_principal,spec_json,created_at,parent_id)
		 VALUES ('c','c','group','present','u',
		 '{"channel_id":"c","type":"group","owner_principal":"u","created_at":1}',1,NULL)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("fixture %q: %v", stmt, err)
		}
	}

	a, err := New(Config{DB: db, HostFactory: func(deps channelhost.HomeDeps) (channelhost.LocalHost, error) {
		return channelhost.New(dir, deps)
	}})
	if err != nil {
		t.Fatalf("one missing image blocked realm startup: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	// Construction wires but never runs; the boot full scan belongs to Start
	// (assembly-complete boundary).
	a.Start()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := a.host.Acquire("c"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(fmt.Errorf("desired channel did not converge to serving"))
		}
		time.Sleep(10 * time.Millisecond)
	}
}
