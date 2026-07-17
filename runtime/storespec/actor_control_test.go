package storespec

import "testing"

func TestPlacementClosedShape(t *testing.T) {
	tests := []struct {
		name string
		p    Placement
		ok   bool
	}{
		{name: "server", p: NewServerPlacement(), ok: true},
		{name: "server with host", p: Placement{Kind: PlacementServer, Host: "d1"}},
		{name: "daemon", p: Placement{Kind: PlacementDaemon, Host: "d1"}, ok: true},
		{name: "daemon without host", p: Placement{Kind: PlacementDaemon}},
		{name: "unknown", p: Placement{Kind: "somewhere"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.Validate() == nil; got != tt.ok {
				t.Fatalf("Validate success = %v, want %v", got, tt.ok)
			}
		})
	}
	if _, err := NewDaemonPlacement(""); err == nil {
		t.Fatal("NewDaemonPlacement accepted an empty host")
	}
}
