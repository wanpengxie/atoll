package identity

import "testing"

func TestDefaultBcryptCostUsesProductionFloor(t *testing.T) {
	svc := NewService(nil, Config{SessionSecret: "test"})
	if svc.bcryptCost < MinProductionBcryptCost {
		t.Fatalf("bcryptCost=%d want >= %d", svc.bcryptCost, MinProductionBcryptCost)
	}
}
