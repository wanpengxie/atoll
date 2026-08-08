package driverproto

import "testing"

func TestResultConstructorsEnforceAmbiguousRetirement(t *testing.T) {
	for _, r := range []interface{ Validate() error }{
		OpenUncertain(FailureTransport, "timeout"),
		StartUncertain(FailureTransport, "timeout"),
		ControlUncertain(FailureTransport, "timeout"),
	} {
		if err := r.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	if err := (OpenResult{}).Validate(); err == nil {
		t.Fatal("zero open result accepted")
	}
	if err := (StartResult{}).Validate(); err == nil {
		t.Fatal("zero start result accepted")
	}
	if err := (ControlResult{}).Validate(); err == nil {
		t.Fatal("zero control result accepted")
	}
}

func TestWorkerTurnTargetNeedsBothIdentities(t *testing.T) {
	if (WorkerTurnTarget{Attempt: 1}).Valid() {
		t.Fatal("attempt alone is not a target")
	}
	if (WorkerTurnTarget{Native: "native"}).Valid() {
		t.Fatal("native ref alone is not a target")
	}
	if !(WorkerTurnTarget{Attempt: 1, Native: "native"}).Valid() {
		t.Fatal("complete target rejected")
	}
}
