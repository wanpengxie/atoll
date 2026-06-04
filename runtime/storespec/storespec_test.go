package storespec

import "testing"

// TestAppendError_Error covers both branches of AppendError.Error():
// Detail present (Reason + ": " + Detail) and Detail empty (Reason alone).
func TestAppendError_Error(t *testing.T) {
	t.Run("with detail", func(t *testing.T) {
		e := &AppendError{
			Reason:           "terminal_duplicate",
			Detail:           "row already exists",
			PartialMessageID: "m-1",
		}
		got := e.Error()
		want := "terminal_duplicate: row already exists"
		if got != want {
			t.Fatalf("Error() = %q, want %q", got, want)
		}
	})

	t.Run("without detail", func(t *testing.T) {
		e := &AppendError{Reason: "terminal_duplicate"}
		got := e.Error()
		want := "terminal_duplicate"
		if got != want {
			t.Fatalf("Error() = %q, want %q", got, want)
		}
	})

	// AppendError must satisfy the error interface (it is used as a typed error).
	var err error = &AppendError{Reason: "x"}
	if err.Error() != "x" {
		t.Fatalf("error interface dispatch = %q, want %q", err.Error(), "x")
	}
}

// TestRecord_IsActive covers Record.IsActive(): DeregisteredAt == 0 means
// active, any nonzero value means deregistered.
func TestRecord_IsActive(t *testing.T) {
	active := Record{DeregisteredAt: 0}
	if !active.IsActive() {
		t.Fatalf("IsActive() = false for DeregisteredAt=0, want true")
	}

	gone := Record{DeregisteredAt: 1700000000000}
	if gone.IsActive() {
		t.Fatalf("IsActive() = true for DeregisteredAt!=0, want false")
	}
}
