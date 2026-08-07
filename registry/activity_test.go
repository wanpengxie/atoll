package registry

import "testing"

func TestActivityEndedStatusVocabulariesAreSeparate(t *testing.T) {
	for _, s := range []string{ActivityTurnEndedStatusOK, ActivityTurnEndedStatusFailed, ActivityTurnEndedStatusInterrupted} {
		if !IsActivityTurnEndedStatus(s) {
			t.Fatalf("turn status %q rejected", s)
		}
	}
	if IsActivityTurnEndedStatus(ActivityToolEndedStatusCompleted) {
		t.Fatal("tool completed leaked into turn vocabulary")
	}
	for _, s := range []string{ActivityToolEndedStatusCompleted, ActivityToolEndedStatusFailed} {
		if !IsActivityToolEndedStatus(s) {
			t.Fatalf("tool status %q rejected", s)
		}
	}
	if IsActivityToolEndedStatus(ActivityTurnEndedStatusOK) || IsActivityToolEndedStatus(ActivityTurnEndedStatusInterrupted) {
		t.Fatal("turn statuses leaked into tool vocabulary")
	}
}
