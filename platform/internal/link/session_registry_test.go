package link

// sessionRegistry unit tests: the one session truth (three-layer index,
// current pointer), the two live credentials, the verdict write (beginSeal)
// with its candidate-only-reason guard, and the candidate TTL exit.

import (
	"testing"
	"time"
)

func activeRecord(t *testing.T, registry *sessionRegistry, key string) *sessionRecord {
	t.Helper()
	record, err := registry.mint(key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.activate(record); err != nil {
		t.Fatal(err)
	}
	return record
}

func sealedReason(
	t *testing.T,
	registry *sessionRegistry,
	generation SessionGeneration,
) SessionEndReason {
	t.Helper()
	for _, snapshot := range registry.snapshots() {
		if snapshot.Generation == generation {
			return snapshot.Reason
		}
	}
	t.Fatalf("generation %s not found in snapshots", generation)
	return ""
}

func TestSessionRegistryOneTruthAndNoFallback(t *testing.T) {
	registry := newSessionRegistry(nil)
	first := activeRecord(t, registry, "daemon-a")
	second := activeRecord(t, registry, "daemon-a")

	if !registry.admit(first).allows() {
		t.Fatal("displaced active session lost Admit authority")
	}
	if registry.authority(first).allows() {
		t.Fatal("displaced session retained Current authority")
	}
	if !registry.authority(second).allows() {
		t.Fatal("successor is not current")
	}
	if registry.beginSeal(second, sessionEvidence{reason: SessionCarrierLost}) != sealCommitted {
		t.Fatal("successor seal did not commit")
	}
	if registry.currentRecord("daemon-a") != nil {
		t.Fatal("current fell back to an older active session")
	}
	if !registry.admit(first).allows() {
		t.Fatal("successor seal mutated the displaced active session")
	}
}

func TestSessionAuthoritiesZeroRejectAndSealCutsBothAnswers(t *testing.T) {
	if (admitAuthority{}).allows() || (currentAuthority{}).allows() {
		t.Fatal("zero authority admitted work")
	}
	registry := newSessionRegistry(nil)
	record := activeRecord(t, registry, "daemon-a")
	admit := registry.admit(record)
	current := registry.authority(record)
	if !admit.allows() || !current.allows() {
		t.Fatal("active current session was rejected")
	}
	if registry.beginSeal(record, sessionEvidence{reason: SessionRevoked}) != sealCommitted {
		t.Fatal("seal did not commit")
	}
	if admit.allows() || current.allows() {
		t.Fatal("a live credential retained a snapshot verdict after seal")
	}
}

func TestSessionGenerationIsCanonicalUUIDv7AndClosedReasonIsRetained(t *testing.T) {
	registry := newSessionRegistry(nil)
	record, err := registry.mint("daemon-a")
	if err != nil {
		t.Fatal(err)
	}
	if registry.beginSeal(record, sessionEvidence{reason: SessionHandshakeTimeout}) != sealCommitted {
		t.Fatal("candidate close did not commit")
	}
	registry.completeSeal(record, 2)
	snapshots := registry.snapshots()
	if len(snapshots) != 1 {
		t.Fatalf("snapshots=%d want 1", len(snapshots))
	}
	got := snapshots[0]
	if got.State != SessionClosed || got.Reason != SessionHandshakeTimeout || got.Abandoned != 2 {
		t.Fatalf("closed snapshot=%+v", got)
	}
	adopted := newSessionRegistry(nil)
	if _, err := adopted.adopt(got.Generation, got.Key); err != nil {
		t.Fatalf("daemon rejected home UUIDv7: %v", err)
	}
	if _, err := adopted.adopt(got.Generation, got.Key); err == nil {
		t.Fatal("daemon accepted a reused session generation")
	}
}

func TestManagementEnumeratesCandidatesAndKicksOneExactGeneration(t *testing.T) {
	registry := newSessionRegistry(nil)
	first, err := registry.mint("daemon-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.mint("daemon-a")
	if err != nil {
		t.Fatal(err)
	}
	acceptor := &Acceptor{sessions: registry, logger: registry.logger}
	if got := acceptor.Sessions(); len(got) != 2 {
		t.Fatalf("candidate sessions=%d want 2", len(got))
	}
	if !acceptor.KickSession(first.generation) {
		t.Fatal("exact candidate kick was rejected")
	}
	select {
	case <-first.sealed:
	default:
		t.Fatal("exact kick did not write the verdict")
	}
	if reason := sealedReason(t, registry, first.generation); reason != SessionRevoked {
		t.Fatalf("first reason=%s", reason)
	}
	select {
	case <-second.sealed:
		t.Fatal("exact kick disturbed the other generation")
	default:
	}
}

func TestCandidateAutomaticallyReportsHandshakeTimeout(t *testing.T) {
	registry := newSessionRegistry(nil)
	registry.candidateTTL = 15 * time.Millisecond
	record, err := registry.mint("daemon-a")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-record.sealed:
		if reason := sealedReason(t, registry, record.generation); reason != SessionHandshakeTimeout {
			t.Fatalf("reason=%s", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("candidate did not leave automatically")
	}
}

// Candidate-only evidence is confirmed under the same lock that writes the
// verdict: a handshake timeout racing a completed activate is refused as
// stale, and the session remains alive for real evidence.
func TestStaleCandidateEvidenceDoesNotSealActiveSession(t *testing.T) {
	registry := newSessionRegistry(nil)
	record := activeRecord(t, registry, "daemon-a")
	if got := registry.beginSeal(record, sessionEvidence{
		reason: SessionHandshakeTimeout, detail: "stale_timer",
	}); got != sealStaleEvidence {
		t.Fatalf("handshake timeout on active session sealed: verdict=%d", got)
	}
	if got := registry.beginSeal(record, sessionEvidence{
		reason: SessionAdmissionRejected, detail: "stale_reject",
	}); got != sealStaleEvidence {
		t.Fatalf("admission rejection on active session sealed: verdict=%d", got)
	}
	if !registry.admit(record).allows() {
		t.Fatal("stale evidence cut admission")
	}
	if got := registry.beginSeal(record, sessionEvidence{
		reason: SessionCarrierLost,
	}); got != sealCommitted {
		t.Fatalf("real evidence refused after stale refusals: verdict=%d", got)
	}
}

// The candidate TTL callback itself is conditional: an activated session's
// expired timer must not write any verdict.
func TestCandidateTimerCallbackIsConditionalOnCandidateState(t *testing.T) {
	registry := newSessionRegistry(nil)
	record := activeRecord(t, registry, "daemon-a")
	registry.reportIfCandidate(record, SessionHandshakeTimeout, "fired_after_activate")
	select {
	case <-record.sealed:
		t.Fatal("active session was sealed by a stale candidate timer")
	default:
	}
	if !registry.admit(record).allows() {
		t.Fatal("stale candidate timer cut admission")
	}
}

// Evidence is never parked where later evidence can be lost behind it: a
// stale candidate-only report on an active session writes nothing, and the
// next real reason still lands with its own attribution.
func TestRealEvidenceStillLandsAfterStaleCandidateReport(t *testing.T) {
	registry := newSessionRegistry(nil)
	record := activeRecord(t, registry, "daemon-a")
	record.report(SessionHandshakeTimeout, "stale_timer", nil)
	select {
	case <-record.sealed:
		t.Fatal("stale candidate evidence sealed an active session")
	default:
	}
	record.report(SessionRevoked, "real_reason", nil)
	select {
	case <-record.sealed:
	default:
		t.Fatal("real evidence was lost behind the stale report")
	}
	if reason := sealedReason(t, registry, record.generation); reason != SessionRevoked {
		t.Fatalf("reason=%s want revoked", reason)
	}
}
