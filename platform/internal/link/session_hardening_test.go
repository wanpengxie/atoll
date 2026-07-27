package link

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
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
	case evidence := <-first.death:
		if evidence.reason != SessionRevoked {
			t.Fatalf("first reason=%s", evidence.reason)
		}
	default:
		t.Fatal("first candidate did not receive revoke")
	}
	select {
	case <-second.death:
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
	case evidence := <-record.death:
		if evidence.reason != SessionHandshakeTimeout {
			t.Fatalf("reason=%s", evidence.reason)
		}
	case <-time.After(time.Second):
		t.Fatal("candidate did not leave automatically")
	}
}

func TestControlTaskPoolBusyIsLocalAndSeatsFollowRealGoroutines(t *testing.T) {
	var evidence bool
	pool := newControlTaskPool(nil, func(SessionEndReason, string, error) { evidence = true })
	release := make(chan struct{})
	for i := 0; i < controlTaskCapacity; i++ {
		if !pool.submit(func() { <-release }, nil) {
			t.Fatalf("task %d unexpectedly rejected", i)
		}
	}
	busy := make(chan struct{}, 1)
	if pool.submit(func() {}, func() { busy <- struct{}{} }) {
		t.Fatal("full task pool admitted excess work")
	}
	select {
	case <-busy:
	default:
		t.Fatal("full task pool did not return busy immediately")
	}
	joined, abandoned := pool.drain(20 * time.Millisecond)
	if joined || abandoned != controlTaskCapacity {
		t.Fatalf("drain joined=%v abandoned=%d", joined, abandoned)
	}
	if evidence {
		t.Fatal("task-pool saturation escalated to session death")
	}
	if got := pool.active.Load(); got != controlTaskCapacity {
		t.Fatalf("active seats=%d want %d before real goroutines exit", got, controlTaskCapacity)
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for pool.active.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := pool.active.Load(); got != 0 {
		t.Fatalf("active seats remained after real exit: %d", got)
	}
}

func TestProbeReplyBypassesSaturatedControlTaskPool(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	ls := &linkSession{ctrl: left}
	ls.controlTasks = newControlTaskPool(nil, nil)
	ls.openSeats = make(chan struct{}, openAttemptCapacity)
	release := make(chan struct{})
	for i := 0; i < controlTaskCapacity; i++ {
		ls.controlTasks.submit(func() { <-release }, nil)
	}
	done := make(chan struct{})
	go func() {
		ls.readControl(left)
		close(done)
	}()
	probe, _ := encodeControl(controlFrame{Kind: ctrlProbe, Probe: &Probe{Nonce: "n-1"}})
	go func() { _, _ = right.Write(append(probe, '\n')) }()
	_ = right.SetReadDeadline(time.Now().Add(time.Second))
	var reply controlFrame
	if err := json.NewDecoder(right).Decode(&reply); err != nil {
		t.Fatalf("read direct probe reply: %v", err)
	}
	if reply.Kind != ctrlProbeReply || reply.ProbeReply == nil || reply.ProbeReply.Nonce != "n-1" {
		t.Fatalf("probe reply=%+v", reply)
	}
	close(release)
	_ = right.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("control reader did not exit")
	}
}

func TestMalformedControlReportsEvidenceWithoutMechanismKill(t *testing.T) {
	carrierA, carrierB := net.Pipe()
	client, err := yamux.Client(carrierA, linkYamuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	server, err := yamux.Server(carrierB, linkYamuxConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer server.Close()
	evidence := make(chan SessionEndReason, 1)
	ls := newLinkSession(client, nil, nil, nil, nil,
		func(reason SessionEndReason, _ string, _ error) { evidence <- reason }, nil)
	reader, writer := net.Pipe()
	defer reader.Close()
	go ls.readControl(reader)
	if _, err := writer.Write([]byte("not-json\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case reason := <-evidence:
		if reason != SessionProtocolViolation {
			t.Fatalf("reason=%s", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("decode evidence was not reported")
	}
	select {
	case <-client.CloseChan():
		t.Fatal("link mechanism killed the carrier before an owner decision")
	default:
	}
	_ = writer.Close()
}

func TestOpenCapacityBusyAndCancellationDoNotReportSessionDeath(t *testing.T) {
	ls := &linkSession{
		openSeats: make(chan struct{}, openAttemptCapacity),
		evidence: func(SessionEndReason, string, error) {
			t.Fatal("local open pressure reported session death")
		},
	}
	for i := 0; i < openAttemptCapacity; i++ {
		ls.openSeats <- struct{}{}
	}
	if _, err := ls.openTagged(context.Background(), streamLane); !errors.Is(err, ErrOpenBusy) {
		t.Fatalf("open error=%v want busy", err)
	}
}

func TestYamuxConnectionWriteTimeoutIsCarrierEvidence(t *testing.T) {
	if !isConnectionWriteTimeout(yamux.ErrConnectionWriteTimeout) {
		t.Fatal("yamux connection write timeout was not classified as carrier loss")
	}
}
