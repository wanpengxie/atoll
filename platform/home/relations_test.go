package home

import (
	"context"
	"fmt"
	"github.com/wanpengxie/atoll/platform/channelmember"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/storespec"
	"sync"
	"testing"
)

func TestRelationshipUniquenessAcrossDeclarationsAndConcurrentIntroductions(t *testing.T) {
	h := openAdmissionHome(t, "relations")
	var wg sync.WaitGroup
	results := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := h.actors.Introduce(context.Background(), actorctl.IntroduceRequest{DeclID: fmt.Sprintf("alias-%d", i), Seed: "other", Kind: actor.KindChannel, Definition: storespec.ActorDefinition{Class: fmt.Sprintf("custom-seat-%d", i), Config: []byte(`{"arbitrary_business_config":true}`)}, Placement: storespec.NewServerPlacement()})
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)
	succeeded := 0
	for err := range results {
		if err == nil {
			succeeded++
		}
	}
	if succeeded != 1 {
		t.Fatalf("created %d seats for same relation", succeeded)
	}
	// Same two channels, distinct protocol: peer may coexist with the seat.
	_, err := h.actors.Introduce(context.Background(), actorctl.IntroduceRequest{DeclID: "peer-other", Seed: "other", Kind: actor.KindPeer, Definition: storespec.ActorDefinition{Class: "peeractor", Config: []byte(`{"channel":"other"}`)}, Placement: storespec.NewServerPlacement()})
	if err != nil {
		t.Fatal(err)
	}
	// A different body is a different relation, independent of class/singleton.
	second, err := h.actors.Introduce(context.Background(), actorctl.IntroduceRequest{DeclID: "another-body", Seed: "another-body", Kind: actor.KindChannel, Definition: storespec.ActorDefinition{Class: channelmember.SeatClass, Config: []byte(`{"body":"another"}`)}, Placement: storespec.NewServerPlacement()})
	if err != nil {
		t.Fatal(err)
	}
	err = h.actors.ApplyDeclaration(context.Background(), actorctl.DeclarationChange{ActorID: second.ActorID, Definition: storespec.ActorDefinition{Class: channelmember.SeatClass, Config: []byte(`{"body":"other"}`)}})
	if err != nil {
		t.Fatalf("business configuration was interpreted as relation identity: %v", err)
	}
	if err := h.actors.checkChannelMember("another-body"); err == nil {
		t.Fatal("configuration change altered the existing channel member identity")
	}
}
