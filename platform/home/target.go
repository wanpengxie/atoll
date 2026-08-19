package home

import (
	"strings"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func resolveActorTarget(target string, active []storespec.ActiveIdentity) (actor.ActorID, error) {
	if target == string(actor.SystemActorID) {
		return actor.SystemActorID, nil
	}
	parts := strings.Split(target, ":")
	if len(parts) < 1 || len(parts) > 3 {
		return "", &actorbase.TargetResolveError{Code: "invalid_args", Target: target}
	}
	for _, part := range parts {
		if part == "" {
			return "", &actorbase.TargetResolveError{Code: "invalid_args", Target: target}
		}
	}
	matches := make([]actor.ActorID, 0, 1)
	for _, identity := range active {
		member := strings.Split(string(identity.ID), ":")
		if len(member) != 3 {
			continue
		}
		matched := false
		switch len(parts) {
		case 3:
			matched = parts[0] == member[0] && parts[1] == member[1] && parts[2] == member[2]
		case 2:
			matched = (parts[0] == member[0] && parts[1] == member[1]) ||
				(parts[0] == member[1] && parts[1] == member[2])
		case 1:
			matched = parts[0] == member[1]
		}
		if matched {
			matches = append(matches, identity.ID)
		}
	}
	switch len(matches) {
	case 1:
		return matches[0], nil
	case 0:
		return "", &actorbase.TargetResolveError{Code: "not_found", Target: target}
	default:
		return "", &actorbase.TargetResolveError{Code: "actor_ambiguous", Target: target}
	}
}

// memberOfDeclaration answers "which live seat came from this declaration".
// It is a lookup by KEY, not a second addressing rule: nothing here matches
// spellings, so no naming choice can make it right or wrong. The roster is the
// same live list addressing reads, so this adds no new source of actor truth.
//
// It is deliberately single-valued. A declaration whose seats are not capped at
// one has no "the seat"; that caller must name the member it means, and the
// ambiguous verdict says so rather than picking one.
func memberOfDeclaration(declID string, active []storespec.ActiveIdentity) (actor.ActorID, error) {
	matches := make([]actor.ActorID, 0, 1)
	for _, identity := range active {
		if identity.SourceDeclID == declID {
			matches = append(matches, identity.ID)
		}
	}
	switch len(matches) {
	case 1:
		return matches[0], nil
	case 0:
		return "", &actorbase.TargetResolveError{Code: "not_found", Target: declID}
	default:
		return "", &actorbase.TargetResolveError{Code: "actor_ambiguous", Target: declID}
	}
}

func (a *actorSystem) MemberOfDeclaration(declID string) (actor.ActorID, error) {
	active, err := a.home.controller.ActiveIdentities()
	if err != nil {
		return "", err
	}
	return memberOfDeclaration(declID, active)
}

func (a *actorSystem) ResolveTarget(target string) (actor.ActorID, error) {
	active, err := a.home.controller.ActiveIdentities()
	if err != nil {
		return "", err
	}
	return resolveActorTarget(target, active)
}
