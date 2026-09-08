package home

import (
	"fmt"
	"strings"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
)

// Within the channel-member protocol, the seed is the existing body Channel ID.
// Implementation class and business configuration do not define the relationship.
func (a *actorSystem) checkChannelMember(body string) error {
	if body == "" || strings.Contains(body, ":") || body == string(a.home.channelID) {
		return &channelspec.OperationError{Code: channelspec.ErrCodeBadPayload, Detail: "channel member requires a distinct body Channel ID"}
	}
	identities, err := a.home.controller.ActiveIdentities()
	if err != nil {
		return err
	}
	for _, member := range identities {
		parts := strings.Split(string(member.ID), ":")
		if member.Kind == actor.KindChannel && len(parts) == 3 && parts[1] == body {
			return &channelspec.OperationError{Code: channelspec.ErrCodeConflictExists, Detail: fmt.Sprintf("channel %s is already a member: %s", body, member.ID)}
		}
	}
	return nil
}
