//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/pkg/coagentsdk"
	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

func waitSDKActor(
	t *testing.T,
	client *coagentsdk.Client,
	channelID string,
	actorID string,
	timeout time.Duration,
	predicate func(coagentsdk.ActorInfo) bool,
) coagentsdk.ActorInfo {
	t.Helper()
	return harness.EventuallyValue(t, "SDK actor "+actorID, timeout, func() (coagentsdk.ActorInfo, bool) {
		actors, err := client.ListActors(context.Background(), channelID)
		if err != nil {
			return coagentsdk.ActorInfo{}, false
		}
		for _, a := range actors {
			if a.ActorID == actorID && predicate(a) {
				return a, true
			}
		}
		return coagentsdk.ActorInfo{}, false
	})
}
