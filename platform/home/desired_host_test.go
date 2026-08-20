package home

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// boundDevices answers with a fixed attachment set for whatever channel it is
// asked about.
type boundDevices []string

func (boundDevices) IsBound(context.Context, channel.ID, string) (bool, error) { return true, nil }
func (b boundDevices) ListBoundDeviceIDs(context.Context, channel.ID) ([]string, error) {
	return []string(b), nil
}
func (boundDevices) ChannelDesired(context.Context, channel.ID) (channelspec.ChannelDesiredFacts, bool, error) {
	return channelspec.ChannelDesiredFacts{}, false, nil
}

func placementHome(devices ...string) *Home {
	return &Home{channelID: "c0.work", registryBindings: boundDevices(devices)}
}

func operationCode(t *testing.T, err error) channelspec.OperationErrorCode {
	t.Helper()
	var opErr *channelspec.OperationError
	if !errors.As(err, &opErr) {
		t.Fatalf("want an operation error, got %v", err)
	}
	return opErr.Code
}

// The point of the parameter: with several devices attached, which one runs the
// member is a choice someone has to make. Before it existed the channel took
// the first bound device — and binding order carries no intent, so the answer
// was arbitrary while looking decided.
func TestASeatRunsOnTheDeviceItWasGiven(t *testing.T) {
	h := placementHome("laptop", "phone", "sandbox")
	placement, err := h.resolveDaemonPlacement(context.Background(), "phone")
	if err != nil {
		t.Fatalf("resolveDaemonPlacement: %v", err)
	}
	if placement.Host != "phone" {
		t.Fatalf("host = %q, want phone", placement.Host)
	}
}

// Naming a device this channel cannot reach must be refused rather than
// honoured: the member would be declared, placed, and permanently absent.
func TestADeviceThisChannelCannotReachIsRefused(t *testing.T) {
	h := placementHome("laptop")
	_, err := h.resolveDaemonPlacement(context.Background(), "someone-elses-phone")
	if code := operationCode(t, err); code != channelspec.ErrCodeInvalidDesiredHost {
		t.Fatalf("code = %q, want %q", code, channelspec.ErrCodeInvalidDesiredHost)
	}
}

// No preference keeps the old behaviour, so every existing caller is unchanged.
func TestNoPreferenceStillLandsSomewhere(t *testing.T) {
	h := placementHome("laptop", "phone")
	placement, err := h.resolveDaemonPlacement(context.Background(), "")
	if err != nil {
		t.Fatalf("resolveDaemonPlacement: %v", err)
	}
	if placement.Host != "laptop" {
		t.Fatalf("host = %q, want the first attached device", placement.Host)
	}
}

// A channel with nothing attached cannot run a daemon-placed class at all, and
// says so — with or without a preference.
func TestAChannelWithNoDeviceRefusesEitherWay(t *testing.T) {
	h := placementHome()
	for _, host := range []string{"", "phone"} {
		if _, err := h.resolveDaemonPlacement(context.Background(), host); operationCode(t, err) != channelspec.ErrCodeInvalidDesiredHost {
			t.Fatalf("desired_host %q: want invalid_desired_host", host)
		}
	}
}
