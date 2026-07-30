package app_test

import (
	"log/slog"
	"testing"

	"github.com/wanpengxie/atoll/platform/compute"
)

func daemonComputeConfig(
	t *testing.T,
	rawURL string,
	credential string,
	factories compute.ActorFactorySource,
	logger *slog.Logger,
) compute.Config {
	t.Helper()
	return compute.Config{
		ServerWS: rawURL, Credential: credential, AtollHome: t.TempDir(),
		Logger: logger,
		BuildCompartment: func(string, string) (compute.CompartmentResources, error) {
			return compute.CompartmentResources{Factories: factories}, nil
		},
	}
}
