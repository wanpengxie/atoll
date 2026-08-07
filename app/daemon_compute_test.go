package app_test

// The /compute door: daemon-side compute config used by the live tests, and
// the credential rules of the endpoint itself — credentials travel in the
// Authorization header only (a ?key= query credential would leak into logs),
// and malformed/invalid bearers are refused loudly.

import (
	"log/slog"
	"net/http"
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

func TestComputeRejectsQueryCredentialAndMalformedBearer(t *testing.T) {
	env := setupTestApp(t)
	query := env.do(t, http.MethodGet, "/compute?key=must-not-enter-logs", nil, nil)
	assertStatus(t, query, http.StatusUnauthorized)

	missing := env.do(t, http.MethodGet, "/compute", nil, nil)
	assertStatus(t, missing, http.StatusBadRequest)
	malformed := env.doHeaders(t, http.MethodGet, "/compute", nil, nil, map[string]string{
		"Authorization": "Basic credentials",
	})
	assertStatus(t, malformed, http.StatusBadRequest)
	invalid := env.doHeaders(t, http.MethodGet, "/compute", nil, nil, map[string]string{
		"Authorization": "Bearer invalid",
	})
	assertStatus(t, invalid, http.StatusUnauthorized)
}
