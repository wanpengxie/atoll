package app_test

import (
	"net/http"
	"testing"
)

func TestComputePlanHTTPEndpointRetired(t *testing.T) {
	env := setupTestApp(t)
	w := env.do(t, "GET", "/compute/plan", nil, nil)
	assertStatus(t, w, http.StatusNotFound)
}
