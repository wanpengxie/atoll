package archtest

import (
	"fmt"
	"strings"
	"testing"
)

// gateway-build-spec.md §1 S0 / 表⑤ 法度墙: the drivers/ umbrella package
// (drivers/tools = the ex-actors adapters, drivers/gateway = the human ingress
// component, drivers/agents = the LLM engine adapters) is a leaf subsystem. Two invariants, defined
// here BEFORE the gateway body lands (裁决 2 "围栏定义先行合入"):
//
//   A. drivers/* import confinement — a driver may name only the substrate +
//      stdlib faces it adapts over: lib/*, protocol/*, runtime/* (exported),
//      platform (export face), registry. drivers → app is FORBIDDEN outright
//      (routing policy / membership events reach the gateway through injected
//      seams the assembly root wires, never by importing app — design §5.3).
//
//   B. drivers/* consumer confinement — nobody imports a driver EXCEPT the
//      assembly root cmd/* (裁决 2: the ONLY legitimate consumer, it拉起 the
//      fat-daemon registry + wires the gateway). registry itself must never
//      import drivers (would close the drivers→registry→drivers cycle) — that
//      falls out of B for free (registry is not under cmd/). Two named app
//      integration-test files are an explicit, justified exception (see
//      driversTestImporterAllowlist).
//
// Both walk INCLUDING _test.go (walkImportsAll): a cross-domain test fixture
// reaching across the fence is exactly the leak S0 is closing (the four ex-
// actors test consumers). The two that cannot be de-fenced (they drive the app
// HTTP stack against the real xhs/kimi adapter, and drivers→app forbids moving
// them into the drivers domain) are named exceptions, not a blanket _test.go
// relaxation.

const driversPrefix = platformModulePrefix + "drivers/"

// driversAllowedImportPrefixes is fence A's allowlist (module-relative, i.e.
// the text after platformModulePrefix). "platform" with no trailing slash also
// admits the platform root package itself.
var driversAllowedImportPrefixes = []string{
	"drivers/",   // intra-umbrella
	"lib/",       // stdlib faces (actorbase, introspect, …)
	"protocol/",  // the wire ontology
	"runtime/",   // exported runtime faces (actorrt, schedule)
	"platform",   // the substrate export face (platform, platform/…)
	"registry",   // driver self-registration target (drivers→registry, 裁决 2)
}

// TestDriversImportConfinement — fence A.
func TestDriversImportConfinement(t *testing.T) {
	var v []string
	walkImportsAll(t, func(slash, imp string) {
		if !strings.HasPrefix(slash, "../drivers/") {
			return
		}
		if !strings.HasPrefix(imp, platformModulePrefix) {
			return // stdlib / third-party (gorilla) — not this fence's concern
		}
		sub := strings.TrimPrefix(imp, platformModulePrefix)
		for _, p := range driversAllowedImportPrefixes {
			if strings.HasPrefix(sub, p) {
				return
			}
		}
		v = append(v, fmt.Sprintf(
			"%s imports %q — drivers/* may name only lib/protocol/runtime + platform export face + registry; drivers→app is forbidden (routing/membership reach the gateway through injected seams the assembly root wires)",
			slash, imp))
	})
	failViolations(t, "drivers/* import confinement (lib/protocol/runtime + platform + registry only)", v)
}

// driversTestImporterAllowlist: the two app-integration tests that legitimately
// wire the concrete xhs/kimi adapter against the full app HTTP stack. They
// cannot move into the drivers domain (drivers→app is forbidden by fence A) and
// cannot inline-double the adapter (they assert the adapter's REAL device wire
// behaviour). Named here so the exception is auditable, not a blanket test
// relaxation. (gateway-build-spec S0 处置: 迁域内/黑盒 infeasible for这两 —
// 见 S0 交接申报, owner 待拍.)
var driversTestImporterAllowlist = map[string]bool{
	"../app/xhs_live_test.go":      true,
	"../app/metatool_live_test.go": true,
	// gateway 期 S3: the app test harness (setupTestApp) wires the real human-ingress
	// connector into the app exactly as cmd/server does — the black-box ws frame
	// tests drive the app HTTP stack against the real gateway, and drivers→app forbids
	// moving them into the drivers domain, so this is a named exception (not a blanket
	// relaxation), same posture as the two live tests above.
	"../app/e2e_test.go": true,
}

// TestDriversConsumerConfinement — fence B.
func TestDriversConsumerConfinement(t *testing.T) {
	var v []string
	walkImportsAll(t, func(slash, imp string) {
		if !strings.HasPrefix(imp, driversPrefix) {
			return
		}
		if strings.HasPrefix(slash, "../cmd/") || strings.HasPrefix(slash, "../drivers/") {
			return // assembly root (裁决 2) + intra-umbrella
		}
		if driversTestImporterAllowlist[slash] {
			return
		}
		v = append(v, fmt.Sprintf(
			"%s imports %q — drivers/* has ONE legitimate consumer: the assembly root cmd/* (裁决 2). registry must never import drivers (cycle). Reach a driver through the cmd-wired seam, not a direct import",
			slash, imp))
	})
	failViolations(t, "drivers/* consumer confinement (cmd/* assembly root only)", v)
}
