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
// the text after platformModulePrefix), for the HasPrefix-matched entries.
// The platform export face is NOT a HasPrefix entry here (platform-topology
// 批 裁决7): "platform" as a bare prefix would also admit
// platform/internal/* — Go's internal/ visibility rule already blocks that
// import, but a prefix-matched archtest allowlist saying so is a false
// "second lock". driversAllowedExactPlatform below is the real, precise
// admission list for the platform export face.
var driversAllowedImportPrefixes = []string{
	"drivers/",  // intra-umbrella
	"lib/",      // stdlib faces (actorbase, introspect, …)
	"protocol/", // the wire ontology
	"runtime/",  // exported runtime faces (actorrt, schedule)
	"registry",  // driver self-registration target (drivers→registry, 裁决 2)
}

// driversAllowedExactPlatform is fence A's precise platform-export-face
// admission (platform-topology 批 裁决8): exactly the platform root package,
// the platform/subjectgate export subpackage, and the platform/channelhost
// package — nothing under platform/internal/* is named here, so this
// allowlist plus Go's internal/ rule is a REAL double lock, not a prefix
// that silently also admits platform/internal/*. platform/compute is
// DELIBERATELY absent: drivers has zero compute-host consumers (实测) — a
// driver importing platform/compute is a fence violation, not an
// allowlist gap to backfill.
var driversAllowedExactPlatform = map[string]bool{
	"platform":             true,
	"platform/subjectgate": true,
	"platform/channelhost": true,
}

func driversImportAllowed(sub string) bool {
	if driversAllowedExactPlatform[sub] {
		return true
	}
	for _, p := range driversAllowedImportPrefixes {
		if strings.HasPrefix(sub, p) {
			return true
		}
	}
	return false
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
		if driversImportAllowed(sub) {
			return
		}
		v = append(v, fmt.Sprintf(
			"%s imports %q — drivers/* may name only lib/protocol/runtime + platform export face (platform, platform/subjectgate, platform/channelhost) + registry; drivers→app is forbidden",
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
	// 连接模型勘误期修复批 P1-4: same posture as e2e_test.go above — a real write-order
	// regression test (creator's directory-commit poke ordering) needs the SAME real
	// gateway wiring against the real app HTTP stack; it cannot move into drivers/*
	// (drivers→app is forbidden) nor inline-double the wiring without losing the very
	// thing under test (the real Admit/tx-commit/poke sequence).
	"../app/channel_poke_test.go": true,
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
