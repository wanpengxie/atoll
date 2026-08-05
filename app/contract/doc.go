// The package comment lives on contract.go (one authoritative header per
// package — go doc must not print two "Package contract" paragraphs).
//
// Registry discipline recorded here so it sits next to the registry files:
// every /api endpoint and /ws must have exactly one registry entry. Stable
// methods live outside /api/experimental; unstable methods live under that
// prefix and carry Experimental=true. There is no third, unlabelled grey area:
// a route that cannot satisfy one of those two forms does not belong on the
// shell contract surface.
package contract
