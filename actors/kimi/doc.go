// Package kimi defines the Kimi WebBridge tool actor — it drives the
// user's real browser via a Chrome extension connected through the
// daemon's local-device transport.
//
// This package contains ONLY protocol definitions and describe metadata.
// The actual browser automation runs in the Chrome extension; the daemon
// is a generic envelope bridge. (The local-device transport is an additive
// daemon-side bridge件, reintroduced alongside the concrete xhs/kimi device
// actors — see platform-redesign §8 拍板项 4.)
package kimi
