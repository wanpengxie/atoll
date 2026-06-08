// Package kimi defines the Kimi WebBridge tool actor — it drives the
// user's real browser via a Chrome extension connected through the
// daemon's localdevice transport.
//
// This package contains ONLY protocol definitions and describe metadata.
// The actual browser automation runs in the Chrome extension; the daemon
// is a generic envelope bridge (platform/localdevice).
package kimi
