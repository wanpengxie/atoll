package devicetransit

// XHS device_type closed set. The mobile/desktop entries are reserved for
// v1-compatible future clients; production validation still recognizes them
// so the wire contract stays closed and explicit.
const (
	DeviceTypeXHSChromeExtension  = "xhs.chrome_extension"
	DeviceTypeXHSMobileWebview    = "xhs.mobile_webview"
	DeviceTypeXHSDesktopAssistant = "xhs.desktop_assistant"
)

// IsXHSDeviceType reports whether deviceType is in the xhs domain closed set.
func IsXHSDeviceType(deviceType string) bool {
	switch deviceType {
	case DeviceTypeXHSChromeExtension,
		DeviceTypeXHSMobileWebview,
		DeviceTypeXHSDesktopAssistant:
		return true
	default:
		return false
	}
}
