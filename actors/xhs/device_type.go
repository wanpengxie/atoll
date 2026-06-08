package xhs

// XHS device_type closed set. The mobile/desktop entries are reserved for
// v1-compatible future clients; production validation still recognizes them
// so the wire contract stays closed and explicit.
const (
	DeviceTypeChromeExtension  = "xhs.chrome_extension"
	DeviceTypeMobileWebview    = "xhs.mobile_webview"
	DeviceTypeDesktopAssistant = "xhs.desktop_assistant"
)

// IsDeviceType reports whether deviceType is in the xhs domain closed set.
func IsDeviceType(deviceType string) bool {
	switch deviceType {
	case DeviceTypeChromeExtension,
		DeviceTypeMobileWebview,
		DeviceTypeDesktopAssistant:
		return true
	default:
		return false
	}
}
