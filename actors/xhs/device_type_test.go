package xhs

import "testing"

func TestIsDeviceTypeClosedSet(t *testing.T) {
	for _, typ := range []string{
		DeviceTypeChromeExtension,
		DeviceTypeMobileWebview,
		DeviceTypeDesktopAssistant,
	} {
		if !IsDeviceType(typ) {
			t.Fatalf("IsDeviceType(%q)=false", typ)
		}
	}
	for _, typ := range []string{"", "xhs.unknown", "chrome_extension"} {
		if IsDeviceType(typ) {
			t.Fatalf("IsDeviceType(%q)=true", typ)
		}
	}
}
