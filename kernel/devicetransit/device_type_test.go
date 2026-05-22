package devicetransit

import "testing"

func TestIsXHSDeviceTypeClosedSet(t *testing.T) {
	for _, typ := range []string{
		DeviceTypeXHSChromeExtension,
		DeviceTypeXHSMobileWebview,
		DeviceTypeXHSDesktopAssistant,
	} {
		if !IsXHSDeviceType(typ) {
			t.Fatalf("%s should be accepted", typ)
		}
	}
	for _, typ := range []string{"", "xhs", "feishu_mobile"} {
		if IsXHSDeviceType(typ) {
			t.Fatalf("%s should be rejected", typ)
		}
	}
}
