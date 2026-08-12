package resourcespec

import "testing"

func TestFileAddressRoundTrip(t *testing.T) {
	for _, raw := range []string{
		"daemon://a/c0/x",
		"daemon://host/c0.ops/docs/report.pdf",
		"daemon://host/c0.ops/a%20b/%E6%96%87%E4%BB%B6",
	} {
		t.Run(raw, func(t *testing.T) {
			addr, err := ParseFileAddress(raw)
			if err != nil {
				t.Fatal(err)
			}
			got, err := FormatFileAddress(addr)
			if err != nil || got != raw {
				t.Fatalf("round trip = %q, %v; want %q", got, err, raw)
			}
		})
	}
}

func TestFileAddressSeparatesExactlyOneChannelSegment(t *testing.T) {
	address, err := ParseFileAddress("daemon://local-device/c0.proj-x.backend/docs/report.txt")
	if err != nil {
		t.Fatal(err)
	}
	if address.Host != "local-device" || address.Channel != "c0.proj-x.backend" || address.Path != "docs/report.txt" {
		t.Fatalf("parsed address=%+v", address)
	}
}

func TestFileAddressRejectsInvalidForms(t *testing.T) {
	for _, raw := range []string{
		"host/x", "other://host/c0/x", "daemon:///c0/x", "daemon://user@host/c0/x",
		"daemon://host:80/c0/x", "daemon://host/c0/x?q=1", "daemon://host/c0/x#f",
		"daemon://host", "daemon://host/", "daemon://host/c0", "daemon:////file", "daemon://host/c0/a//b",
		"daemon://host/c0/a/./b", "daemon://host/c0/a/../b", "daemon://host/c0/a%2Fb",
		"daemon://host/c0/a%2fb", "daemon://host/c0/%78", "daemon://host/c0/a%2",
		"daemon://host/c0/%e6%96%87",
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := ParseFileAddress(raw); err == nil {
				t.Fatalf("ParseFileAddress(%q) succeeded", raw)
			}
		})
	}
	if _, err := FormatFileAddress(FileAddress{Scheme: DaemonScheme, Host: "host", Channel: "c0", Path: "/x"}); err == nil {
		t.Fatal("constructor accepted a leading slash")
	}
}
