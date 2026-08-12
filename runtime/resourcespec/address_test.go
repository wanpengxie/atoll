package resourcespec

import "testing"

func TestFileAddressRoundTrip(t *testing.T) {
	for _, raw := range []string{
		"daemon://a/x",
		"daemon://host/docs/report.pdf",
		"daemon://host/a%20b/%E6%96%87%E4%BB%B6",
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

func TestFileAddressRejectsInvalidForms(t *testing.T) {
	for _, raw := range []string{
		"host/x", "other://host/x", "daemon:///x", "daemon://user@host/x",
		"daemon://host:80/x", "daemon://host/x?q=1", "daemon://host/x#f",
		"daemon://host", "daemon://host/", "daemon://host/a//b",
		"daemon://host/a/./b", "daemon://host/a/../b", "daemon://host/a%2Fb",
		"daemon://host/a%2fb", "daemon://host/%78", "daemon://host/a%2",
		"daemon://host/%e6%96%87",
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := ParseFileAddress(raw); err == nil {
				t.Fatalf("ParseFileAddress(%q) succeeded", raw)
			}
		})
	}
	if _, err := FormatFileAddress(FileAddress{Scheme: DaemonScheme, Host: "host", Path: "/x"}); err == nil {
		t.Fatal("constructor accepted a leading slash")
	}
}
