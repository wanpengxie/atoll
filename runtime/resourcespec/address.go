package resourcespec

import (
	"errors"
	"net/url"
	"strings"
)

const DaemonScheme = "daemon"

// FileAddress is the canonical daemon-backed file name. Path is logical and
// never carries the URI's separating leading slash.
type FileAddress struct {
	Scheme string
	Host   string
	Path   string
}

// ParseFileAddress accepts only the one canonical spelling of a daemon file
// address. It deliberately validates URI shape, not the device-name alphabet;
// device names can only enter the registry through lagoon's validator.
func ParseFileAddress(raw string) (FileAddress, error) {
	if raw == "" || strings.Contains(strings.ToLower(raw), "%2f") || !canonicalEscapeCase(raw) {
		return FileAddress{}, errors.New("resourcespec: invalid daemon file address")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != DaemonScheme || u.Opaque != "" || u.Host == "" ||
		u.User != nil || strings.ContainsAny(u.Host, "@:") || u.RawQuery != "" ||
		u.ForceQuery || u.Fragment != "" || !strings.HasPrefix(u.Path, "/") {
		return FileAddress{}, errors.New("resourcespec: invalid daemon file address")
	}
	path := strings.TrimPrefix(u.Path, "/")
	if err := validateLogicalPath(path); err != nil {
		return FileAddress{}, err
	}
	addr := FileAddress{Scheme: DaemonScheme, Host: u.Host, Path: path}
	canonical, err := FormatFileAddress(addr)
	if err != nil || canonical != raw {
		return FileAddress{}, errors.New("resourcespec: non-canonical daemon file address")
	}
	return addr, nil
}

func canonicalEscapeCase(raw string) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '%' {
			continue
		}
		if i+2 >= len(raw) || !upperHex(raw[i+1]) || !upperHex(raw[i+2]) {
			return false
		}
		i += 2
	}
	return true
}

func upperHex(b byte) bool { return b >= '0' && b <= '9' || b >= 'A' && b <= 'F' }

// FormatFileAddress constructs the sole canonical daemon address spelling.
func FormatFileAddress(addr FileAddress) (string, error) {
	if addr.Scheme != DaemonScheme || addr.Host == "" || strings.ContainsAny(addr.Host, "@:") {
		return "", errors.New("resourcespec: invalid daemon file address")
	}
	if err := validateLogicalPath(addr.Path); err != nil {
		return "", err
	}
	u := &url.URL{Scheme: DaemonScheme, Host: addr.Host, Path: "/" + addr.Path}
	return u.String(), nil
}

func validateLogicalPath(path string) error {
	if path == "" || strings.HasPrefix(path, "/") {
		return errors.New("resourcespec: invalid daemon file path")
	}
	for _, segment := range strings.Split(path, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return errors.New("resourcespec: invalid daemon file path")
		}
	}
	return nil
}
