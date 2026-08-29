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
	Scheme  string
	Host    string
	Channel string
	Path    string
}

type FilePrefix struct {
	Host    string
	Channel string
	Path    string
}

// ParseFileAddress accepts only the one canonical spelling of a daemon file
// address: daemon://<device-name>/<channel-name>/<path>. These are canonical,
// human-readable registry names; internal authority resolves them to ids.
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
	segments := strings.SplitN(path, "/", 2)
	if len(segments) != 2 || segments[0] == "" {
		return FileAddress{}, errors.New("resourcespec: invalid daemon channel segment")
	}
	if err := validateLogicalPath(segments[1]); err != nil {
		return FileAddress{}, err
	}
	addr := FileAddress{Scheme: DaemonScheme, Host: u.Host, Channel: segments[0], Path: segments[1]}
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
	if addr.Scheme != DaemonScheme || addr.Host == "" || strings.ContainsAny(addr.Host, "@:") ||
		addr.Channel == "" || strings.Contains(addr.Channel, "/") {
		return "", errors.New("resourcespec: invalid daemon file address")
	}
	if err := validateLogicalPath(addr.Path); err != nil {
		return "", err
	}
	u := &url.URL{Scheme: DaemonScheme, Host: addr.Host, Path: "/" + addr.Channel + "/" + addr.Path}
	return u.String(), nil
}

// ParseFilePrefix parses the file-list spelling. Unlike a file address, the
// file-path remainder may be empty or end in a slash; the channel segment is
// still mandatory and occupies exactly one path segment.
func ParseFilePrefix(raw string) (FilePrefix, error) {
	if raw == "" || strings.Contains(strings.ToLower(raw), "%2f") || !canonicalEscapeCase(raw) {
		return FilePrefix{}, errors.New("resourcespec: invalid daemon file prefix")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != DaemonScheme || u.Opaque != "" || u.Host == "" ||
		u.User != nil || strings.ContainsAny(u.Host, "@:") || u.RawQuery != "" ||
		u.ForceQuery || u.Fragment != "" || !strings.HasPrefix(u.Path, "/") {
		return FilePrefix{}, errors.New("resourcespec: invalid daemon file prefix")
	}
	path := strings.TrimPrefix(u.Path, "/")
	segments := strings.SplitN(path, "/", 2)
	if len(segments) != 2 || segments[0] == "" ||
		validateLogicalPrefix(segments[1]) != nil {
		return FilePrefix{}, errors.New("resourcespec: invalid daemon file prefix")
	}
	prefix := FilePrefix{Host: u.Host, Channel: segments[0], Path: segments[1]}
	canonical := (&url.URL{Scheme: DaemonScheme, Host: prefix.Host, Path: "/" + prefix.Channel + "/" + prefix.Path}).String()
	if canonical != raw {
		return FilePrefix{}, errors.New("resourcespec: non-canonical daemon file prefix")
	}
	return prefix, nil
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

func validateLogicalPrefix(path string) error {
	if strings.HasPrefix(path, "/") {
		return errors.New("resourcespec: invalid daemon file prefix")
	}
	if path == "" {
		return nil
	}
	parts := strings.Split(path, "/")
	for i, segment := range parts {
		if segment == "" && i == len(parts)-1 {
			continue
		}
		if segment == "" || segment == "." || segment == ".." {
			return errors.New("resourcespec: invalid daemon file prefix")
		}
	}
	return nil
}
