package workbuddy

import "regexp"

var nativeSecretPattern = regexp.MustCompile(`(?i)(bearer\s+)[^\s,;]+|((?:authorization|api[_-]?key|access[_-]?token|refresh[_-]?token|password|credential)\s*[:=]\s*)[^\s,;]+`)

func redactNative(s string) string { return nativeSecretPattern.ReplaceAllString(s, "$1$2[redacted]") }
