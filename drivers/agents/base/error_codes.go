package base

const (
	errorCancelled       = "cancelled"
	errorCASMismatch     = "cas_mismatch"
	errorInterrupted     = "interrupted"
	errorOverloaded      = "overloaded"
	errorProviderTimeout = "provider_timeout"
	errorProviderCrash   = "provider_crash"
	errorProviderFailed  = "provider_failed"
	errorInputTooLarge   = "input_too_large"
	errorEmptyInput      = "empty_input"
)

var runtimeErrorCodes = []string{
	errorCancelled,
	errorCASMismatch,
	errorInterrupted,
	errorOverloaded,
	errorProviderTimeout,
	errorProviderCrash,
	errorProviderFailed,
	errorInputTooLarge,
	errorEmptyInput,
}
