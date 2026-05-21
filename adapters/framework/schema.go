package framework

import (
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ValidateTypeDeclaration runs the install-time validation rules for
// one type's TypeDeclaration view per L2 §1.4.2.
//
// Level A (proto-layer0 §1.4.1 / proto-layer1 §1.3): payload is opaque
// to the protocol layer; the type_registry stores NO payload schema
// fields and install does NOT validate payload schemas. The only
// install-time closed-set checks are allowed_kinds membership +
// terminal_convention enum.
//
// Returns nil on success. Returns an *InstallError wrapping
// message.InstallTypeRegistryInvalid on failure — caller errors.As-es
// it to map to RPC reject reason.
func ValidateTypeDeclaration(typeName string, td adapter.TypeDeclaration) error {
	if len(td.AllowedKinds) == 0 {
		return fmt.Errorf("%w: type=%s allowed_kinds empty",
			asInstallError(message.InstallTypeRegistryInvalid), typeName)
	}
	seenKind := map[message.Kind]bool{}
	for _, k := range td.AllowedKinds {
		switch k {
		case message.KindEvent, message.KindRequest, message.KindResponse:
		default:
			return fmt.Errorf("%w: type=%s allowed_kinds contains invalid kind %q",
				asInstallError(message.InstallTypeRegistryInvalid), typeName, k)
		}
		if seenKind[k] {
			return fmt.Errorf("%w: type=%s allowed_kinds contains duplicate %q",
				asInstallError(message.InstallTypeRegistryInvalid), typeName, k)
		}
		seenKind[k] = true
	}

	if td.TerminalConvention != "" &&
		td.TerminalConvention != string(TerminalPayloadStatus) &&
		td.TerminalConvention != string(TerminalSingleResponse) {
		return fmt.Errorf("%w: type=%s terminal_convention %q invalid",
			asInstallError(message.InstallTypeRegistryInvalid), typeName, td.TerminalConvention)
	}

	return nil
}
