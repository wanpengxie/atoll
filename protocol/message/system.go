package message

// SystemLocus identifies which membrane owns a reserved system request.
type SystemLocus string

const (
	SystemLocusC0       SystemLocus = "c0"
	SystemLocusMembrane SystemLocus = "membrane"
)

// SystemEntry is one member of the closed reserved message-type table.
type SystemEntry struct {
	Name  string
	Kind  Kind
	Locus SystemLocus
}

const (
	TypeSystemChannelCreate         = "system.channel.create"
	TypeSystemChannelGet            = "system.channel.get"
	TypeSystemChannelList           = "system.channel.list"
	TypeSystemChannelSet            = "system.channel.set"
	TypeSystemChannelDelete         = "system.channel.delete"
	TypeSystemChannelTemplateCreate = "system.channel.template.create"
	TypeSystemChannelTemplateGet    = "system.channel.template.get"
	TypeSystemChannelTemplateList   = "system.channel.template.list"
	TypeSystemChannelTemplateSet    = "system.channel.template.set"
	TypeSystemChannelTemplateDelete = "system.channel.template.delete"
	TypeSystemActorTemplateCreate   = "system.actor.template.create"
	TypeSystemActorTemplateGet      = "system.actor.template.get"
	TypeSystemActorTemplateList     = "system.actor.template.list"
	TypeSystemActorTemplateSet      = "system.actor.template.set"
	TypeSystemActorTemplateDelete   = "system.actor.template.delete"
	TypeSystemActorOverlaySet       = "system.actor.overlay.set"
	TypeSystemActorOverlayDelete    = "system.actor.overlay.delete"
	TypeSystemPrincipalCreate       = "system.principal.create"
	TypeSystemPrincipalLogin        = "system.principal.login"
	TypeSystemPrincipalDelete       = "system.principal.delete"
	TypeSystemPrincipalGet          = "system.principal.get"
	TypeSystemPrincipalList         = "system.principal.list"
	TypeSystemCredentialSet         = "system.credential.set"
	TypeSystemDeviceCreate          = "system.device.create"
	TypeSystemDeviceAttach          = "system.device.attach"
	TypeSystemDeviceDetach          = "system.device.detach"
	TypeSystemDeviceList            = "system.device.list"
	TypeSystemDeviceDelete          = "system.device.delete"
	TypeSystemClassList             = "system.class.list"
	TypeSystemMemberCreate          = "system.member.create"
	TypeSystemMemberAdmit           = "system.member.admit"
	TypeSystemMemberList            = "system.member.list"
	TypeSystemMemberGet             = "system.member.get"
	TypeSystemMemberDelete          = "system.member.delete"
	TypeSystemMemberRestart         = "system.member.restart"
	TypeSystemLogRecent             = "system.log.recent"
	TypeSystemTimerSet              = "system.timer.set"
	TypeSystemTimerCancel           = "system.timer.cancel"
	TypeSystemTimerReset            = "system.timer.reset"
	TypeSystemTimerList             = "system.timer.list"
	TypeSystemMemberCreated         = "system.member.created"
	TypeSystemMemberDeleted         = "system.member.deleted"
	TypeSystemChannelInbound        = "system.channel.inbound"
)

var systemEntries = [...]SystemEntry{
	{Name: TypeSystemChannelCreate, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemChannelGet, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemChannelList, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemChannelSet, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemChannelDelete, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemChannelTemplateCreate, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemChannelTemplateGet, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemChannelTemplateList, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemChannelTemplateSet, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemChannelTemplateDelete, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemActorTemplateCreate, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemActorTemplateGet, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemActorTemplateList, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemActorTemplateSet, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemActorTemplateDelete, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemActorOverlaySet, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemActorOverlayDelete, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemPrincipalCreate, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemPrincipalLogin, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemPrincipalDelete, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemPrincipalGet, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemPrincipalList, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemCredentialSet, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemDeviceCreate, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemDeviceAttach, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemDeviceDetach, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemDeviceList, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemDeviceDelete, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemClassList, Kind: KindRequest, Locus: SystemLocusC0},
	{Name: TypeSystemMemberCreate, Kind: KindRequest, Locus: SystemLocusMembrane},
	{Name: TypeSystemMemberAdmit, Kind: KindRequest, Locus: SystemLocusMembrane},
	{Name: TypeSystemMemberList, Kind: KindRequest, Locus: SystemLocusMembrane},
	{Name: TypeSystemMemberGet, Kind: KindRequest, Locus: SystemLocusMembrane},
	{Name: TypeSystemMemberDelete, Kind: KindRequest, Locus: SystemLocusMembrane},
	{Name: TypeSystemMemberRestart, Kind: KindRequest, Locus: SystemLocusMembrane},
	{Name: TypeSystemLogRecent, Kind: KindRequest, Locus: SystemLocusMembrane},
	{Name: TypeSystemTimerSet, Kind: KindRequest, Locus: SystemLocusMembrane},
	{Name: TypeSystemTimerCancel, Kind: KindRequest, Locus: SystemLocusMembrane},
	{Name: TypeSystemTimerReset, Kind: KindRequest, Locus: SystemLocusMembrane},
	{Name: TypeSystemTimerList, Kind: KindRequest, Locus: SystemLocusMembrane},
	{Name: TypeSystemMemberCreated, Kind: KindEvent, Locus: SystemLocusMembrane},
	{Name: TypeSystemMemberDeleted, Kind: KindEvent, Locus: SystemLocusMembrane},
	{Name: TypeSystemChannelInbound, Kind: KindEvent, Locus: SystemLocusMembrane},
}

// Parse resolves a reserved message type against the closed system table.
func Parse(name string) (SystemEntry, bool) {
	for _, entry := range systemEntries {
		if entry.Name == name {
			return entry, true
		}
	}
	return SystemEntry{}, false
}

// IsMembraneWord reports whether name is a membrane-local system request.
func IsMembraneWord(name string) bool {
	entry, ok := Parse(name)
	return ok && entry.Kind == KindRequest && entry.Locus == SystemLocusMembrane
}

// IsSpaceWord reports whether name is a c0-owned system request.
func IsSpaceWord(name string) bool {
	entry, ok := Parse(name)
	return ok && entry.Kind == KindRequest && entry.Locus == SystemLocusC0
}

// SystemEntries returns the closed table as a copy for consumers that project
// the protocol vocabulary (for example the membrane manifest).
func SystemEntries() []SystemEntry {
	return append([]SystemEntry(nil), systemEntries[:]...)
}
