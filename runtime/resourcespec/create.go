package resourcespec

// FileNodeType is the physical node shape created or reported by the file
// driver. It does not widen ResourceKind: both regular files and directories
// remain file-kind resources whose truth is the channel filesystem.
type FileNodeType string

const (
	FileNodeRegular   FileNodeType = "regular"
	FileNodeDirectory FileNodeType = "directory"
	FileNodeOther     FileNodeType = "other"
)

// CreateSpec selects the byte locus and, for file-kind resources, the physical
// node shape. File bytes never enter the resources table; WithContent means
// the caller will redeem the returned write route and is valid only for a
// regular node.
type CreateSpec struct {
	Kind        ResourceKind
	WithContent bool
	NodeType    FileNodeType
}
