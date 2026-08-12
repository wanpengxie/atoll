package resourcespec

// CreateSpec selects the byte locus. File bytes never enter the resources
// table; WithContent means the caller will redeem the returned write route.
type CreateSpec struct {
	Kind        ResourceKind
	WithContent bool
}
