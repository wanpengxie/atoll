package channel

// Ref is the federation-shaped channel reference: (org_id?, id). Demo
// deployments leave OrgID empty (single-org); M2+ federation populates
// it to express cross-org channel mirrors.
//
// Carved out as a kernel/ type to satisfy m1.5-tickets §T10 ("M1.5 完成
// 时确保后续 federation / SaaS 平滑加, 不需要重写"). The protocol layer
// can refer to a channel via Ref without forcing single-org semantics
// onto every downstream caller.
type Ref struct {
	OrgID string // empty in single-org deployments (M1.5 demo)
	ID    ID
}

// Local returns true when the ref has no OrgID set (single-org / demo).
func (r Ref) Local() bool { return r.OrgID == "" }
