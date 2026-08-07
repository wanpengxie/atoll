// Package hostcommon holds the welding implementation shared by both host
// packages (platform/home and platform/compute): the ActorFactory
// single Proc representation, its Build entry point, and the
// delivery-outcome label helper OutcomeString. Neither host owns this shape —
// it is common to both — so it lives in its own internal package and is
// re-exported by the platform root under aliases (ActorFactory) for the
// membrane word-list; Build itself never crosses out of the platform tree.
package hostcommon
