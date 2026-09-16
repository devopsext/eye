package common

type Anomaly interface {
	ID() string
	Begin() Stamp
	End() Stamp
	// Cases() []Case
}
