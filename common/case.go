package common

type CaseLevel = int

const (
	CaseLevelUnknown = iota
	CaseLevelNormal
	CaseLevelHigh
	CaseLevelLow
)

type CaseImpact = int

const (
	CaseImpactUnknown = iota
	CaseImpactZero
	CaseImpactAverage
	CaseImpactSevere
)

type CaseCategory = int

const (
	CaseCategoryUnknown = iota
	CaseCategoryHealthy
	CaseCategoryOperational
	CaseCategoryFailure
)

type Case interface {
	//
}
