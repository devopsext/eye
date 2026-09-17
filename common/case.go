package common

import (
	"maps"
	"slices"
	"strings"

	"github.com/devopsext/utils"
)

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

var impacts map[CaseImpact]string
var categories map[CaseCategory]string

func CaseImpactToString(impact CaseImpact) string {

	i, ok := impacts[impact]
	if !ok {
		return ""
	}
	return i
}

func StringToCaseImpact(impact string) CaseImpact {

	for k, v := range impacts {
		if v == impact {
			return k
		}
	}
	return CaseImpactUnknown
}

func StringToCaseImpacts(impacts string) []CaseImpact {

	r := []CaseImpact{}
	if utils.IsEmpty(impacts) {
		return r
	}

	arr := strings.Split(impacts, ",")
	for _, v := range arr {

		s := strings.TrimSpace(v)
		if utils.IsEmpty(s) {
			continue
		}

		i := StringToCaseImpact(s)
		if !utils.Contains(r, i) {
			r = append(r, i)
		}
	}
	return r
}

func AllCaseImpacts() []CaseImpact {

	keys := maps.Keys(impacts)
	return slices.Collect(keys)
}

func CaseCategoryToString(category CaseCategory) string {

	c, ok := categories[category]
	if !ok {
		return ""
	}
	return c
}

func StringToCaseCategory(category string) CaseCategory {

	for k, v := range categories {
		if v == category {
			return k
		}
	}
	return CaseCategoryUnknown
}

func StringToCaseCategories(categories string) []CaseCategory {

	r := []CaseCategory{}
	if utils.IsEmpty(categories) {
		return r
	}

	arr := strings.Split(categories, ",")
	for _, v := range arr {

		s := strings.TrimSpace(v)
		if utils.IsEmpty(s) {
			continue
		}

		c := StringToCaseCategory(s)
		if !utils.Contains(r, c) {
			r = append(r, c)
		}
	}
	return r
}

func AllCaseCategories() []CaseCategory {

	keys := maps.Keys(categories)
	return slices.Collect(keys)
}

func init() {

	impacts = make(map[CaseImpact]string)
	impacts[CaseImpactUnknown] = "unknown"
	impacts[CaseImpactZero] = "zero"
	impacts[CaseImpactAverage] = "average"
	impacts[CaseImpactSevere] = "severe"

	categories = make(map[CaseCategory]string)
	categories[CaseCategoryUnknown] = "unknown"
	categories[CaseCategoryHealthy] = "healthy"
	categories[CaseCategoryOperational] = "operational"
	categories[CaseCategoryFailure] = "failure"
}
