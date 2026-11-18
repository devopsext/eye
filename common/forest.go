package common

import (
	"strconv"
	"time"

	"github.com/devopsext/utils"
)

type ForestShiftWeight struct {
	Shift  time.Duration
	Weight float64
}

// Quartile Profiler Conditioned on 168 Hourly Buckets (7 Days x 24 Hours)
type ForestProfilerKind = int

const (
	ForestProfilerKindDay = iota
	ForestProfilerKindHour
	ForestProfilerKind30Minutes
	ForestProfilerKind15Minutes
	ForestProfilerKind10Minutes
	ForestProfilerKind5Minutes
	ForestProfilerKindMinute
)

var profileKinds map[ForestProfilerKind]string

func ProfileKindToString(kind ForestProfilerKind) string {

	pk, ok := profileKinds[kind]
	if !ok {
		return ""
	}
	return pk
}

func StringToProfileKind(kind string) ForestProfilerKind {

	for pk, v := range profileKinds {
		if v == kind {
			return pk
		}
	}
	return ForestProfilerKindDay
}

func ParseShiftWeights(weights string) []ForestShiftWeight {

	r := []ForestShiftWeight{}
	m := utils.MapGetKeyValues(weights)

	for k, v := range m {

		d, err := ParseDuration(k)
		if err != nil {
			continue
		}
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			continue
		}

		r = append(r, ForestShiftWeight{
			Shift:  d,
			Weight: f,
		})
	}
	return r
}

func init() {

	profileKinds = make(map[CaseImpact]string)
	profileKinds[ForestProfilerKindDay] = "day"
	profileKinds[ForestProfilerKindHour] = "hour"
	profileKinds[ForestProfilerKind30Minutes] = "30mins"
	profileKinds[ForestProfilerKind15Minutes] = "15mins"
	profileKinds[ForestProfilerKind10Minutes] = "10mins"
	profileKinds[ForestProfilerKind5Minutes] = "5mins"
	profileKinds[ForestProfilerKindMinute] = "minute"
}
