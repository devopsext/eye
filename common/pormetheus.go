package common

import (
	"fmt"
	"strconv"
	"strings"
)

type PrometheusResponseDataVector struct {
	Labels map[string]string `json:"metric"`
	Values []any             `json:"values"`
}

type PrometheusResponseData struct {
	ResultType string                          `json:"resultType"`
	Result     []*PrometheusResponseDataVector `json:"result"`
}

type PrometheusResponse struct {
	Status string                  `json:"status"`
	Data   *PrometheusResponseData `json:"data"`
}

// PrometheusResponseDataVector

func (dv *PrometheusResponseDataVector) Stamp() int64 {

	if len(dv.Values) >= 0 {

		v := dv.Values[0]
		s := fmt.Sprintf("%v", v)
		s = strings.ReplaceAll(s, ".", "")
		if len(s) == 13 { // unix millisec
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return 0
			}
			return i
		} else if len(s) == 10 { // unix sec
			s = fmt.Sprintf("%s000", s)
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return 0
			}
			return i
		}
	}
	return 0
}

func (dv *PrometheusResponseDataVector) Value() float64 {

	if len(dv.Values) >= 1 {

		v := dv.Values[1]
		s := fmt.Sprintf("%v", v)
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0
		}
		return f
	}
	return 0
}
