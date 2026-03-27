package common

type PrometheusResponseDataVector struct {
	Labels map[string]string `json:"metric"`
	Values [][]any           `json:"values"`
}

type PrometheusResponseData struct {
	ResultType string                          `json:"resultType"`
	Result     []*PrometheusResponseDataVector `json:"result"`
}

type PrometheusResponse struct {
	Status string                  `json:"status"`
	Data   *PrometheusResponseData `json:"data"`
}
