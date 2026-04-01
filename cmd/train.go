package cmd

import (
	"sync"

	"github.com/devopsext/eye/common"
	"github.com/devopsext/eye/model"
	toolsVendors "github.com/devopsext/tools/vendors"
	"github.com/spf13/cobra"
)

var v1ModelOptions = model.V1ModelOptions{
	File: envGet("V1_MODEL_FILE", "").(string),

	AppCommonLabels: envStringExpand("V1_MODEL_APP_COMMON_LABELS", ""),

	AppInTrafficQuery: envStringExpand("V1_MODEL_APP_IN_TRAFFIC_QUERY", ""),
	AppInErrorsQuery:  envStringExpand("V1_MODEL_APP_IN_ERRORS_QUERY", ""),
	AppInLatencyQuery: envStringExpand("V1_MODEL_APP_IN_LATENCY_QUERY", ""),

	AppOutTrafficQuery: envStringExpand("V1_MODEL_APP_OUT_TRAFFIC_QUERY", ""),
	AppOutErrorsQuery:  envStringExpand("V1_MODEL_APP_OUT_ERRORS_QUERY", ""),
	AppOutLatencyQuery: envStringExpand("V1_MODEL_APP_OUT_LATENCY_QUERY", ""),

	Prometheus: toolsVendors.PrometheusOptions{
		URL:      envStringExpand("V1_MODEL_PROMETHEUS_URL", ""),
		User:     envStringExpand("V1_MODEL_PROMETHEUS_USER", ""),
		Password: envStringExpand("V1_MODEL_PROMETHEUS_PASSWORD", ""),
		Timeout:  envGet("V1_MODEL_PROMETHEUS_TIMEOUT", 30).(int),
		Insecure: envGet("V1_MODEL_PROMETHEUS_INSECURE", false).(bool),
		From:     envGet("V1_MODEL_PROMETHEUS_FROM", "").(string),
		To:       envGet("V1_MODEL_PROMETHEUS_TO", "").(string),
		Step:     envGet("V1_MODEL_PROMETHEUS_STEP", "60s").(string),
		Params:   envGet("V1_MODEL_PROMETHEUS_PARAMS", "").(string),
	},
}

func NewTrainCommand(wg *sync.WaitGroup) *cobra.Command {

	trainCmd := cobra.Command{
		Use:   "train",
		Short: "Train model",
	}

	trainV1ModelCmd := cobra.Command{
		Use:   "v1",
		Short: "Train V1 model",
		Run: func(cmd *cobra.Command, args []string) {

			obs := common.NewObservability(logs, metrics)
			logger := obs.Logs()

			logger.Info("Begin training V1 model...")

			v1 := model.NewV1Model(v1ModelOptions, obs)
			v1.Train(wg)

			wg.Wait()

			logger.Info("End training V1 model")
		},
	}

	flags := trainV1ModelCmd.PersistentFlags()
	flags.StringVar(&v1ModelOptions.File, "v1-model-file", v1ModelOptions.File, "V1 model file path")

	flags.StringVar(&v1ModelOptions.AppCommonLabels, "v1-model-app-common-labels", v1ModelOptions.AppCommonLabels, "V1 model prometheus app common labels")

	flags.StringVar(&v1ModelOptions.AppInTrafficQuery, "v1-model-app-in-traffic-query", v1ModelOptions.AppInTrafficQuery, "V1 model prometheus app incoming traffic query")
	flags.StringVar(&v1ModelOptions.AppInErrorsQuery, "v1-model-app-in-errors-query", v1ModelOptions.AppInErrorsQuery, "V1 model prometheus app incoming errors query")
	flags.StringVar(&v1ModelOptions.AppInLatencyQuery, "v1-model-app-in-latency-query", v1ModelOptions.AppInLatencyQuery, "V1 model prometheus app incoming latency query")

	flags.StringVar(&v1ModelOptions.AppOutTrafficQuery, "v1-model-app-out-traffic-query", v1ModelOptions.AppOutTrafficQuery, "V1 model prometheus app outgoing traffic query")
	flags.StringVar(&v1ModelOptions.AppOutErrorsQuery, "v1-model-app-out-errors-query", v1ModelOptions.AppOutErrorsQuery, "V1 model prometheus app outgoing errors query")
	flags.StringVar(&v1ModelOptions.AppOutLatencyQuery, "v1-model-app-out-latency-query", v1ModelOptions.AppOutLatencyQuery, "V1 model prometheus app outgoing latency query")

	flags.StringVar(&v1ModelOptions.Prometheus.URL, "v1-model-prometheus-url", v1ModelOptions.Prometheus.URL, "V1 model prometheus url")
	flags.StringVar(&v1ModelOptions.Prometheus.User, "v1-model-prometheus-user", v1ModelOptions.Prometheus.User, "V1 model prometheus user")
	flags.StringVar(&v1ModelOptions.Prometheus.Password, "v1-model-prometheus-password", v1ModelOptions.Prometheus.Password, "V1 model prometheus password")
	flags.IntVar(&v1ModelOptions.Prometheus.Timeout, "v1-model-prometheus-timeout", v1ModelOptions.Prometheus.Timeout, "V1 model prometheus timeout")
	flags.BoolVar(&v1ModelOptions.Prometheus.Insecure, "v1-model-prometheus-insecure", v1ModelOptions.Prometheus.Insecure, "V1 model prometheus insecure")
	flags.StringVar(&v1ModelOptions.Prometheus.From, "v1-model-prometheus-from", v1ModelOptions.Prometheus.From, "V1 model prometheus from")
	flags.StringVar(&v1ModelOptions.Prometheus.To, "v1-model-prometheus-to", v1ModelOptions.Prometheus.To, "V1 model prometheus to")
	flags.StringVar(&v1ModelOptions.Prometheus.Step, "v1-model-prometheus-step", v1ModelOptions.Prometheus.Step, "V1 model prometheus step")
	flags.StringVar(&v1ModelOptions.Prometheus.Params, "v1-model-prometheus-params", v1ModelOptions.Prometheus.Params, "V1 model prometheus params")
	trainCmd.AddCommand(&trainV1ModelCmd)

	return &trainCmd
}
