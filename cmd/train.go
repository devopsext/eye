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

	AppQuery:     envFileContentExpand("V1_MODEL_APP_QUERY", ""),
	AppTolerance: envGet("V1_MODEL_APP_TOLERANCE", 0).(int),

	AppSignalCommonLabels:    envStringExpand("V1_MODEL_APP_SIGNAL_COMMON_LABELS", ""),
	AppSignalInTrafficQuery:  envFileContentExpand("V1_MODEL_APP_SIGNAL_IN_TRAFFIC_QUERY", ""),
	AppSignalInErrorsQuery:   envFileContentExpand("V1_MODEL_APP_SIGNAL_IN_ERRORS_QUERY", ""),
	AppSignalInLatencyQuery:  envFileContentExpand("V1_MODEL_APP_SIGNAL_IN_LATENCY_QUERY", ""),
	AppSignalOutTrafficQuery: envFileContentExpand("V1_MODEL_APP_SIGNAL_OUT_TRAFFIC_QUERY", ""),
	AppSignalOutErrorsQuery:  envFileContentExpand("V1_MODEL_APP_SIGNAL_OUT_ERRORS_QUERY", ""),
	AppSignalOutLatencyQuery: envFileContentExpand("V1_MODEL_APP_SIGNAL_OUT_LATENCY_QUERY", ""),
	AppSignalSaturationQuery: envFileContentExpand("V1_MODEL_APP_SIGNAL_SATURATION_QUERY", ""),
	AppSignalTolerance:       envGet("V1_MODEL_APP_SIGNAL_TOLERANCE", 0).(int),

	HostQuery:                 envFileContentExpand("V1_MODEL_HOST_QUERY", ""),
	HostTolerance:             envGet("V1_MODEL_HOST_TOLERANCE", 0).(int),
	HostSignalCommonLabels:    envStringExpand("V1_MODEL_HOST_SIGNAL_COMMON_LABELS", ""),
	HostSignalSaturationQuery: envFileContentExpand("V1_MODEL_HOST_SIGNAL_SATURATION_QUERY", ""),

	Prometheus: toolsVendors.PrometheusOptions{
		URL:      envStringExpand("V1_MODEL_PROMETHEUS_URL", ""),
		User:     envStringExpand("V1_MODEL_PROMETHEUS_USER", ""),
		Password: envStringExpand("V1_MODEL_PROMETHEUS_PASSWORD", ""),
		Timeout:  envGet("V1_MODEL_PROMETHEUS_TIMEOUT", 30).(int),
		Insecure: envGet("V1_MODEL_PROMETHEUS_INSECURE", false).(bool),
		From:     envGet("V1_MODEL_PROMETHEUS_FROM", "-1h").(string),
		To:       envGet("V1_MODEL_PROMETHEUS_TO", "").(string),
		Step:     envGet("V1_MODEL_PROMETHEUS_STEP", "60s").(string),
		Params:   envGet("V1_MODEL_PROMETHEUS_PARAMS", "").(string),
	},

	Span: envGet("V1_MODEL_PROMETHEUS_SPAN", "").(string),
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

	flags.StringVar(&v1ModelOptions.AppQuery, "v1-model-app-query", v1ModelOptions.AppQuery, "V1 model prometheus app query")
	flags.IntVar(&v1ModelOptions.AppTolerance, "v1-model-app-tolerance", v1ModelOptions.AppTolerance, "V1 model prometheus app tolerance")

	flags.StringVar(&v1ModelOptions.AppSignalCommonLabels, "v1-model-app-signal-common-labels", v1ModelOptions.AppSignalCommonLabels, "V1 model prometheus app common labels")
	flags.StringVar(&v1ModelOptions.AppSignalInTrafficQuery, "v1-model-app-signal-in-traffic-query", v1ModelOptions.AppSignalInTrafficQuery, "V1 model prometheus app incoming traffic query")
	flags.StringVar(&v1ModelOptions.AppSignalInErrorsQuery, "v1-model-app-signal-in-errors-query", v1ModelOptions.AppSignalInErrorsQuery, "V1 model prometheus app incoming errors query")
	flags.StringVar(&v1ModelOptions.AppSignalInLatencyQuery, "v1-model-app-signal-in-latency-query", v1ModelOptions.AppSignalInLatencyQuery, "V1 model prometheus app incoming latency query")
	flags.StringVar(&v1ModelOptions.AppSignalOutTrafficQuery, "v1-model-app-signal-out-traffic-query", v1ModelOptions.AppSignalOutTrafficQuery, "V1 model prometheus app outgoing traffic query")
	flags.StringVar(&v1ModelOptions.AppSignalOutErrorsQuery, "v1-model-app-signal-out-errors-query", v1ModelOptions.AppSignalOutErrorsQuery, "V1 model prometheus app outgoing errors query")
	flags.StringVar(&v1ModelOptions.AppSignalOutLatencyQuery, "v1-model-app-signal-out-latency-query", v1ModelOptions.AppSignalOutLatencyQuery, "V1 model prometheus app outgoing latency query")
	flags.StringVar(&v1ModelOptions.AppSignalSaturationQuery, "v1-model-app-signal-saturation-query", v1ModelOptions.AppSignalSaturationQuery, "V1 model prometheus app saturation query")
	flags.IntVar(&v1ModelOptions.AppSignalTolerance, "v1-model-app-signal-tolerance", v1ModelOptions.AppSignalTolerance, "V1 model prometheus app signal tolerance")

	flags.StringVar(&v1ModelOptions.HostQuery, "v1-model-host-query", v1ModelOptions.HostQuery, "V1 model prometheus host query")
	flags.IntVar(&v1ModelOptions.HostTolerance, "v1-model-host-tolerance", v1ModelOptions.HostTolerance, "V1 model prometheus host tolerance")
	flags.StringVar(&v1ModelOptions.HostSignalCommonLabels, "v1-model-host-signal-common-labels", v1ModelOptions.HostSignalCommonLabels, "V1 model prometheus host common labels")
	flags.StringVar(&v1ModelOptions.HostSignalSaturationQuery, "v1-model-host-signal-saturation-query", v1ModelOptions.HostSignalSaturationQuery, "V1 model prometheus host saturation query")

	flags.StringVar(&v1ModelOptions.Prometheus.URL, "v1-model-prometheus-url", v1ModelOptions.Prometheus.URL, "V1 model prometheus url")
	flags.StringVar(&v1ModelOptions.Prometheus.User, "v1-model-prometheus-user", v1ModelOptions.Prometheus.User, "V1 model prometheus user")
	flags.StringVar(&v1ModelOptions.Prometheus.Password, "v1-model-prometheus-password", v1ModelOptions.Prometheus.Password, "V1 model prometheus password")
	flags.IntVar(&v1ModelOptions.Prometheus.Timeout, "v1-model-prometheus-timeout", v1ModelOptions.Prometheus.Timeout, "V1 model prometheus timeout")
	flags.BoolVar(&v1ModelOptions.Prometheus.Insecure, "v1-model-prometheus-insecure", v1ModelOptions.Prometheus.Insecure, "V1 model prometheus insecure")
	flags.StringVar(&v1ModelOptions.Prometheus.From, "v1-model-prometheus-from", v1ModelOptions.Prometheus.From, "V1 model prometheus from")
	flags.StringVar(&v1ModelOptions.Prometheus.To, "v1-model-prometheus-to", v1ModelOptions.Prometheus.To, "V1 model prometheus to")
	flags.StringVar(&v1ModelOptions.Prometheus.Step, "v1-model-prometheus-step", v1ModelOptions.Prometheus.Step, "V1 model prometheus step")
	flags.StringVar(&v1ModelOptions.Prometheus.Params, "v1-model-prometheus-params", v1ModelOptions.Prometheus.Params, "V1 model prometheus params")

	flags.StringVar(&v1ModelOptions.Span, "v1-model-prometheus-span", v1ModelOptions.Span, "V1 model prometheus span")

	trainCmd.AddCommand(&trainV1ModelCmd)

	return &trainCmd
}
