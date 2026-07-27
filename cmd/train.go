package cmd

import (
	"sync"

	"github.com/devopsext/eye/common"
	"github.com/devopsext/eye/model"
	toolsVendors "github.com/devopsext/tools/vendors"
	"github.com/spf13/cobra"
)

var alphaModelOptions = model.AlphaModelOptions{
	File:        envGet("ALPHA_MODEL_FILE", "").(string),
	FileRewrite: envGet("ALPHA_MODEL_FILE_REWRITE", false).(bool),

	AppQuery:     envFileContentExpand("ALPHA_MODEL_APP_QUERY", ""),
	AppTolerance: envGet("ALPHA_MODEL_APP_TOLERANCE", 0).(int),

	AppSignalCommonLabels:    envStringExpand("ALPHA_MODEL_APP_SIGNAL_COMMON_LABELS", ""),
	AppSignalInTrafficQuery:  envFileContentExpand("ALPHA_MODEL_APP_SIGNAL_IN_TRAFFIC_QUERY", ""),
	AppSignalInErrorsQuery:   envFileContentExpand("ALPHA_MODEL_APP_SIGNAL_IN_ERRORS_QUERY", ""),
	AppSignalInLatencyQuery:  envFileContentExpand("ALPHA_MODEL_APP_SIGNAL_IN_LATENCY_QUERY", ""),
	AppSignalOutTrafficQuery: envFileContentExpand("ALPHA_MODEL_APP_SIGNAL_OUT_TRAFFIC_QUERY", ""),
	AppSignalOutErrorsQuery:  envFileContentExpand("ALPHA_MODEL_APP_SIGNAL_OUT_ERRORS_QUERY", ""),
	AppSignalOutLatencyQuery: envFileContentExpand("ALPHA_MODEL_APP_SIGNAL_OUT_LATENCY_QUERY", ""),
	AppSignalSaturationQuery: envFileContentExpand("ALPHA_MODEL_APP_SIGNAL_SATURATION_QUERY", ""),
	AppSignalTolerance:       envGet("ALPHA_MODEL_APP_SIGNAL_TOLERANCE", 0).(int),

	HostQuery:                 envFileContentExpand("ALPHA_MODEL_HOST_QUERY", ""),
	HostTolerance:             envGet("ALPHA_MODEL_HOST_TOLERANCE", 0).(int),
	HostSignalCommonLabels:    envStringExpand("ALPHA_MODEL_HOST_SIGNAL_COMMON_LABELS", ""),
	HostSignalSaturationQuery: envFileContentExpand("ALPHA_MODEL_HOST_SIGNAL_SATURATION_QUERY", ""),
	HostSignalTolerance:       envGet("ALPHA_MODEL_HOST_SIGNAL_TOLERANCE", 0).(int),

	Prometheus: toolsVendors.PrometheusOptions{
		URL:      envStringExpand("ALPHA_MODEL_PROMETHEUS_URL", ""),
		User:     envStringExpand("ALPHA_MODEL_PROMETHEUS_USER", ""),
		Password: envStringExpand("ALPHA_MODEL_PROMETHEUS_PASSWORD", ""),
		Timeout:  envGet("ALPHA_MODEL_PROMETHEUS_TIMEOUT", 30).(int),
		Insecure: envGet("ALPHA_MODEL_PROMETHEUS_INSECURE", false).(bool),
		From:     envGet("ALPHA_MODEL_PROMETHEUS_FROM", "-1h").(string),
		To:       envGet("ALPHA_MODEL_PROMETHEUS_TO", "").(string),
		Step:     envGet("ALPHA_MODEL_PROMETHEUS_STEP", "60s").(string),
		Params:   envGet("ALPHA_MODEL_PROMETHEUS_PARAMS", "").(string),
	},

	Span:        envGet("ALPHA_MODEL_PROMETHEUS_SPAN", "").(string),
	Concurrency: envGet("ALPHA_MODEL_PROMETHEUS_CONCURRENCY", 100).(int),
}

func NewTrainCommand(wg *sync.WaitGroup) *cobra.Command {

	trainCmd := cobra.Command{
		Use:   "train",
		Short: "Train model",
	}

	trainV1ModelCmd := cobra.Command{
		Use:   "v1",
		Short: "Train Alpha model",
		Run: func(cmd *cobra.Command, args []string) {

			obs := common.NewObservability(logs, metrics)
			logger := obs.Logs()

			logger.Info("Begin training Alpha model...")

			v1 := model.NewAlphaModel(alphaModelOptions, obs)
			v1.Train(wg)

			wg.Wait()

			logger.Info("End training Alpha model")
		},
	}

	flags := trainV1ModelCmd.PersistentFlags()
	flags.StringVar(&alphaModelOptions.File, "alpha-model-file", alphaModelOptions.File, "Alpha model file path")
	flags.BoolVar(&alphaModelOptions.FileRewrite, "alpha-model-file-rewrite", alphaModelOptions.FileRewrite, "Alpha model file rewrite")

	flags.StringVar(&alphaModelOptions.AppQuery, "alpha-model-app-query", alphaModelOptions.AppQuery, "Alpha model prometheus app query")
	flags.IntVar(&alphaModelOptions.AppTolerance, "alpha-model-app-tolerance", alphaModelOptions.AppTolerance, "Alpha model prometheus app tolerance")

	flags.StringVar(&alphaModelOptions.AppSignalCommonLabels, "alpha-model-app-signal-common-labels", alphaModelOptions.AppSignalCommonLabels, "Alpha model prometheus app common labels")
	flags.StringVar(&alphaModelOptions.AppSignalInTrafficQuery, "alpha-model-app-signal-in-traffic-query", alphaModelOptions.AppSignalInTrafficQuery, "Alpha model prometheus app incoming traffic query")
	flags.StringVar(&alphaModelOptions.AppSignalInErrorsQuery, "alpha-model-app-signal-in-errors-query", alphaModelOptions.AppSignalInErrorsQuery, "Alpha model prometheus app incoming errors query")
	flags.StringVar(&alphaModelOptions.AppSignalInLatencyQuery, "alpha-model-app-signal-in-latency-query", alphaModelOptions.AppSignalInLatencyQuery, "Alpha model prometheus app incoming latency query")
	flags.StringVar(&alphaModelOptions.AppSignalOutTrafficQuery, "alpha-model-app-signal-out-traffic-query", alphaModelOptions.AppSignalOutTrafficQuery, "Alpha model prometheus app outgoing traffic query")
	flags.StringVar(&alphaModelOptions.AppSignalOutErrorsQuery, "alpha-model-app-signal-out-errors-query", alphaModelOptions.AppSignalOutErrorsQuery, "Alpha model prometheus app outgoing errors query")
	flags.StringVar(&alphaModelOptions.AppSignalOutLatencyQuery, "alpha-model-app-signal-out-latency-query", alphaModelOptions.AppSignalOutLatencyQuery, "Alpha model prometheus app outgoing latency query")
	flags.StringVar(&alphaModelOptions.AppSignalSaturationQuery, "alpha-model-app-signal-saturation-query", alphaModelOptions.AppSignalSaturationQuery, "Alpha model prometheus app saturation query")
	flags.IntVar(&alphaModelOptions.AppSignalTolerance, "alpha-model-app-signal-tolerance", alphaModelOptions.AppSignalTolerance, "Alpha model prometheus app signal tolerance")

	flags.StringVar(&alphaModelOptions.HostQuery, "alpha-model-host-query", alphaModelOptions.HostQuery, "Alpha model prometheus host query")
	flags.IntVar(&alphaModelOptions.HostTolerance, "alpha-model-host-tolerance", alphaModelOptions.HostTolerance, "Alpha model prometheus host tolerance")
	flags.StringVar(&alphaModelOptions.HostSignalCommonLabels, "alpha-model-host-signal-common-labels", alphaModelOptions.HostSignalCommonLabels, "Alpha model prometheus host common labels")
	flags.StringVar(&alphaModelOptions.HostSignalSaturationQuery, "alpha-model-host-signal-saturation-query", alphaModelOptions.HostSignalSaturationQuery, "Alpha model prometheus host saturation query")
	flags.IntVar(&alphaModelOptions.HostSignalTolerance, "alpha-model-host-signal-tolerance", alphaModelOptions.HostSignalTolerance, "Alpha model prometheus host signal tolerance")

	flags.StringVar(&alphaModelOptions.Prometheus.URL, "alpha-model-prometheus-url", alphaModelOptions.Prometheus.URL, "Alpha model prometheus url")
	flags.StringVar(&alphaModelOptions.Prometheus.User, "alpha-model-prometheus-user", alphaModelOptions.Prometheus.User, "Alpha model prometheus user")
	flags.StringVar(&alphaModelOptions.Prometheus.Password, "alpha-model-prometheus-password", alphaModelOptions.Prometheus.Password, "Alpha model prometheus password")
	flags.IntVar(&alphaModelOptions.Prometheus.Timeout, "alpha-model-prometheus-timeout", alphaModelOptions.Prometheus.Timeout, "Alpha model prometheus timeout")
	flags.BoolVar(&alphaModelOptions.Prometheus.Insecure, "alpha-model-prometheus-insecure", alphaModelOptions.Prometheus.Insecure, "Alpha model prometheus insecure")
	flags.StringVar(&alphaModelOptions.Prometheus.From, "alpha-model-prometheus-from", alphaModelOptions.Prometheus.From, "Alpha model prometheus from")
	flags.StringVar(&alphaModelOptions.Prometheus.To, "alpha-model-prometheus-to", alphaModelOptions.Prometheus.To, "Alpha model prometheus to")
	flags.StringVar(&alphaModelOptions.Prometheus.Step, "alpha-model-prometheus-step", alphaModelOptions.Prometheus.Step, "Alpha model prometheus step")
	flags.StringVar(&alphaModelOptions.Prometheus.Params, "alpha-model-prometheus-params", alphaModelOptions.Prometheus.Params, "Alpha model prometheus params")

	flags.StringVar(&alphaModelOptions.Span, "alpha-model-prometheus-span", alphaModelOptions.Span, "Alpha model prometheus span")
	flags.IntVar(&alphaModelOptions.Concurrency, "alpha-model-prometheus-concurrency", alphaModelOptions.Concurrency, "Alpha model prometheus concurrency")

	trainCmd.AddCommand(&trainV1ModelCmd)

	return &trainCmd
}
