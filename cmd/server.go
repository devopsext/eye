package cmd

import (
	"sync"

	"github.com/devopsext/eye/common"
	"github.com/devopsext/eye/handler"
	"github.com/devopsext/eye/model"
	"github.com/devopsext/eye/server"
	toolsVendors "github.com/devopsext/tools/vendors"
	"github.com/spf13/cobra"
)

var testModelOptions = model.TestModelOptions{
	Schedule:     envGet("TEST_MODEL_SCHEDULE", "").(string),
	WaitInterval: envGet("TEST_MODEL_WAIT_INTERVAL", "").(string),
}

var alphaModelOptions = model.AlphaModelOptions{
	FilePath:    envGet("ALPHA_MODEL_FILE_PATH", "").(string),
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
		From:     envGet("ALPHA_MODEL_FROM", "-1h").(string),
		To:       envGet("ALPHA_MODEL_TO", "").(string),
		Step:     envGet("ALPHA_MODEL_STEP", "60s").(string),
		Params:   envGet("ALPHA_MODEL_PARAMS", "").(string),
	},

	Span:        envGet("ALPHA_MODEL_SPAN", "").(string),
	Retention:   envGet("ALPHA_MODEL_RETENTION", "").(string),
	Schedule:    envGet("ALPHA_MODEL_SCHEDULE", "").(string),
	Concurrency: envGet("ALPHA_MODEL_CONCURRENCY", 100).(int),
}

var httpServerOptions = server.HttpServerOptions{
	ServerName: envGet("HTTP_SERVER_NAME", "").(string),
	Listen:     envGet("HTTP_SERVER_LISTEN", ":80").(string),
	Tls:        envGet("HTTP_SERVER_TLS", false).(bool),
	Insecure:   envGet("HTTP_SERVER_INSECURE", false).(bool),
	CA:         envGet("HTTP_SERVER_CA", "").(string),
	Crt:        envGet("HTTP_SERVER_CRT", "").(string),
	Key:        envGet("HTTP_SERVER_KEY", "").(string),
	Timeout:    envGet("HTTP_SERVER_TIMEOUT", 30).(int),
}

var httpHealthHandlerOptions = handler.HttpHealthHandlerOptions{
	URL: envGet("HTTP_HEALTH_URL", "/health").(string),
}

var httpInspectHandlerOptions = handler.HttpInspectHandlerOptions{
	URL: envGet("HTTP_INSPECT_URL", "/inspect").(string),
}

func NewServerCommand(wg *sync.WaitGroup) *cobra.Command {

	serverCmd := cobra.Command{
		Use:   "server",
		Short: "Server",
		Run: func(cmd *cobra.Command, args []string) {

			obs := common.NewObservability(logs, metrics)

			models := common.NewModels()
			models.Add(model.NewTestModel(testModelOptions, obs))
			models.Add(model.NewAlphaModel(alphaModelOptions, obs))

			trainServer := server.NewTrainServer(models, obs)

			handlers := common.NewHandlers()
			handlers.Add(handler.NewHttpHealthHandler(httpHealthHandlerOptions, obs))
			handlers.Add(handler.NewHttpInspectHandler(httpInspectHandlerOptions, obs))
			httpServer := server.NewHttpServer(httpServerOptions, handlers, obs)

			servers := common.NewServers()
			servers.Add(trainServer)
			servers.Add(httpServer)
			servers.Start(wg)

			wg.Wait()
		},
	}

	flags := serverCmd.PersistentFlags()

	// Http Server
	flags.StringVar(&httpServerOptions.ServerName, "http-server-name", httpServerOptions.ServerName, "Http server name")
	flags.StringVar(&httpServerOptions.Listen, "http-server-listen", httpServerOptions.Listen, "Http server listen")
	flags.BoolVar(&httpServerOptions.Tls, "http-server-tls", httpServerOptions.Tls, "Http server TLS")
	flags.BoolVar(&httpServerOptions.Insecure, "http-server-insecure", httpServerOptions.Insecure, "Http server insecure skip verify")
	flags.StringVar(&httpServerOptions.CA, "http-server-ca", httpServerOptions.CA, "Http server ca file or content")
	flags.StringVar(&httpServerOptions.Crt, "http-server-crt", httpServerOptions.Crt, "Http server crt file or content")
	flags.StringVar(&httpServerOptions.Key, "http-server-key", httpServerOptions.Key, "Http server key file or content")
	flags.IntVar(&httpServerOptions.Timeout, "http-server-timeout", httpServerOptions.Timeout, "Http server timeout")

	flags.StringVar(&httpHealthHandlerOptions.URL, "http-health-url", httpHealthHandlerOptions.URL, "Http health handler url")
	flags.StringVar(&httpInspectHandlerOptions.URL, "http-inspect-url", httpInspectHandlerOptions.URL, "Http inspect handler url")

	// Test model

	flags.StringVar(&testModelOptions.Schedule, "test-model-schedule", testModelOptions.Schedule, "Test model schedule")
	flags.StringVar(&testModelOptions.WaitInterval, "test-model-wait-interval", testModelOptions.WaitInterval, "Test model wait interval")

	// Alpha model

	flags.StringVar(&alphaModelOptions.FilePath, "alpha-model-file-path", alphaModelOptions.FilePath, "Alpha model file path")
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

	flags.StringVar(&alphaModelOptions.Prometheus.From, "alpha-model-from", alphaModelOptions.Prometheus.From, "Alpha model from")
	flags.StringVar(&alphaModelOptions.Prometheus.To, "alpha-model-to", alphaModelOptions.Prometheus.To, "Alpha model to")
	flags.StringVar(&alphaModelOptions.Prometheus.Step, "alpha-model-step", alphaModelOptions.Prometheus.Step, "Alpha model step")
	flags.StringVar(&alphaModelOptions.Prometheus.Params, "alpha-model-params", alphaModelOptions.Prometheus.Params, "Alpha model params")
	flags.StringVar(&alphaModelOptions.Span, "alpha-model-span", alphaModelOptions.Span, "Alpha model span")
	flags.StringVar(&alphaModelOptions.Retention, "alpha-model-retention", alphaModelOptions.Retention, "Alpha model retention")
	flags.StringVar(&alphaModelOptions.Schedule, "alpha-model-schedule", alphaModelOptions.Schedule, "Alpha model schedule")

	flags.IntVar(&alphaModelOptions.Concurrency, "alpha-model-concurrency", alphaModelOptions.Concurrency, "Alpha model concurrency")

	return &serverCmd
}
