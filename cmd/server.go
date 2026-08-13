package cmd

import (
	"strings"
	"sync"
	"time"

	"github.com/devopsext/eye/common"
	"github.com/devopsext/eye/datasource"
	"github.com/devopsext/eye/handler"
	"github.com/devopsext/eye/model"
	"github.com/devopsext/eye/server"
	toolsVendors "github.com/devopsext/tools/vendors"
	"github.com/spf13/cobra"
)

var prometheusOptions = datasource.PrometheusOptions{

	AppQuery:     envFileContentExpand("PROMETHEUS_APP_QUERY", ""),
	AppTolerance: envGet("PROMETHEUS_APP_TOLERANCE", 0).(int),

	AppSignalCommonLabels:    envStringExpand("PROMETHEUS_APP_SIGNAL_COMMON_LABELS", ""),
	AppSignalInTrafficQuery:  envFileContentExpand("PROMETHEUS_APP_SIGNAL_IN_TRAFFIC_QUERY", ""),
	AppSignalInErrorsQuery:   envFileContentExpand("PROMETHEUS_APP_SIGNAL_IN_ERRORS_QUERY", ""),
	AppSignalInLatencyQuery:  envFileContentExpand("PROMETHEUS_APP_SIGNAL_IN_LATENCY_QUERY", ""),
	AppSignalOutTrafficQuery: envFileContentExpand("PROMETHEUS_APP_SIGNAL_OUT_TRAFFIC_QUERY", ""),
	AppSignalOutErrorsQuery:  envFileContentExpand("PROMETHEUS_APP_SIGNAL_OUT_ERRORS_QUERY", ""),
	AppSignalOutLatencyQuery: envFileContentExpand("PROMETHEUS_APP_SIGNAL_OUT_LATENCY_QUERY", ""),
	AppSignalSaturationQuery: envFileContentExpand("PROMETHEUS_APP_SIGNAL_SATURATION_QUERY", ""),
	AppSignalTolerance:       envGet("PROMETHEUS_APP_SIGNAL_TOLERANCE", 0).(int),

	HostQuery:                 envFileContentExpand("PROMETHEUS_HOST_QUERY", ""),
	HostTolerance:             envGet("PROMETHEUS_HOST_TOLERANCE", 0).(int),
	HostSignalCommonLabels:    envStringExpand("PROMETHEUS_HOST_SIGNAL_COMMON_LABELS", ""),
	HostSignalSaturationQuery: envFileContentExpand("PROMETHEUS_HOST_SIGNAL_SATURATION_QUERY", ""),
	HostSignalTolerance:       envGet("PROMETHEUS_HOST_SIGNAL_TOLERANCE", 0).(int),

	Prometheus: toolsVendors.PrometheusOptions{
		URL:      envStringExpand("PROMETHEUS_URL", ""),
		User:     envStringExpand("PROMETHEUS_USER", ""),
		Password: envStringExpand("PROMETHEUS_PASSWORD", ""),
		Timeout:  envGet("PROMETHEUS_TIMEOUT", 30).(int),
		Insecure: envGet("PROMETHEUS_INSECURE", false).(bool),
		From:     envGet("PROMETHEUS_FROM", "-1h").(string),
		To:       envGet("PROMETHEUS_TO", "").(string),
		Step:     envGet("PROMETHEUS_STEP", "60s").(string),
		Params:   envGet("PROMETHEUS_PARAMS", "").(string),
	},

	Span:        envGet("PROMETHEUS_SPAN", "").(string),
	Window:      envGet("PROMETHEUS_WINDOW", "").(string),
	Schedule:    envGet("PROMETHEUS_SCHEDULE", "").(string),
	Concurrency: envGet("PROMETHEUS_CONCURRENCY", 100).(int),

	TimeFormat: envGet("PROMETHEUS_TIME_FORMAT", time.DateTime).(string),
}

var testModelOptions = model.TestModelOptions{
	Schedule:     envGet("TEST_MODEL_SCHEDULE", "").(string),
	WaitInterval: envGet("TEST_MODEL_WAIT_INTERVAL", "").(string),
}

var forestModelOptions = model.ForestModelOptions{
	FilePath:    envGet("FOREST_MODEL_FILE_PATH", "").(string),
	Retention:   envGet("FOREST_MODEL_RETENTION", "").(string),
	Schedule:    envGet("FOREST_MODEL_SCHEDULE", "").(string),
	Concurrency: envGet("FOREST_MODEL_CONCURRENCY", 100).(int),
	Filter:      strings.Split(envStringExpand("FOREST_MODEL_FILTER", ""), ","),
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
			models.Add(model.NewForestModel(forestModelOptions, obs))

			schedules := common.NewSchedules()
			schedules.Add(datasource.NewPrometheus(prometheusOptions, obs, models.OnData))
			scheduler := server.NewScheduler(schedules, obs)

			handlers := common.NewHandlers()
			handlers.Add(handler.NewHttpHealthHandler(httpHealthHandlerOptions, obs))
			handlers.Add(handler.NewHttpInspectHandler(httpInspectHandlerOptions, obs))
			http := server.NewHttpServer(httpServerOptions, handlers, obs)

			servers := common.NewServers()
			servers.Add(scheduler)
			servers.Add(http)
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

	// Prometheus

	flags.StringVar(&prometheusOptions.AppQuery, "prometheus-app-query", prometheusOptions.AppQuery, "Prometheus app query")
	flags.IntVar(&prometheusOptions.AppTolerance, "prometheus-app-tolerance", prometheusOptions.AppTolerance, "Prometheus app tolerance")

	flags.StringVar(&prometheusOptions.AppSignalCommonLabels, "prometheus-app-signal-common-labels", prometheusOptions.AppSignalCommonLabels, "Prometheus app common labels")
	flags.StringVar(&prometheusOptions.AppSignalInTrafficQuery, "prometheus-app-signal-in-traffic-query", prometheusOptions.AppSignalInTrafficQuery, "Prometheus app incoming traffic query")
	flags.StringVar(&prometheusOptions.AppSignalInErrorsQuery, "prometheus-app-signal-in-errors-query", prometheusOptions.AppSignalInErrorsQuery, "Prometheus app incoming errors query")
	flags.StringVar(&prometheusOptions.AppSignalInLatencyQuery, "prometheus-app-signal-in-latency-query", prometheusOptions.AppSignalInLatencyQuery, "Prometheus app incoming latency query")
	flags.StringVar(&prometheusOptions.AppSignalOutTrafficQuery, "prometheus-app-signal-out-traffic-query", prometheusOptions.AppSignalOutTrafficQuery, "Prometheus app outgoing traffic query")
	flags.StringVar(&prometheusOptions.AppSignalOutErrorsQuery, "prometheus-app-signal-out-errors-query", prometheusOptions.AppSignalOutErrorsQuery, "Prometheus app outgoing errors query")
	flags.StringVar(&prometheusOptions.AppSignalOutLatencyQuery, "prometheus-app-signal-out-latency-query", prometheusOptions.AppSignalOutLatencyQuery, "Prometheus app outgoing latency query")
	flags.StringVar(&prometheusOptions.AppSignalSaturationQuery, "prometheus-app-signal-saturation-query", prometheusOptions.AppSignalSaturationQuery, "Prometheus app saturation query")
	flags.IntVar(&prometheusOptions.AppSignalTolerance, "prometheus-app-signal-tolerance", prometheusOptions.AppSignalTolerance, "Prometheus app signal tolerance")

	flags.StringVar(&prometheusOptions.HostQuery, "prometheus-host-query", prometheusOptions.HostQuery, "Prometheus host query")
	flags.IntVar(&prometheusOptions.HostTolerance, "prometheus-host-tolerance", prometheusOptions.HostTolerance, "Prometheus host tolerance")
	flags.StringVar(&prometheusOptions.HostSignalCommonLabels, "prometheus-host-signal-common-labels", prometheusOptions.HostSignalCommonLabels, "Prometheus host common labels")
	flags.StringVar(&prometheusOptions.HostSignalSaturationQuery, "prometheus-host-signal-saturation-query", prometheusOptions.HostSignalSaturationQuery, "Prometheus host saturation query")
	flags.IntVar(&prometheusOptions.HostSignalTolerance, "prometheus-host-signal-tolerance", prometheusOptions.HostSignalTolerance, "Prometheus host signal tolerance")

	flags.StringVar(&prometheusOptions.Prometheus.URL, "prometheus-prometheus-url", prometheusOptions.Prometheus.URL, "Prometheus url")
	flags.StringVar(&prometheusOptions.Prometheus.User, "prometheus-prometheus-user", prometheusOptions.Prometheus.User, "Prometheus user")
	flags.StringVar(&prometheusOptions.Prometheus.Password, "prometheus-prometheus-password", prometheusOptions.Prometheus.Password, "Prometheus password")
	flags.IntVar(&prometheusOptions.Prometheus.Timeout, "prometheus-prometheus-timeout", prometheusOptions.Prometheus.Timeout, "Prometheus timeout")
	flags.BoolVar(&prometheusOptions.Prometheus.Insecure, "prometheus-prometheus-insecure", prometheusOptions.Prometheus.Insecure, "Prometheus insecure")

	flags.StringVar(&prometheusOptions.Prometheus.From, "prometheus-from", prometheusOptions.Prometheus.From, "Prometheus from")
	flags.StringVar(&prometheusOptions.Prometheus.To, "prometheus-to", prometheusOptions.Prometheus.To, "Prometheus to")
	flags.StringVar(&prometheusOptions.Prometheus.Step, "prometheus-step", prometheusOptions.Prometheus.Step, "Prometheus step")
	flags.StringVar(&prometheusOptions.Prometheus.Params, "prometheus-params", prometheusOptions.Prometheus.Params, "Prometheus params")
	flags.StringVar(&prometheusOptions.Span, "prometheus-span", prometheusOptions.Span, "Prometheus span")
	flags.StringVar(&prometheusOptions.Window, "prometheus-window", prometheusOptions.Window, "Prometheus window")
	flags.StringVar(&prometheusOptions.Schedule, "prometheus-schedule", prometheusOptions.Schedule, "Prometheus schedule")

	flags.IntVar(&prometheusOptions.Concurrency, "prometheus-concurrency", prometheusOptions.Concurrency, "Forest model concurrency")

	// Test model

	flags.StringVar(&testModelOptions.Schedule, "test-model-schedule", testModelOptions.Schedule, "Test model schedule")
	flags.StringVar(&testModelOptions.WaitInterval, "test-model-wait-interval", testModelOptions.WaitInterval, "Test model wait interval")

	// Forest model

	flags.StringVar(&forestModelOptions.FilePath, "forest-model-file-path", forestModelOptions.FilePath, "Forest model file path")
	flags.StringVar(&forestModelOptions.Retention, "forest-model-retention", forestModelOptions.Retention, "Forest model retention")
	flags.StringVar(&forestModelOptions.Schedule, "forest-model-schedule", forestModelOptions.Schedule, "Forest model schedule")
	flags.IntVar(&forestModelOptions.Concurrency, "forest-model-concurrency", forestModelOptions.Concurrency, "Forest model concurrency")
	flags.StringSliceVar(&forestModelOptions.Filter, "forest-model-filter", forestModelOptions.Filter, "Forest model filter")

	return &serverCmd
}
