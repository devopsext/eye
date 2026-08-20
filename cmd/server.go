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

var promSignalOptions = datasource.PrometheusSignalOptions{

	AppQuery:                 envFileContentExpand("PROMETHEUS_APP_QUERY", ""),
	AppTolerance:             envGet("PROMETHEUS_APP_TOLERANCE", 0).(int),
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
}

var promLongOptions = datasource.PrometheusOptions{

	Name: envGet("PROMETHEUS_LONG_NAME", "Longterm").(string),
	Prometheus: toolsVendors.PrometheusOptions{
		URL:      envStringExpand("PROMETHEUS_LONG_URL", ""),
		User:     envStringExpand("PROMETHEUS_LONG_USER", ""),
		Password: envStringExpand("PROMETHEUS_LONG_PASSWORD", ""),
		Timeout:  envGet("PROMETHEUS_LONG_TIMEOUT", 30).(int),
		Insecure: envGet("PROMETHEUS_LONG_INSECURE", false).(bool),
		From:     envGet("PROMETHEUS_LONG_FROM", "-1h").(string),
		To:       envGet("PROMETHEUS_LONG_TO", "").(string),
		Step:     envGet("PROMETHEUS_LONG_STEP", "60s").(string),
		Params:   envGet("PROMETHEUS_LONG_PARAMS", "").(string),
	},
	Span:        envGet("PROMETHEUS_LONG_SPAN", "").(string),
	Window:      envGet("PROMETHEUS_LONG_WINDOW", "").(string),
	Schedule:    envGet("PROMETHEUS_LONG_SCHEDULE", "").(string),
	Concurrency: envGet("PROMETHEUS_LONG_CONCURRENCY", 50).(int),
	TimeFormat:  envGet("PROMETHEUS_LONG_TIME_FORMAT", time.DateTime).(string),
	State:       envGet("PROMETHEUS_LONG_STATE", "").(string),
}

var promShortOptions = datasource.PrometheusOptions{

	Name: envGet("PROMETHEUS_LONG_NAME", "Shortterm").(string),
	Prometheus: toolsVendors.PrometheusOptions{
		URL:      envStringExpand("PROMETHEUS_SHORT_URL", ""),
		User:     envStringExpand("PROMETHEUS_SHORT_USER", ""),
		Password: envStringExpand("PROMETHEUS_SHORT_PASSWORD", ""),
		Timeout:  envGet("PROMETHEUS_SHORT_TIMEOUT", 30).(int),
		Insecure: envGet("PROMETHEUS_SHORT_INSECURE", false).(bool),
		From:     envGet("PROMETHEUS_SHORT_FROM", "-5m").(string),
		To:       envGet("PROMETHEUS_SHORT_TO", "").(string),
		Step:     envGet("PROMETHEUS_SHORT_STEP", "15s").(string),
		Params:   envGet("PROMETHEUS_SHORT_PARAMS", "").(string),
	},
	Span:        envGet("PROMETHEUS_SHORT_SPAN", "").(string),
	Window:      envGet("PROMETHEUS_SHORT_WINDOW", "").(string),
	Schedule:    envGet("PROMETHEUS_SHORT_SCHEDULE", "").(string),
	Concurrency: envGet("PROMETHEUS_SHORT_CONCURRENCY", 50).(int),
	TimeFormat:  envGet("PROMETHEUS_SHORT_TIME_FORMAT", time.DateTime).(string),
}

var testModelOptions = model.TestModelOptions{
	WaitInterval: envGet("TEST_MODEL_WAIT_INTERVAL", "").(string),
}

var forestModelOptions = model.ForestModelOptions{
	Path:        envGet("FOREST_MODEL_PATH", "").(string),
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
			schedules.Add(datasource.NewPrometheus(promLongOptions, promSignalOptions, obs, models.OnTrain))
			schedules.Add(datasource.NewPrometheus(promShortOptions, promSignalOptions, obs, models.OnDetect))
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

	// Prometheus signals
	flags.StringVar(&promSignalOptions.AppQuery, "prometheus-app-query", promSignalOptions.AppQuery, "Prometheus app query")
	flags.IntVar(&promSignalOptions.AppTolerance, "prometheus-app-tolerance", promSignalOptions.AppTolerance, "Prometheus app tolerance")
	flags.StringVar(&promSignalOptions.AppSignalCommonLabels, "prometheus-app-signal-common-labels", promSignalOptions.AppSignalCommonLabels, "Prometheus app common labels")
	flags.StringVar(&promSignalOptions.AppSignalInTrafficQuery, "prometheus-app-signal-in-traffic-query", promSignalOptions.AppSignalInTrafficQuery, "Prometheus app incoming traffic query")
	flags.StringVar(&promSignalOptions.AppSignalInErrorsQuery, "prometheus-app-signal-in-errors-query", promSignalOptions.AppSignalInErrorsQuery, "Prometheus app incoming errors query")
	flags.StringVar(&promSignalOptions.AppSignalInLatencyQuery, "prometheus-app-signal-in-latency-query", promSignalOptions.AppSignalInLatencyQuery, "Prometheus app incoming latency query")
	flags.StringVar(&promSignalOptions.AppSignalOutTrafficQuery, "prometheus-app-signal-out-traffic-query", promSignalOptions.AppSignalOutTrafficQuery, "Prometheus app outgoing traffic query")
	flags.StringVar(&promSignalOptions.AppSignalOutErrorsQuery, "prometheus-app-signal-out-errors-query", promSignalOptions.AppSignalOutErrorsQuery, "Prometheus app outgoing errors query")
	flags.StringVar(&promSignalOptions.AppSignalOutLatencyQuery, "prometheus-app-signal-out-latency-query", promSignalOptions.AppSignalOutLatencyQuery, "Prometheus app outgoing latency query")
	flags.StringVar(&promSignalOptions.AppSignalSaturationQuery, "prometheus-app-signal-saturation-query", promSignalOptions.AppSignalSaturationQuery, "Prometheus app saturation query")
	flags.IntVar(&promSignalOptions.AppSignalTolerance, "prometheus-app-signal-tolerance", promSignalOptions.AppSignalTolerance, "Prometheus app signal tolerance")
	flags.StringVar(&promSignalOptions.HostQuery, "prometheus-host-query", promSignalOptions.HostQuery, "Prometheus host query")
	flags.IntVar(&promSignalOptions.HostTolerance, "prometheus-host-tolerance", promSignalOptions.HostTolerance, "Prometheus host tolerance")
	flags.StringVar(&promSignalOptions.HostSignalCommonLabels, "prometheus-host-signal-common-labels", promSignalOptions.HostSignalCommonLabels, "Prometheus host common labels")
	flags.StringVar(&promSignalOptions.HostSignalSaturationQuery, "prometheus-host-signal-saturation-query", promSignalOptions.HostSignalSaturationQuery, "Prometheus host saturation query")
	flags.IntVar(&promSignalOptions.HostSignalTolerance, "prometheus-host-signal-tolerance", promSignalOptions.HostSignalTolerance, "Prometheus host signal tolerance")
	// Prometheus long term
	flags.StringVar(&promLongOptions.Name, "prometheus-long-name", promLongOptions.Name, "Prometheus long term name")
	flags.StringVar(&promLongOptions.Prometheus.URL, "prometheus-long-url", promLongOptions.Prometheus.URL, "Prometheus long term url")
	flags.StringVar(&promLongOptions.Prometheus.User, "prometheus-long-user", promLongOptions.Prometheus.User, "Prometheus long term user")
	flags.StringVar(&promLongOptions.Prometheus.Password, "prometheus-long-password", promLongOptions.Prometheus.Password, "Prometheus long term password")
	flags.IntVar(&promLongOptions.Prometheus.Timeout, "prometheus-long-timeout", promLongOptions.Prometheus.Timeout, "Prometheus long term timeout")
	flags.BoolVar(&promLongOptions.Prometheus.Insecure, "prometheus-long-insecure", promLongOptions.Prometheus.Insecure, "Prometheus long term insecure")
	flags.StringVar(&promLongOptions.Prometheus.From, "prometheus-long-from", promLongOptions.Prometheus.From, "Prometheus long term from")
	flags.StringVar(&promLongOptions.Prometheus.To, "prometheus-long-to", promLongOptions.Prometheus.To, "Prometheus long term to")
	flags.StringVar(&promLongOptions.Prometheus.Step, "prometheus-long-step", promLongOptions.Prometheus.Step, "Prometheus long term step")
	flags.StringVar(&promLongOptions.Prometheus.Params, "prometheus-long-params", promLongOptions.Prometheus.Params, "Prometheus long term params")
	flags.StringVar(&promLongOptions.Span, "prometheus-long-span", promLongOptions.Span, "Prometheus long term span")
	flags.StringVar(&promLongOptions.Window, "prometheus-long-window", promLongOptions.Window, "Prometheus long term window")
	flags.StringVar(&promLongOptions.Schedule, "prometheus-long-schedule", promLongOptions.Schedule, "Prometheus long term schedule")
	flags.IntVar(&promLongOptions.Concurrency, "prometheus-long-concurrency", promLongOptions.Concurrency, "Prometheus long term concurrency")
	flags.StringVar(&promLongOptions.TimeFormat, "prometheus-long-time-format", promLongOptions.TimeFormat, "Prometheus long term log time format")
	flags.StringVar(&promLongOptions.State, "prometheus-long-state", promLongOptions.State, "Prometheus long term state")
	// Prometheus short term
	flags.StringVar(&promShortOptions.Name, "prometheus-short-name", promShortOptions.Name, "Prometheus short term name")
	flags.StringVar(&promShortOptions.Prometheus.URL, "prometheus-short-url", promShortOptions.Prometheus.URL, "Prometheus short term url")
	flags.StringVar(&promShortOptions.Prometheus.User, "prometheus-short-user", promShortOptions.Prometheus.User, "Prometheus short term user")
	flags.StringVar(&promShortOptions.Prometheus.Password, "prometheus-short-password", promShortOptions.Prometheus.Password, "Prometheus short term password")
	flags.IntVar(&promShortOptions.Prometheus.Timeout, "prometheus-short-timeout", promShortOptions.Prometheus.Timeout, "Prometheus short term timeout")
	flags.BoolVar(&promShortOptions.Prometheus.Insecure, "prometheus-short-insecure", promShortOptions.Prometheus.Insecure, "Prometheus short term insecure")
	flags.StringVar(&promShortOptions.Prometheus.From, "prometheus-short-from", promShortOptions.Prometheus.From, "Prometheus short term from")
	flags.StringVar(&promShortOptions.Prometheus.To, "prometheus-short-to", promShortOptions.Prometheus.To, "Prometheus short term to")
	flags.StringVar(&promShortOptions.Prometheus.Step, "prometheus-short-step", promShortOptions.Prometheus.Step, "Prometheus short term step")
	flags.StringVar(&promShortOptions.Prometheus.Params, "prometheus-short-params", promShortOptions.Prometheus.Params, "Prometheus short term params")
	flags.StringVar(&promShortOptions.Span, "prometheus-short-span", promShortOptions.Span, "Prometheus short term span")
	flags.StringVar(&promShortOptions.Window, "prometheus-short-window", promShortOptions.Window, "Prometheus short term window")
	flags.StringVar(&promShortOptions.Schedule, "prometheus-short-schedule", promShortOptions.Schedule, "Prometheus short term schedule")
	flags.IntVar(&promShortOptions.Concurrency, "prometheus-short-concurrency", promShortOptions.Concurrency, "Prometheus short term concurrency")
	flags.StringVar(&promShortOptions.TimeFormat, "prometheus-short-time-format", promShortOptions.TimeFormat, "Prometheus short term log time format")

	// Test model
	flags.StringVar(&testModelOptions.WaitInterval, "test-model-wait-interval", testModelOptions.WaitInterval, "Test model wait interval")

	// Forest model
	flags.StringVar(&forestModelOptions.Path, "forest-model-path", forestModelOptions.Path, "Forest model path")
	flags.IntVar(&forestModelOptions.Concurrency, "forest-model-concurrency", forestModelOptions.Concurrency, "Forest model concurrency")
	flags.StringSliceVar(&forestModelOptions.Filter, "forest-model-filter", forestModelOptions.Filter, "Forest model filter")

	return &serverCmd
}
