package cmd

import (
	"strings"
	"sync"
	"time"

	"github.com/devopsext/eye/common"
	"github.com/devopsext/eye/datasource"
	"github.com/devopsext/eye/forest"
	"github.com/devopsext/eye/generator"
	"github.com/devopsext/eye/handler"
	"github.com/devopsext/eye/model"
	"github.com/devopsext/eye/notifier"
	"github.com/devopsext/eye/server"
	"github.com/devopsext/tools/vendors"
	toolsVendors "github.com/devopsext/tools/vendors"
	"github.com/spf13/cobra"
)

var slackOptions = notifier.SlackOptions{
	SlackOptions: vendors.SlackOptions{
		Timeout:  envGet("SLACK_TIMEOUT", 30).(int),
		Insecure: envGet("SLACK_INSECURE", false).(bool),
		Token:    envGet("SLACK_TOKEN", "").(string),
	},
	Channel:     envGet("SLACK_CHANNEL", "").(string),
	Message:     envFileContentExpand("SLACK_MESSAGE", ""),
	NotifyTTL:   envGet("SLACK_NOTIFY_TTL", "5m").(string),
	Concurrency: envGet("SLACK_CONCURRENCY", 5).(int),
}

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

	HostQuery:                 envFileContentExpand("PROMETHEUS_HOST_QUERY", ""),
	HostSignalCommonLabels:    envStringExpand("PROMETHEUS_HOST_SIGNAL_COMMON_LABELS", ""),
	HostSignalSaturationQuery: envFileContentExpand("PROMETHEUS_HOST_SIGNAL_SATURATION_QUERY", ""),
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

var pushgatewayOptions = generator.PushgatewayOptions{

	Name:     envGet("PUSHGATEWAY_NAME", "Pushgateway").(string),
	URL:      envStringExpand("PUSHGATEWAY_URL", ""),
	Timeout:  envGet("PUSHGATEWAY_TIMEOUT", 30).(int),
	Insecure: envGet("PUSHGATEWAY_INSECURE", false).(bool),
	Schedule: envGet("PUSHGATEWAY_SCHEDULE", "").(string),
	Files:    envGet("PUSHGATEWAY_FILES", "").(string),
	Outdir:   envGet("PUSHGATEWAY_OUTDIR", "").(string),
}

var testModelOptions = model.TestModelOptions{
	Enabled:      envGet("TEST_MODEL_ENABLED", false).(bool),
	WaitInterval: envGet("TEST_MODEL_WAIT_INTERVAL", "").(string),
}

var forestModelOptions = model.ForestModelOptions{
	Enabled:       envGet("FOREST_MODEL_ENABLED", false).(bool),
	Concurrency:   envGet("FOREST_MODEL_CONCURRENCY", 100).(int),
	Filter:        strings.Split(envStringExpand("FOREST_MODEL_FILTER", ""), ","),
	EngineTTL:     envGet("FOREST_MODEL_ENGINE_TTL", "1h").(string),
	DetectionTTL:  envGet("FOREST_MODEL_DETECTION_TTL", "5m").(string),
	DetectionMass: envGet("FOREST_MODEL_DETECTION_MASS", 10.0).(float64),
	ApplicationOptions: forest.ApplicationEngineOptions{
		Path:            envGet("FOREST_MODEL_APPLICATION_PATH", "").(string),
		TreesNumber:     envGet("FOREST_MODEL_APPLICATION_TREES_NUMBER", model.ForestModelTreesNumber).(int),
		SubsampleSize:   envGet("FOREST_MODEL_APPLICATION_SUBSAMPLE_SIZE", model.ForestModelSubsampleSize).(int),
		OutlierRatio:    envGet("FOREST_MODEL_APPLICATION_OUTLIER_RATIO", model.ForestModelOutlierRatio).(float64),
		TrafficMaxSlots: envGet("FOREST_MODEL_APPLICATION_TRAFFIC_MAX_SLOTS", model.ForestModelApplicationMaxSlots).(int),
		RangeMultiplier: envGet("FOREST_MODEL_APPLICATION_RANGE_MULTIPLIER", model.ForestModelApplicationRangeMultiplier).(float64),
		MinScore:        envGet("FOREST_MODEL_APPLICATION_MIN_SCORE", 6).(int),
		Categories:      envGet("FOREST_MODEL_APPLICATION_CATEGORIES", "").(string),
		Impacts:         envGet("FOREST_MODEL_APPLICATION_IMPACTS", "").(string),
	},
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
	Path: envGet("HTTP_HEALTH_PATH", "/health").(string),
}

var httpApplicationHandlerOptions = handler.HttpApplicationHandlerOptions{
	Path: envGet("HTTP_APPLICATION_PATH", "/app,/app/").(string),
	Page: envStringExpand("HTTP_APPLICATION_PAGE", ""),
}

var httpInspectHandlerOptions = handler.HttpInspectHandlerOptions{
	Path: envGet("HTTP_INSPECT_PATH", "/inspect").(string),
}

func NewServerCommand(wg *sync.WaitGroup) *cobra.Command {

	serverCmd := cobra.Command{
		Use:   "server",
		Short: "Server",
		Run: func(cmd *cobra.Command, args []string) {

			obs := common.NewObservability(logs, metrics)

			notifiers := common.NewNotifiers()
			notifiers.Add(notifier.NewSlack(slackOptions, obs))

			handlers := common.NewHandlers()
			handlers.Add(handler.NewHttpHealthHandler(httpHealthHandlerOptions, obs))
			handlers.Add(handler.NewHttpApplicationHandler(httpApplicationHandlerOptions, obs))
			handlers.Add(handler.NewHttpInspectHandler(httpInspectHandlerOptions, obs))

			models := common.NewModels(notifiers.Subscribers(), handlers.FrameSubscribers(), handlers.StateSubscribers())
			models.Add(model.NewTestModel(testModelOptions, obs))
			models.Add(model.NewForestModel(forestModelOptions, obs))

			generators := common.NewGenerators()
			generators.Add(generator.NewPushgateway(pushgatewayOptions, obs))

			datasources := common.NewDataSources()
			datasources.Add(datasource.NewPrometheus(promLongOptions, promSignalOptions, obs, models.Train, generators.SetData))
			datasources.Add(datasource.NewPrometheus(promShortOptions, promSignalOptions, obs, models.Detect))

			schedules := common.NewSchedules()
			schedules.AddDatasources(datasources)
			schedules.AddGenerators(generators)
			scheduler := server.NewScheduler(schedules, obs)

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

	flags.StringVar(&httpHealthHandlerOptions.Path, "http-health-path", httpHealthHandlerOptions.Path, "Http health handler path")
	flags.StringVar(&httpApplicationHandlerOptions.Path, "http-application-path", httpApplicationHandlerOptions.Path, "Http application handler path")
	flags.StringVar(&httpApplicationHandlerOptions.Page, "http-application-page", httpApplicationHandlerOptions.Page, "Http application handler page")
	flags.StringVar(&httpInspectHandlerOptions.Path, "http-inspect-path", httpInspectHandlerOptions.Path, "Http inspect handler path")

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
	flags.StringVar(&promSignalOptions.HostQuery, "prometheus-host-query", promSignalOptions.HostQuery, "Prometheus host query")
	flags.StringVar(&promSignalOptions.HostSignalCommonLabels, "prometheus-host-signal-common-labels", promSignalOptions.HostSignalCommonLabels, "Prometheus host common labels")
	flags.StringVar(&promSignalOptions.HostSignalSaturationQuery, "prometheus-host-signal-saturation-query", promSignalOptions.HostSignalSaturationQuery, "Prometheus host saturation query")
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
	flags.IntVar(&forestModelOptions.Concurrency, "forest-model-concurrency", forestModelOptions.Concurrency, "Forest model concurrency")
	flags.StringSliceVar(&forestModelOptions.Filter, "forest-model-filter", forestModelOptions.Filter, "Forest model filter")
	flags.StringVar(&forestModelOptions.EngineTTL, "forest-model-engine-ttl", forestModelOptions.EngineTTL, "Forest model engine ttl")
	flags.StringVar(&forestModelOptions.DetectionTTL, "forest-model-detection-ttl", forestModelOptions.DetectionTTL, "Forest model detection ttl")
	flags.Float64Var(&forestModelOptions.DetectionMass, "forest-model-detection-mass", forestModelOptions.DetectionMass, "Forest model detection mass threshold")
	// add application options ....

	// Slack
	flags.IntVar(&slackOptions.SlackOptions.Timeout, "slack-timeout", slackOptions.SlackOptions.Timeout, "Slack timeout")
	flags.BoolVar(&slackOptions.SlackOptions.Insecure, "slack-insecure", slackOptions.SlackOptions.Insecure, "Slack insecure")
	flags.StringVar(&slackOptions.SlackOptions.Token, "slack-token", slackOptions.SlackOptions.Token, "Slack token")
	flags.StringVar(&slackOptions.Channel, "slack-channel", slackOptions.Channel, "Slack channel")
	flags.StringVar(&slackOptions.Message, "slack-message", slackOptions.Message, "Slack message")
	flags.StringVar(&slackOptions.NotifyTTL, "slack-notify-ttl", slackOptions.NotifyTTL, "Slack notify ttl")
	flags.IntVar(&slackOptions.Concurrency, "slack-concurrency", slackOptions.Concurrency, "Slack concurrency")

	return &serverCmd
}
