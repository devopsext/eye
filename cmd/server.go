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

var promIssuesOptions = datasource.PrometheusIssuesOptions{
	Name:     envGet("PROMETHEUS_ISSUES_NAME", "Issues").(string),
	Schedule: envGet("PROMETHEUS_ISSUES_SCHEDULE", "").(string),
	Prometheus: toolsVendors.PrometheusOptions{
		URL:      envStringExpand("PROMETHEUS_ISSUES_URL", ""),
		User:     envStringExpand("PROMETHEUS_ISSUES_USER", ""),
		Password: envStringExpand("PROMETHEUS_ISSUES_PASSWORD", ""),
		Timeout:  envGet("PROMETHEUS_ISSUES_TIMEOUT", 30).(int),
		Insecure: envGet("PROMETHEUS_ISSUES_INSECURE", false).(bool),
		Query:    envFileContentExpand("PROMETHEUS_ISSUES_QUERY", ""),
		From:     envGet("PROMETHEUS_ISSUES_FROM", "").(string),
		To:       envGet("PROMETHEUS_ISSUES_TO", "").(string),
		Step:     envGet("PROMETHEUS_ISSUES_STEP", "60s").(string),
		Params:   envGet("PROMETHEUS_ISSUES_PARAMS", "partial_response=true").(string),
	},
	StartName:  envStringExpand("PROMETHEUS_ISSUES_START_NAME", ""),
	EndName:    envStringExpand("PROMETHEUS_ISSUES_END_NAME", ""),
	Names:      envStringExpand("PROMETHEUS_ISSUES_NAMES", ""),
	Pattern:    envStringExpand("PROMETHEUS_ISSUES_PATTERN", ""),
	TimeFormat: envGet("PROMETHEUS_ISSUES_TIME_FORMAT", time.RFC3339).(string),
	Preload:    envGet("PROMETHEUS_ISSUES_PRELOAD", false).(bool),
}

var promLongSignalOptions = datasource.PrometheusSignalOptions{

	AppQuery:           envFileContentExpand("PROMETHEUS_LONG_APP_QUERY", ""),
	AppCommonLabels:    envStringExpand("PROMETHEUS_LONG_APP_COMMON_LABELS", ""),
	AppInTrafficQuery:  envFileContentExpand("PROMETHEUS_LONG_APP_IN_TRAFFIC_QUERY", ""),
	AppInErrorsQuery:   envFileContentExpand("PROMETHEUS_LONG_APP_IN_ERRORS_QUERY", ""),
	AppInLatencyQuery:  envFileContentExpand("PROMETHEUS_LONG_APP_IN_LATENCY_QUERY", ""),
	AppOutTrafficQuery: envFileContentExpand("PROMETHEUS_LONG_APP_OUT_TRAFFIC_QUERY", ""),
	AppOutErrorsQuery:  envFileContentExpand("PROMETHEUS_LONG_APP_OUT_ERRORS_QUERY", ""),
	AppOutLatencyQuery: envFileContentExpand("PROMETHEUS_LONG_APP_OUT_LATENCY_QUERY", ""),
	AppSaturationQuery: envFileContentExpand("PROMETHEUS_LONG_APP_SATURATION_QUERY", ""),

	HostQuery:           envFileContentExpand("PROMETHEUS_LONG_HOST_QUERY", ""),
	HostCommonLabels:    envStringExpand("PROMETHEUS_LONG_HOST_COMMON_LABELS", ""),
	HostSaturationQuery: envFileContentExpand("PROMETHEUS_LONG_HOST_SATURATION_QUERY", ""),
}

var promLongOptions = datasource.PrometheusOptions{

	Name:     envGet("PROMETHEUS_LONG_NAME", "Longterm").(string),
	Schedule: envGet("PROMETHEUS_LONG_SCHEDULE", "").(string),
	Prometheus: toolsVendors.PrometheusOptions{
		URL:      envStringExpand("PROMETHEUS_LONG_URL", ""),
		User:     envStringExpand("PROMETHEUS_LONG_USER", ""),
		Password: envStringExpand("PROMETHEUS_LONG_PASSWORD", ""),
		Timeout:  envGet("PROMETHEUS_LONG_TIMEOUT", 30).(int),
		Insecure: envGet("PROMETHEUS_LONG_INSECURE", false).(bool),
		From:     envGet("PROMETHEUS_LONG_FROM", "").(string),
		To:       envGet("PROMETHEUS_LONG_TO", "").(string),
		Step:     envGet("PROMETHEUS_LONG_STEP", "60s").(string),
		Params:   envGet("PROMETHEUS_LONG_PARAMS", "partial_response=true").(string),
	},
	Span:        envGet("PROMETHEUS_LONG_SPAN", "").(string),
	Frame:       envGet("PROMETHEUS_LONG_FRAME", "").(string),
	Concurrency: envGet("PROMETHEUS_LONG_CONCURRENCY", 50).(int),
	TimeFormat:  envGet("PROMETHEUS_LONG_TIME_FORMAT", time.DateTime).(string),
	State:       envGet("PROMETHEUS_LONG_STATE", "").(string),
	Interval:    envGet("PROMETHEUS_LONG_INTERVAL", "").(string),
}

var promShortSignalOptions = datasource.PrometheusSignalOptions{

	AppQuery:           envFileContentExpand("PROMETHEUS_SHORT_APP_QUERY", ""),
	AppCommonLabels:    envStringExpand("PROMETHEUS_SHORT_APP_COMMON_LABELS", ""),
	AppInTrafficQuery:  envFileContentExpand("PROMETHEUS_SHORT_APP_IN_TRAFFIC_QUERY", ""),
	AppInErrorsQuery:   envFileContentExpand("PROMETHEUS_SHORT_APP_IN_ERRORS_QUERY", ""),
	AppInLatencyQuery:  envFileContentExpand("PROMETHEUS_SHORT_APP_IN_LATENCY_QUERY", ""),
	AppOutTrafficQuery: envFileContentExpand("PROMETHEUS_SHORT_APP_OUT_TRAFFIC_QUERY", ""),
	AppOutErrorsQuery:  envFileContentExpand("PROMETHEUS_SHORT_APP_OUT_ERRORS_QUERY", ""),
	AppOutLatencyQuery: envFileContentExpand("PROMETHEUS_SHORT_APP_OUT_LATENCY_QUERY", ""),
	AppSaturationQuery: envFileContentExpand("PROMETHEUS_SHORT_APP_SATURATION_QUERY", ""),

	HostQuery:           envFileContentExpand("PROMETHEUS_SHORT_HOST_QUERY", ""),
	HostCommonLabels:    envStringExpand("PROMETHEUS_SHORT_HOST_COMMON_LABELS", ""),
	HostSaturationQuery: envFileContentExpand("PROMETHEUS_SHORT_HOST_SATURATION_QUERY", ""),
}

var promShortOptions = datasource.PrometheusOptions{

	Name:     envGet("PROMETHEUS_LONG_NAME", "Shortterm").(string),
	Schedule: envGet("PROMETHEUS_SHORT_SCHEDULE", "").(string),
	Prometheus: toolsVendors.PrometheusOptions{
		URL:      envStringExpand("PROMETHEUS_SHORT_URL", ""),
		User:     envStringExpand("PROMETHEUS_SHORT_USER", ""),
		Password: envStringExpand("PROMETHEUS_SHORT_PASSWORD", ""),
		Timeout:  envGet("PROMETHEUS_SHORT_TIMEOUT", 30).(int),
		Insecure: envGet("PROMETHEUS_SHORT_INSECURE", false).(bool),
		From:     envGet("PROMETHEUS_SHORT_FROM", "").(string),
		To:       envGet("PROMETHEUS_SHORT_TO", "").(string),
		Step:     envGet("PROMETHEUS_SHORT_STEP", "15s").(string),
		Params:   envGet("PROMETHEUS_SHORT_PARAMS", "partial_response=true").(string),
	},
	Span:        envGet("PROMETHEUS_SHORT_SPAN", "").(string),
	Frame:       envGet("PROMETHEUS_SHORT_FRAME", "").(string),
	Concurrency: envGet("PROMETHEUS_SHORT_CONCURRENCY", 50).(int),
	TimeFormat:  envGet("PROMETHEUS_SHORT_TIME_FORMAT", time.DateTime).(string),
	Interval:    envGet("PROMETHEUS_SHORT_INTERVAL", "").(string),
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
	Enabled:             envGet("FOREST_MODEL_ENABLED", false).(bool),
	Concurrency:         envGet("FOREST_MODEL_CONCURRENCY", 100).(int),
	Filter:              strings.Split(envStringExpand("FOREST_MODEL_FILTER", ""), ","),
	EngineTTL:           envGet("FOREST_MODEL_ENGINE_TTL", "1h").(string),
	DetectionTTL:        envGet("FOREST_MODEL_DETECTION_TTL", "5m").(string),
	DetectionMass:       envGet("FOREST_MODEL_DETECTION_MASS", 10.0).(float64),
	DetectionCategories: envGet("FOREST_MODEL_DETECTION_CATEGORIES", "").(string),
	DetectionImpacts:    envGet("FOREST_MODEL_DETECTION_IMPACTS", "").(string),
	DetectionScore:      envGet("FOREST_MODEL_DETECTION_SCORE", 5).(int),
	ApplicationOptions: forest.ApplicationEngineOptions{
		Path:                  envGet("FOREST_MODEL_APP_PATH", "").(string),
		TreesNumber:           envGet("FOREST_MODEL_APP_TREES_NUMBER", model.ForestModelTreesNumber).(int),
		SubsampleSize:         envGet("FOREST_MODEL_APP_SUBSAMPLE_SIZE", model.ForestModelSubsampleSize).(int),
		OutlierRatio:          envGet("FOREST_MODEL_APP_OUTLIER_RATIO", model.ForestModelOutlierRatio).(float64),
		InRequestsMaxSlots:    envGet("FOREST_MODEL_APP_IN_REQUESTS_MAX_SLOTS", 2).(int),
		InThroughputMaxSlots:  envGet("FOREST_MODEL_APP_IN_THROUGHPUT_MAX_SLOTS", 1).(int),
		OutRequestsMaxSlots:   envGet("FOREST_MODEL_APP_OUT_REQUESTS_MAX_SLOTS", 2).(int),
		OutThroughputMaxSlots: envGet("FOREST_MODEL_APP_OUT_THROUGHPUT_MAX_SLOTS", 1).(int),
		RangeMultiplier:       envGet("FOREST_MODEL_APP_RANGE_MULTIPLIER", 1.5).(float64),
		ProfileKind:           envGet("FOREST_MODEL_APP_PROFILE_KIND", "hour").(string),
		ShiftWeights:          envGet("FOREST_MODEL_APP_SHIFT_WEIGHTS", "").(string),
	},
	HostOptions: forest.HostEngineOptions{
		Path:            envGet("FOREST_MODEL_HOST_PATH", "").(string),
		TreesNumber:     envGet("FOREST_MODEL_HOST_TREES_NUMBER", model.ForestModelTreesNumber).(int),
		SubsampleSize:   envGet("FOREST_MODEL_HOST_SUBSAMPLE_SIZE", model.ForestModelSubsampleSize).(int),
		OutlierRatio:    envGet("FOREST_MODEL_HOST_OUTLIER_RATIO", model.ForestModelOutlierRatio).(float64),
		RangeMultiplier: envGet("FOREST_MODEL_HOST_RANGE_MULTIPLIER", 1.5).(float64),
		ProfileKind:     envGet("FOREST_MODEL_HOST_PROFILE_KIND", "hour").(string),
		ShiftWeights:    envGet("FOREST_MODEL_HOST_SHIFT_WEIGHTS", "").(string),
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
	Path:   envGet("HTTP_APP_PATH", "/app,/app/").(string),
	Page:   envStringExpand("HTTP_APP_PAGE", ""),
	Idents: envStringExpand("HTTP_APP_IDENTS", ""),
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

			modelSubscribers := &common.ModelSubscribers{
				Anomalies: notifiers.Subscribers(),
				Frames:    handlers.FrameSubscribers(),
				States:    handlers.StateSubscribers(),
			}

			issues := datasource.NewPrometheusIssues(promIssuesOptions, obs)

			models := common.NewModels(modelSubscribers)
			models.Add(model.NewTestModel(testModelOptions, obs))
			models.Add(model.NewForestModel(forestModelOptions, obs, issues))

			generators := common.NewGenerators()
			generators.Add(generator.NewPushgateway(pushgatewayOptions, obs))

			datasources := common.NewDataSources()
			datasources.Add(issues)
			datasources.Add(datasource.NewPrometheus(promLongOptions, promLongSignalOptions, obs, models.Train, generators.SetData))
			datasources.Add(datasource.NewPrometheus(promShortOptions, promShortSignalOptions, obs, models.Detect))

			schedules := common.NewSchedules()
			schedules.AddDatasources(datasources)
			schedules.AddGenerators(generators)

			servers := common.NewServers()
			servers.Add(server.NewScheduler(schedules, obs))
			servers.Add(server.NewHttpServer(httpServerOptions, handlers, obs))

			if promIssuesOptions.Preload {
				issues.Load()
			}
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

	// Prometheus long signals
	flags.StringVar(&promLongSignalOptions.AppQuery, "prometheus-long-app-query", promLongSignalOptions.AppQuery, "Prometheus long app query")
	flags.StringVar(&promLongSignalOptions.AppCommonLabels, "prometheus-long-app-common-labels", promLongSignalOptions.AppCommonLabels, "Prometheus long app common labels")
	flags.StringVar(&promLongSignalOptions.AppInTrafficQuery, "prometheus-long-app-in-traffic-query", promLongSignalOptions.AppInTrafficQuery, "Prometheus long app incoming traffic query")
	flags.StringVar(&promLongSignalOptions.AppInErrorsQuery, "prometheus-long-app-in-errors-query", promLongSignalOptions.AppInErrorsQuery, "Prometheus long app incoming errors query")
	flags.StringVar(&promLongSignalOptions.AppInLatencyQuery, "prometheus-long-app-in-latency-query", promLongSignalOptions.AppInLatencyQuery, "Prometheus long app incoming latency query")
	flags.StringVar(&promLongSignalOptions.AppOutTrafficQuery, "prometheus-long-app-out-traffic-query", promLongSignalOptions.AppOutTrafficQuery, "Prometheus long app outgoing traffic query")
	flags.StringVar(&promLongSignalOptions.AppOutErrorsQuery, "prometheus-long-app-out-errors-query", promLongSignalOptions.AppOutErrorsQuery, "Prometheus long app outgoing errors query")
	flags.StringVar(&promLongSignalOptions.AppOutLatencyQuery, "prometheus-long-app-out-latency-query", promLongSignalOptions.AppOutLatencyQuery, "Prometheus long app outgoing latency query")
	flags.StringVar(&promLongSignalOptions.AppSaturationQuery, "prometheus-long-app-saturation-query", promLongSignalOptions.AppSaturationQuery, "Prometheus long app saturation query")
	flags.StringVar(&promLongSignalOptions.HostQuery, "prometheus-long-host-query", promLongSignalOptions.HostQuery, "Prometheus long host query")
	flags.StringVar(&promLongSignalOptions.HostCommonLabels, "prometheus-long-host-common-labels", promLongSignalOptions.HostCommonLabels, "Prometheus long host common labels")
	flags.StringVar(&promLongSignalOptions.HostSaturationQuery, "prometheus-long-host-saturation-query", promLongSignalOptions.HostSaturationQuery, "Prometheus long host saturation query")
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
	flags.StringVar(&promLongOptions.Frame, "prometheus-long-frame", promLongOptions.Frame, "Prometheus long term frame")
	flags.StringVar(&promLongOptions.Schedule, "prometheus-long-schedule", promLongOptions.Schedule, "Prometheus long term schedule")
	flags.IntVar(&promLongOptions.Concurrency, "prometheus-long-concurrency", promLongOptions.Concurrency, "Prometheus long term concurrency")
	flags.StringVar(&promLongOptions.TimeFormat, "prometheus-long-time-format", promLongOptions.TimeFormat, "Prometheus long term log time format")
	flags.StringVar(&promLongOptions.State, "prometheus-long-state", promLongOptions.State, "Prometheus long term state")
	// Prometheus short signals
	flags.StringVar(&promShortSignalOptions.AppQuery, "prometheus-short-app-query", promShortSignalOptions.AppQuery, "Prometheus short app query")
	flags.StringVar(&promShortSignalOptions.AppCommonLabels, "prometheus-short-app-common-labels", promShortSignalOptions.AppCommonLabels, "Prometheus short app common labels")
	flags.StringVar(&promShortSignalOptions.AppInTrafficQuery, "prometheus-short-app-in-traffic-query", promShortSignalOptions.AppInTrafficQuery, "Prometheus short app incoming traffic query")
	flags.StringVar(&promShortSignalOptions.AppInErrorsQuery, "prometheus-short-app-in-errors-query", promShortSignalOptions.AppInErrorsQuery, "Prometheus short app incoming errors query")
	flags.StringVar(&promShortSignalOptions.AppInLatencyQuery, "prometheus-short-app-in-latency-query", promShortSignalOptions.AppInLatencyQuery, "Prometheus short app incoming latency query")
	flags.StringVar(&promShortSignalOptions.AppOutTrafficQuery, "prometheus-short-app-out-traffic-query", promShortSignalOptions.AppOutTrafficQuery, "Prometheus short app outgoing traffic query")
	flags.StringVar(&promShortSignalOptions.AppOutErrorsQuery, "prometheus-short-app-out-errors-query", promShortSignalOptions.AppOutErrorsQuery, "Prometheus short app outgoing errors query")
	flags.StringVar(&promShortSignalOptions.AppOutLatencyQuery, "prometheus-short-app-out-latency-query", promShortSignalOptions.AppOutLatencyQuery, "Prometheus short app outgoing latency query")
	flags.StringVar(&promShortSignalOptions.AppSaturationQuery, "prometheus-short-app-saturation-query", promShortSignalOptions.AppSaturationQuery, "Prometheus short app saturation query")
	flags.StringVar(&promShortSignalOptions.HostQuery, "prometheus-short-host-query", promShortSignalOptions.HostQuery, "Prometheus short host query")
	flags.StringVar(&promShortSignalOptions.HostCommonLabels, "prometheus-short-host-common-labels", promShortSignalOptions.HostCommonLabels, "Prometheus short host common labels")
	flags.StringVar(&promShortSignalOptions.HostSaturationQuery, "prometheus-short-host-saturation-query", promShortSignalOptions.HostSaturationQuery, "Prometheus short host saturation query")
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
	flags.StringVar(&promShortOptions.Frame, "prometheus-short-frame", promShortOptions.Frame, "Prometheus short term frame")
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
