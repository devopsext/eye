package cmd

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/devopsext/utils"
	"github.com/spf13/cobra"

	sreCommon "github.com/devopsext/sre/common"
	sreProvider "github.com/devopsext/sre/provider"
)

var version = "unknown"
var APPNAME = "TOOLS"

var logs = sreCommon.NewLogs()
var metrics = sreCommon.NewMetrics()
var stdout *sreProvider.Stdout
var mainWG sync.WaitGroup

type RootOptions struct {
	Logs     []string
	Metrics  []string
	Profiler string
}

var rootOptions = RootOptions{
	Logs:     strings.Split(envGet("LOGS", "stdout").(string), ","),
	Metrics:  strings.Split(envGet("METRICS", "prometheus").(string), ","),
	Profiler: envGet("PROFILER", "").(string),
}

var stdoutOptions = sreProvider.StdoutOptions{
	Format:          envGet("STDOUT_FORMAT", "text").(string),
	Level:           envGet("STDOUT_LEVEL", "info").(string),
	Template:        envGet("STDOUT_TEMPLATE", "{{.file}} {{.msg}}").(string),
	TimestampFormat: envGet("STDOUT_TIMESTAMP_FORMAT", time.RFC3339Nano).(string),
	TextColors:      envGet("STDOUT_TEXT_COLORS", true).(bool),
	Debug:           envGet("STDOUT_DEBUG", false).(bool),
}

/*var prometheusMetricsOptions = sreProvider.PrometheusOptions{
	URL:       envGet("PROMETHEUS_METRICS_URL", "/metrics").(string),
	Listen:    envGet("PROMETHEUS_METRICS_LISTEN", ":8080").(string),
	Prefix:    envGet("PROMETHEUS_METRICS_PREFIX", "").(string),
	GoRuntime: envGet("PROMETHEUS_METRICS_GO_RUNTIME", true).(bool),
}*/

func getOnlyEnv(key string) string {
	value, ok := os.LookupEnv(key)
	if ok {
		return value
	}
	return fmt.Sprintf("$%s", key)
}

func envGet(s string, def interface{}) interface{} {
	return utils.EnvGet(fmt.Sprintf("%s_%s", APPNAME, s), def)
}

func envStringExpand(s string, def string) string {
	snew := envGet(s, def).(string)
	return os.Expand(snew, getOnlyEnv)
}

func envFileContentExpand(s string, def string) string {
	snew := envGet(s, def).(string)
	bytes, err := utils.Content(snew)
	if err != nil {
		return def
	}
	return os.Expand(string(bytes), getOnlyEnv)
}

func Execute() {

	rootCmd := &cobra.Command{
		Use:   "eye",
		Short: "Eye",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {

			stdoutOptions.Version = version
			stdout = sreProvider.NewStdout(stdoutOptions)
			if utils.Contains(rootOptions.Logs, "stdout") && stdout != nil {
				stdout.SetCallerOffset(2)
				logs.Register(stdout)
			}

			logs.Info("Booting...")

			// Metrics
			/*prometheusMetricsOptions.Version = version
			prometheus := sreProvider.NewPrometheusMeter(prometheusMetricsOptions, logs, stdout)
			if utils.Contains(rootOptions.Metrics, "prometheus") && prometheus != nil {
				prometheus.StartInWaitGroup(&mainWG)
				metrics.Register(prometheus)
			}*/
		},
	}

	flags := rootCmd.PersistentFlags()

	flags.StringSliceVar(&rootOptions.Logs, "logs", rootOptions.Logs, "Log providers: stdout")
	flags.StringSliceVar(&rootOptions.Metrics, "metrics", rootOptions.Metrics, "Metric providers: prometheus")
	flags.StringVar(&rootOptions.Profiler, "profiler", rootOptions.Profiler, "Profiler address: aka localhost:6060")

	flags.StringVar(&stdoutOptions.Format, "stdout-format", stdoutOptions.Format, "Stdout format: json, text, template")
	flags.StringVar(&stdoutOptions.Level, "stdout-level", stdoutOptions.Level, "Stdout level: info, warn, error, debug, panic")
	flags.StringVar(&stdoutOptions.Template, "stdout-template", stdoutOptions.Template, "Stdout template")
	flags.StringVar(&stdoutOptions.TimestampFormat, "stdout-timestamp-format", stdoutOptions.TimestampFormat, "Stdout timestamp format")
	flags.BoolVar(&stdoutOptions.TextColors, "stdout-text-colors", stdoutOptions.TextColors, "Stdout text colors")
	flags.BoolVar(&stdoutOptions.Debug, "stdout-debug", stdoutOptions.Debug, "Stdout debug")

	/*flags.StringVar(&prometheusMetricsOptions.URL, "prometheus-metrics-url", prometheusMetricsOptions.URL, "Prometheus metrics endpoint url")
	flags.StringVar(&prometheusMetricsOptions.Listen, "prometheus-metrics-listen", prometheusMetricsOptions.Listen, "Prometheus metrics listen")
	flags.StringVar(&prometheusMetricsOptions.Prefix, "prometheus-metrics-prefix", prometheusMetricsOptions.Prefix, "Prometheus metrics prefix")
	flags.StringVar(&prometheusMetricsOptions.Version, "prometheus-metrics-version", prometheusMetricsOptions.Version, "Prometheus metrics version")
	flags.BoolVar(&prometheusMetricsOptions.GoRuntime, "prometheus-metrics-goruntime", prometheusMetricsOptions.GoRuntime, "Prometheus metrics goruntime")*/

	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the version number",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(version)
		},
	})

	rootCmd.AddCommand(NewTrainCommand(&mainWG))
	rootCmd.AddCommand(NewServerCommand(&mainWG))

	if err := rootCmd.Execute(); err != nil {
		stdout.Error(err)
		os.Exit(1)
	}
}
