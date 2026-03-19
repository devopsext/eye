package cmd

import (
	"sync"

	"github.com/devopsext/eye/common"
	"github.com/devopsext/eye/handler"
	"github.com/devopsext/eye/server"
	"github.com/spf13/cobra"
)

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

var httpMetricHandlerOptions = handler.HttpMetricHandlerOptions{
	URL: envGet("HTTP_METRIC_URL", "/metric").(string),
}

func NewServerCommand(wg *sync.WaitGroup) *cobra.Command {

	serverCmd := cobra.Command{
		Use:   "server",
		Short: "Server",
		Run: func(cmd *cobra.Command, args []string) {

			obs := common.NewObservability(logs, metrics)

			//models := common.NewModels()
			//models.Add(nil)

			handlers := common.NewHandlers()
			handlers.Add(handler.NewHttpHealthHandler(httpHealthHandlerOptions, obs))
			handlers.Add(handler.NewHttpMetricHandler(httpMetricHandlerOptions, obs))

			servers := common.NewServers()
			servers.Add(server.NewHttpServer(httpServerOptions, handlers, obs))
			servers.Start(wg)

			wg.Wait()
		},
	}

	flags := serverCmd.PersistentFlags()
	flags.StringVar(&httpServerOptions.ServerName, "http-server-name", httpServerOptions.ServerName, "Http server name")
	flags.StringVar(&httpServerOptions.Listen, "http-server-listen", httpServerOptions.Listen, "Http server listen")
	flags.BoolVar(&httpServerOptions.Tls, "http-server-tls", httpServerOptions.Tls, "Http server TLS")
	flags.BoolVar(&httpServerOptions.Insecure, "http-server-insecure", httpServerOptions.Insecure, "Http server insecure skip verify")
	flags.StringVar(&httpServerOptions.CA, "http-server-ca", httpServerOptions.CA, "Http server ca file or content")
	flags.StringVar(&httpServerOptions.Crt, "http-server-crt", httpServerOptions.Crt, "Http server crt file or content")
	flags.StringVar(&httpServerOptions.Key, "http-server-key", httpServerOptions.Key, "Http server key file or content")
	flags.IntVar(&httpServerOptions.Timeout, "http-server-timeout", httpServerOptions.Timeout, "Http server timeout")

	flags.StringVar(&httpHealthHandlerOptions.URL, "http-health-url", httpHealthHandlerOptions.URL, "Http health handler url")
	flags.StringVar(&httpMetricHandlerOptions.URL, "http-metric-url", httpMetricHandlerOptions.URL, "Http metric handler url")

	return &serverCmd
}
