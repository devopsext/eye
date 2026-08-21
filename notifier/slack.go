package notifier

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
	toolsRender "github.com/devopsext/tools/render"
	vendors "github.com/devopsext/tools/vendors"
	"github.com/devopsext/utils"
)

type SlackOptions struct {
	vendors.SlackOptions
	Channel string
	Message string
}

type Slack struct {
	options SlackOptions
	logger  sreCommon.Logger
	client  *vendors.Slack
	message *toolsRender.TextTemplate
}

func (s *Slack) Name() string {
	return "Slack"
}

func (s *Slack) renderTemplate(template *toolsRender.TextTemplate, obj interface{}) ([]byte, error) {

	b, err := template.RenderObject(obj)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (s *Slack) Notify(hashes []common.Hash) {

	name := s.Name()
	s.logger.Debug("%s: Notifying...", name)

	when := time.Now()

	d, err := s.renderTemplate(s.message, nil)
	if err != nil {
		s.logger.Error("%s: Rendering template error %s", name, err)
		return
	}

	sd := strings.TrimSpace(string(d))
	if utils.IsEmpty(sd) {
		s.logger.Error("%s: No result from template", name)
		return
	}

	opts := vendors.SlackMessageOptions{
		Channel: s.options.Channel, Text: string(d),
	}
	r, err := s.client.SendMessage(opts)
	if err != nil {
		s.logger.Error("%s: Cannot send message error %s", name, err)
		return
	}

	mr := vendors.SlackMessageResponse{}
	err = json.Unmarshal(r, &mr)
	if err != nil {
		s.logger.Error("%s: Cannot unmarshall response error %s", name, err)
		return
	}
	s.logger.Debug("%s: Notify finished in %s", name, time.Since(when))
}

func NewSlack(options SlackOptions, observability *common.Observability) *Slack {

	logger := observability.Logs()

	r := &Slack{
		options: options,
		logger:  logger,
	}

	name := r.Name()

	if utils.IsEmpty(options.Token) {
		logger.Debug("%s: Token is not defined.", name)
		return nil
	}

	if utils.IsEmpty(options.Message) {
		logger.Debug("%s: Message is not defined.", name)
		return nil
	}

	messageOpts := toolsRender.TemplateOptions{
		Content: options.Message,
	}
	message, err := toolsRender.NewTextTemplate(messageOpts, observability)
	if err != nil {
		logger.Error("%s: Message error: %s", name, err)
		return nil
	}

	r.client = vendors.NewSlack(options.SlackOptions)
	r.message = message

	return r
}
