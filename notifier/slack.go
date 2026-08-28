package notifier

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
	toolsRender "github.com/devopsext/tools/render"
	vendors "github.com/devopsext/tools/vendors"
	"github.com/devopsext/utils"
	"github.com/jellydator/ttlcache/v3"
	"golang.org/x/sync/errgroup"
)

type SlackOptions struct {
	vendors.SlackOptions
	Channel     string
	Message     string
	NotifyTTL   string
	Concurrency int
}

type SlackNotify struct {
	begin    common.Stamp
	end      common.Stamp
	response *vendors.SlackMessageResponse
}

type Slack struct {
	options  SlackOptions
	logger   sreCommon.Logger
	client   *vendors.Slack
	message  *toolsRender.TextTemplate
	notifies *ttlcache.Cache[string, *SlackNotify]
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

func (s *Slack) notifyAnomaly(anomaly common.Anomaly, channel, thread string) (*vendors.SlackMessageResponse, error) {

	if utils.IsEmpty(anomaly) {
		return nil, nil
	}

	d, err := s.renderTemplate(s.message, anomaly)
	if err != nil {
		return nil, err
	}

	sd := strings.TrimSpace(string(d))
	if utils.IsEmpty(sd) {
		return nil, err
	}

	opts := vendors.SlackMessageOptions{
		Channel: channel,
		Thread:  thread,
		Text:    string(d),
	}
	r, err := s.client.SendMessage(opts)
	if err != nil {
		return nil, err
	}

	mr := &vendors.SlackMessageResponse{}
	err = json.Unmarshal(r, mr)
	if err != nil {
		return nil, err
	}
	return mr, nil
}

func (s *Slack) findNotify(id string) *SlackNotify {

	item := s.notifies.Get(id)
	if item == nil {
		return nil
	}
	return item.Value()
}

func (s *Slack) Notify(anomalies []common.Anomaly) {

	if len(anomalies) == 0 {
		return
	}

	name := s.Name()
	when := time.Now()

	s.logger.Debug("%s: Notifying %d...", name, len(anomalies))

	gr := &errgroup.Group{}
	gr.SetLimit(s.options.Concurrency)

	errs := make(chan error, len(anomalies))

	for _, a := range anomalies {

		gr.Go(func() error {

			id := a.ID()
			begin := a.Begin()
			end := a.End()

			channel := s.options.Channel
			thread := ""

			n := s.findNotify(id)
			if n == nil {
				s.logger.Debug("%s: Notify NOT found %s (%s / %s)...", name, id, begin, end)
				n = &SlackNotify{
					begin: begin,
					end:   end,
				}
			} else {
				s.logger.Debug("%s: Notify found %s (%s / %s)...", name, id, n.begin, n.end)
				mr := n.response
				if n.begin != begin || n.end != end {
					n.begin = begin
					n.end = end
				}
				if mr.OK {
					s.notifies.Set(id, n, ttlcache.DefaultTTL)
					return nil
				}
				thread = mr.TS
			}

			mr, err := s.notifyAnomaly(a, channel, thread)
			if err != nil {
				errs <- err
				return nil
			}
			if mr == nil {
				return nil
			}

			n.response = mr
			s.notifies.Set(id, n, ttlcache.DefaultTTL)
			return nil
		})
	}
	gr.Wait()
	close(errs)

	all := []error{}
	for e := range errs {
		all = append(all, e)
	}
	err := errors.Join(all...)
	if err != nil {
		s.logger.Error("%s: Notifying failed error %s", name, err)
		return
	}

	s.logger.Debug("%s: Notifying finished in %s", name, time.Since(when))
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

	ttl := common.DefaultTTL(options.NotifyTTL, 5*time.Minute)
	r.notifies = ttlcache.New(
		ttlcache.WithTTL[string, *SlackNotify](ttl),
	)
	return r
}
