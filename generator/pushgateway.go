package generator

import (
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
	"github.com/devopsext/utils"
	"gopkg.in/yaml.v2"
)

type PushgatewayOptions struct {
	Name     string
	URL      string
	Timeout  int
	Insecure bool
	Schedule string
	Files    string
}

type Pushgateway struct {
	mu            sync.Mutex
	options       PushgatewayOptions
	observability *common.Observability
	logger        sreCommon.Logger
	client        *http.Client
}

// Pushgateway

func (pg *Pushgateway) Name() string {
	return pg.options.Name
}

func (pg *Pushgateway) Schedule() string {
	return pg.options.Schedule
}

func (pg *Pushgateway) labels(v *common.GeneratorValue) string {

	if v == nil {
		return ""
	}

	if v.Labels == nil {
		return ""
	}

	keys := slices.Collect(maps.Keys(v.Labels))
	slices.Sort(keys)

	var sb strings.Builder

	for idx, key := range keys {

		format := "%s=\"%s\""
		if idx > 0 {
			format = fmt.Sprintf(",%s", format)
		}
		fmt.Fprintf(&sb, format, key, v.Labels[key])
	}
	return sb.String()
}

func (pg *Pushgateway) value(v *common.GeneratorValue) float64 {

	if v == nil {
		return 0
	}

	if v.Value.Default != nil {
		return *v.Value.Default
	}

	if v.Value.Min != nil &&
		v.Value.Max != nil {

		min := *v.Value.Min
		max := *v.Value.Max
		return min + rand.Float64()*(max-min)
	}

	return 1
}

func (pg *Pushgateway) push(values common.GeneratorValues) error {

	var sb strings.Builder
	name := pg.Name()

	for n, item := range values {

		if item == nil {
			continue
		}

		if utils.IsEmpty(item.Metric) {
			continue
		}

		fmt.Fprintf(&sb, "# %s\n", n)

		labels := pg.labels(item)
		value := pg.value(item)
		svalue := strconv.FormatFloat(value, 'f', -1, 64)

		s := fmt.Sprintf("%s{%s} %s", item.Metric, labels, svalue)
		pg.logger.Debug("%s: %s", name, s)

		fmt.Fprintln(&sb, s)
	}

	s := sb.String()
	r, code, err := utils.HttpPostRawOutCode(pg.client, pg.options.URL, "text/plain; version=0.0.4", "", []byte(s))

	rs := string(r)
	if err != nil {
		if utils.IsEmpty(rs) {
			return err
		}
		return errors.New(rs)
	}

	if code < 200 && code >= 300 && !utils.IsEmpty(rs) {
		return errors.New(rs)
	}
	return nil
}

func (pg *Pushgateway) Generate(values common.GeneratorValues) error {
	return pg.push(values)
}

func (pg *Pushgateway) Start(wg *sync.WaitGroup) {

	if !pg.mu.TryLock() {
		return
	}
	defer pg.mu.Unlock()

	baseDir := filepath.Dir(pg.options.Files)
	if !utils.DirExists(baseDir) {
		return
	}

	wg.Add(1)
	defer wg.Done()

	name := pg.Name()
	pg.logger.Info("%s: Generating from %s...", name, baseDir)

	files, err := filepath.Glob(pg.options.Files)
	if err != nil {
		pg.logger.Error("%s: Cannot find files in %s, error: %s", name, baseDir, err)
		return
	}

	if len(files) == 0 {
		pg.logger.Info("%s: No files found in %s", name, baseDir)
		return
	}

	found := make(map[string]common.GeneratorValues)

	for _, file := range files {

		content, err := os.ReadFile(file)
		if err != nil {
			pg.logger.Error("%s: Cannot read file %s, error: %s", name, file, err)
			continue
		}

		values := make(common.GeneratorValues)
		err = yaml.Unmarshal(content, values)
		if err != nil {
			pg.logger.Error("%s: Cannot unmarshal file %s, error: %s", name, file, err)
			continue
		}
		found[file] = values
	}

	if len(found) == 0 {
		pg.logger.Info("%s: No values found in %s", name, baseDir)
		return
	}

	for n, item := range found {

		pg.logger.Debug("%s: Pushing %s...", name, n)

		err := pg.push(item)
		if err != nil {
			pg.logger.Error("%s: Cannot generate %s, error: %s", name, n, err)
			continue
		}
		pg.logger.Debug("%s: Pushed %s successful", name, n)
	}
}

func NewPushgateway(options PushgatewayOptions, observability *common.Observability) *Pushgateway {

	logger := observability.Logs()

	if utils.IsEmpty(options.URL) {
		return nil
	}

	return &Pushgateway{
		options:       options,
		observability: observability,
		logger:        logger,
		client:        utils.NewHttpClient(options.Timeout, options.Insecure),
	}
}
