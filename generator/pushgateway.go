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
	"time"

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
	Outdir   string
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

	lbs := v.Labels
	if lbs == nil {
		lbs = make(common.Labels)
	}

	if v.Devation != nil {
		lbs["generator_deviation"] = fmt.Sprintf("%g", *v.Devation)
	}
	if v.Value.Min != nil {
		lbs["generator_min"] = fmt.Sprintf("%g", *v.Value.Min)
	}
	if v.Value.Max != nil {
		lbs["generator_max"] = fmt.Sprintf("%g", *v.Value.Max)
	}
	if !utils.IsEmpty(v.Kind) {
		lbs["generator_kind"] = v.Kind
	}

	keys := slices.Collect(maps.Keys(lbs))
	slices.Sort(keys)

	var sb strings.Builder

	for idx, key := range keys {

		sign := "="
		format := "%s%s\"%s\""
		if idx > 0 {
			format = fmt.Sprintf(",%s", format)
		}
		fmt.Fprintf(&sb, format, key, sign, lbs[key])
	}
	return sb.String()
}

func (pg *Pushgateway) value(v *common.GeneratorValue) *float64 {

	if v == nil {
		return nil
	}

	var vl *float64

	switch v.Kind {
	case common.GeneratorValueKindMin:

		if v.Value.Min != nil {
			vl = v.Value.Min
		}
	case common.GeneratorValueKindMax:

		if v.Value.Max != nil {
			vl = v.Value.Max
		}
	case common.GeneratorValueKindAverage:

		if v.Value.Min != nil &&
			v.Value.Max != nil {

			min := *v.Value.Min
			max := *v.Value.Max
			mm := (min + max) / 2
			vl = &mm
		}
	default:
		mm := 1.0
		vl = &mm
	}

	if vl != nil && v.Devation != nil {
		mm := *vl
		d := rand.Float64() * *v.Devation
		mm = mm + d
		vl = &mm
	}
	return vl
}

func (pg *Pushgateway) push(values common.GeneratorValues) error {

	var sb strings.Builder
	name := pg.Name()

	for n, item := range values {

		if item == nil {
			continue
		}

		if item.Disabled {
			continue
		}

		if utils.IsEmpty(item.Metric) {
			continue
		}

		value := pg.value(item)
		if value == nil {
			continue
		}

		if item.Only {
			sb.Reset()
		}

		fmt.Fprintf(&sb, "# %s\n", n)

		labels := pg.labels(item)
		svalue := strconv.FormatFloat(*value, 'f', -1, 64)

		s := fmt.Sprintf("%s{%s} %s", item.Metric, labels, svalue)
		pg.logger.Debug("%s: %s", name, s)

		fmt.Fprintln(&sb, s)

		if item.Only {
			break
		}
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

func (pg *Pushgateway) save(values common.GeneratorValues, outdir string) error {

	if utils.IsEmpty(outdir) {
		return nil
	}

	if len(values) == 0 {
		return nil
	}

	name := pg.Name()

	stamp := time.Now().UnixMilli()

	dir := filepath.Join(outdir, fmt.Sprintf("%d", stamp))
	if !utils.DirExists(dir) {
		os.MkdirAll(dir, os.ModePerm)
	}

	findByMetric := func(metric string) common.GeneratorValues {

		r := make(common.GeneratorValues)
		for n, item := range values {
			if item.Metric == metric {
				r[n] = item
			}
		}
		return r
	}

	keys := []string{}
	for _, vl := range values {
		if !utils.Contains(keys, vl.Metric) {
			keys = append(keys, vl.Metric)
		}
	}
	slices.Sort(keys)

	for _, k := range keys {

		vls := findByMetric(k)
		if len(vls) == 0 {
			continue
		}

		data, err := yaml.Marshal(vls)
		if err != nil {
			pg.logger.Error("%s: Cannot marshal %s, error: %s", name, k, err)
			continue
		}

		path := filepath.Join(dir, fmt.Sprintf("%s.yaml", k))
		err = os.WriteFile(path, data, os.ModePerm)
		if err != nil {
			pg.logger.Error("%s: Cannot write to file %s, error: %s", name, path, err)
			continue
		}
	}

	return nil
}

func (pg *Pushgateway) SetData(data common.DataSourceData) error {

	if utils.IsEmpty(pg.options.Outdir) {
		return nil
	}

	schemas := data.Schemas()
	if schemas == nil {
		return nil
	}

	values := make(common.GeneratorValues)
	for _, schema := range schemas.Items() {
		if schema == nil {
			continue
		}

		hash := schema.Hash()
		metric := schema.Metric()
		labels := schema.Labels()

		name := fmt.Sprintf("%d", hash)

		min := schema.Min()
		max := schema.Max()
		dev := max - min

		value := &common.GeneratorValue{
			Metric:   metric,
			Labels:   labels,
			Kind:     common.GeneratorValueKindMin,
			Devation: &dev,
			Disabled: false,
			Only:     false,
		}

		value.Value.Min = &min
		value.Value.Max = &max
		values[name] = value
	}

	return pg.save(values, pg.options.Outdir)
}

func (pg *Pushgateway) load() map[string]common.GeneratorValues {

	name := pg.Name()

	if utils.IsEmpty(pg.options.Files) {
		return nil
	}

	files, err := filepath.Glob(pg.options.Files)
	if err != nil {
		pg.logger.Error("%s: Cannot find files %s, error: %s", name, pg.options.Files, err)
		return nil
	}

	if len(files) == 0 {
		pg.logger.Debug("%s: No files found %s", name, pg.options.Files)
		return nil
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
	return found
}

func (pg *Pushgateway) Start(wg *sync.WaitGroup) {

	if !pg.mu.TryLock() {
		return
	}
	defer pg.mu.Unlock()

	wg.Add(1)
	defer wg.Done()

	name := pg.Name()
	found := pg.load()

	if len(found) == 0 {
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
