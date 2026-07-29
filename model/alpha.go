package model

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"maps"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
	toolsVendors "github.com/devopsext/tools/vendors"
	"github.com/devopsext/utils"
	"github.com/jinzhu/copier"
	"github.com/puzpuzpuz/xsync/v4"

	jsoniter "github.com/json-iterator/go"
	"golang.org/x/sync/errgroup"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type AlphaModelSeriesValue struct {
	Hash  common.Hash
	Value float64
}

type AlphaModelSeries = map[common.Stamp][]AlphaModelSeriesValue

type AlphaModelOptions struct {
	File        string
	FileRewrite bool

	AppQuery                 string
	AppTolerance             int
	AppSignalCommonLabels    string
	AppSignalInTrafficQuery  string
	AppSignalInErrorsQuery   string
	AppSignalInLatencyQuery  string
	AppSignalOutTrafficQuery string
	AppSignalOutErrorsQuery  string
	AppSignalOutLatencyQuery string
	AppSignalSaturationQuery string
	AppSignalTolerance       int

	HostQuery                 string
	HostTolerance             int
	HostSignalCommonLabels    string
	HostSignalSaturationQuery string
	HostSignalTolerance       int

	Prometheus  toolsVendors.PrometheusOptions
	Span        string
	Concurrency int
}

type AlphaModel struct {
	options       AlphaModelOptions
	observability *common.Observability
	logger        sreCommon.Logger
}

type AlphaModelData struct {
	names        *common.Names
	attributes   *common.Attributes
	hosts        *common.Hosts
	applications *common.Applications
	measurements *common.Measurements
}

type AlphaModelFileHeader struct {
	Version uint16
}

type AlphaModelFileNames struct {
	Items common.NamesItems
}

type AlphaModelFileAttributes struct {
	Items common.AttributesItems
}

type AlphaModelFileHosts struct {
	Items common.HostsItems
}

type AlphaModelFileApplications struct {
	Items common.ApplicationsItems
}

type AlphaModelFileMeasurements struct {
	First common.Stamp
	Last  common.Stamp
	Items common.MeasurementsItems
}

type AlphaModelFile struct {
	model *AlphaModel
	path  string
	data  *AlphaModelData
}

const (
	AlphaModelFileVersionV0 = iota
	AlphaModelFileVersionV1
)

// AlphaModelFileNames

func (as *AlphaModelFileNames) GobEncode() ([]byte, error) {
	// We use an intermediate struct that holds a standard map
	type intermediate struct {
		Items map[common.Hash]string
	}

	temp := intermediate{
		Items: make(map[common.Hash]string),
	}

	as.Items.Range(func(k common.Hash, v string) bool {
		temp.Items[k] = v
		return true
	})

	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(temp)
	return buf.Bytes(), err
}

func (as *AlphaModelFileNames) GobDecode(data []byte) error {

	type intermediate struct {
		Items map[common.Hash]string
	}

	var temp intermediate
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&temp); err != nil {
		return err
	}

	as.Items = xsync.NewMap[common.Hash, string]()
	for k, v := range temp.Items {
		as.Items.Store(k, v)
	}
	return nil
}

// AlphaModelFileAttributes

func (as *AlphaModelFileAttributes) GobEncode() ([]byte, error) {
	// We use an intermediate struct that holds a standard map
	type intermediate struct {
		Items map[common.Hash]common.Labels
	}

	temp := intermediate{
		Items: make(map[common.Hash]common.Labels),
	}

	as.Items.Range(func(k common.Hash, v common.Labels) bool {
		temp.Items[k] = v
		return true
	})

	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(temp)
	return buf.Bytes(), err
}

func (as *AlphaModelFileAttributes) GobDecode(data []byte) error {

	type intermediate struct {
		Items map[common.Hash]common.Labels
	}

	var temp intermediate
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&temp); err != nil {
		return err
	}

	as.Items = xsync.NewMap[common.Hash, common.Labels]()
	for k, v := range temp.Items {
		as.Items.Store(k, v)
	}
	return nil
}

// AlphaModelFile

func (mf *AlphaModelFile) IsSupported(version uint16) bool {

	supported := []uint16{AlphaModelFileVersionV1}
	if utils.Contains(supported, version) {
		return true
	}
	return false
}

func (mf *AlphaModelFile) DecodeV1(decoder *gob.Decoder) error {

	// read names
	names := AlphaModelFileNames{}
	err := decoder.Decode(&names)
	if err != nil {
		return err
	}
	mf.data.names.SetItems(names.Items)

	// read attributes
	attributes := AlphaModelFileAttributes{}
	err = decoder.Decode(&attributes)
	if err != nil {
		return err
	}
	mf.data.attributes.SetItems(attributes.Items)

	// read hosts
	hosts := AlphaModelFileHosts{}
	err = decoder.Decode(&hosts)
	if err != nil {
		return err
	}
	mf.data.hosts.SetItems(hosts.Items)

	// read applications
	applications := AlphaModelFileApplications{}
	err = decoder.Decode(&applications)
	if err != nil {
		return err
	}
	mf.data.applications.SetItems(applications.Items)

	// read measurements
	measurements := AlphaModelFileMeasurements{}
	err = decoder.Decode(&measurements)
	if err != nil {
		return err
	}
	dms := mf.data.measurements
	dms.SetFirst(measurements.First)
	dms.SetLast(measurements.Last)
	dms.SetItems(measurements.Items)

	return nil
}

func (mf *AlphaModelFile) Load() error {

	f, err := os.Open(mf.path)
	if err != nil {
		return err
	}
	defer f.Close()

	br := bufio.NewReader(f)

	/*gr, err := zlib.NewReader(br)
	if err != nil {
		return err
	}
	defer gr.Close()*/

	decoder := gob.NewDecoder(br)

	// read header
	header := AlphaModelFileHeader{}
	err = decoder.Decode(&header)
	if err != nil {
		return err
	}

	if !mf.IsSupported(header.Version) {
		return fmt.Errorf("file version %d is not supported", header.Version)
	}

	switch header.Version {
	case AlphaModelFileVersionV1:
		return mf.DecodeV1(decoder)
	}
	return nil
}

func (mf *AlphaModelFile) EncodeV1(encoder *gob.Encoder) error {

	// write names
	names := AlphaModelFileNames{
		Items: mf.data.names.GetItems(),
	}
	err := encoder.Encode(&names)
	if err != nil {
		return err
	}

	// write attributes
	attributes := AlphaModelFileAttributes{
		Items: mf.data.attributes.GetItems(),
	}
	err = encoder.Encode(&attributes)
	if err != nil {
		return err
	}

	// write hosts
	hosts := AlphaModelFileHosts{
		Items: mf.data.hosts.GetItems(),
	}
	err = encoder.Encode(&hosts)
	if err != nil {
		return err
	}

	// write applications
	applications := AlphaModelFileApplications{
		Items: mf.data.applications.GetItems(),
	}
	err = encoder.Encode(&applications)
	if err != nil {
		return err
	}

	// write measurements
	dms := mf.data.measurements
	measurements := AlphaModelFileMeasurements{
		First: dms.GetFirst(),
		Last:  dms.GetLast(),
		Items: dms.GetItems(),
	}
	err = encoder.Encode(&measurements)
	if err != nil {
		return err
	}

	return nil
}

func (mf *AlphaModelFile) Save(version uint16) error {

	if !mf.IsSupported(version) {
		return fmt.Errorf("file version %d is not supported", version)
	}

	f, err := os.Create(mf.path)
	if err != nil {
		return err
	}
	defer f.Close()

	bw := bufio.NewWriter(f)
	defer bw.Flush()

	/*gw := zlib.NewWriter(bw)
	defer gw.Close()*/

	encoder := gob.NewEncoder(bw)

	// write header
	err = encoder.Encode(AlphaModelFileHeader{
		Version: version,
	})
	if err != nil {
		return err
	}

	switch version {
	case AlphaModelFileVersionV0:
		// nothing here
	case AlphaModelFileVersionV1:
		return mf.EncodeV1(encoder)
	}
	return nil
}

// AlphaModel

func (m *AlphaModel) Name() string {
	return "AlphaModel"
}

func (m *AlphaModel) info(msg any, args ...any) {
	gid := utils.GoRoutineID()
	m.logger.Info(fmt.Sprintf("%v: [%d] %v", m.Name(), gid, msg), args...)
}

func (m *AlphaModel) error(msg any, args ...any) {
	gid := utils.GoRoutineID()
	m.logger.Error(fmt.Sprintf("%v: [%d] %v", m.Name(), gid, msg), args...)
}

func (m *AlphaModel) debug(msg any, args ...any) {
	gid := utils.GoRoutineID()
	m.logger.Debug(fmt.Sprintf("%v: [%d] %v", m.Name(), gid, msg), args...)
}

func (m *AlphaModel) addUniqueKeys(keys []string, ma []map[string]string) []string {

	r := keys
	for _, v := range ma {
		mKeys := common.GetStringKeys(v)
		for _, v2 := range mKeys {
			if utils.Contains(keys, v2) {
				continue
			}
			r = append(r, v2)
		}
	}
	return r
}

func (m *AlphaModel) sliceCommonLabels(parent, labels map[string]string) []map[string]string {

	r := []map[string]string{}
	pKeys := common.GetStringKeys(parent)
	lKeys := common.GetStringKeys(labels)
	if len(pKeys) == len(lKeys) {
		r = append(r, parent)
		return r
	}
	for k, v := range labels {
		if utils.Contains(pKeys, k) {
			continue
		}
		vv := common.RemoveEmptyStrings(strings.Split(v, ","))
		for _, v2 := range vv {
			lbs := make(map[string]string)
			maps.Copy(lbs, parent)
			lbs[k] = v2
			r2 := m.sliceCommonLabels(lbs, labels)
			r = append(r, r2...)
		}
		pKeys = m.addUniqueKeys(pKeys, r)
	}
	return r
}

func (m *AlphaModel) loadData(q string, from, to time.Time) (*common.PrometheusResponseData, error) {

	opts := toolsVendors.PrometheusOptions{}
	copier.Copy(&opts, &m.options.Prometheus)

	opts.Query = q
	opts.From = strconv.Itoa(int(from.UTC().Unix()))
	opts.To = strconv.Itoa(int(to.UTC().Unix()))

	prom := toolsVendors.NewPrometheus(opts)
	if prom == nil {
		return nil, fmt.Errorf("prometheus cannot create client")
	}

	d, err := prom.Get()
	if err != nil {
		return nil, err
	}

	var res common.PrometheusResponse
	if err := json.Unmarshal(d, &res); err != nil {
		return nil, err
	}

	if res.Status != "success" {
		return nil, fmt.Errorf("prometheus got wrong status %s", res.Status)
	}

	if !utils.Contains([]string{"vector", "matrix"}, res.Data.ResultType) {
		return nil, fmt.Errorf("prometheus supports only vector and matrix data")
	}

	if (res.Data == nil) || (len(res.Data.Result) == 0) {
		return nil, nil
	}
	return res.Data, nil
}

func (m *AlphaModel) getStampedValueSlow(values []any) (bool, common.Stamp, float64) {

	var stamp common.Stamp = 0
	var value float64 = 0.0

	if len(values) < 2 {
		return false, stamp, value
	}

	// get stamp
	s := fmt.Sprintf("%.0f", values[0])
	s = strings.ReplaceAll(s, ".", "")
	if len(s) == 13 { // unix millisec
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return false, stamp, value
		}
		stamp = common.Stamp(i)
	} else if len(s) == 10 { // unix sec
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return false, stamp, value
		}
		stamp = common.Stamp(i * 1000) // unix millisec
	}

	// get value
	s = fmt.Sprintf("%s", values[1])
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return false, stamp, value
	}
	value = f

	if stamp == 0 {
		return false, stamp, value
	}
	return true, stamp, value
}

func (m *AlphaModel) getStampedValue(values []any) (bool, common.Stamp, float64) {

	if len(values) < 2 {
		return false, 0, 0
	}

	// 1. Extract Timestamp using a type switch
	var rawStamp int64
	switch v := values[0].(type) {
	case float64:
		rawStamp = int64(v)
	case int64:
		rawStamp = v
	case int:
		rawStamp = int64(v)
	case string:
		// Fallback in case it actually arrived as a string
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return false, 0, 0
		}
		rawStamp = int64(parsed)
	default:
		return false, 0, 0
	}

	// 2. Validate digit length mathematically instead of using strings
	var stamp common.Stamp
	if rawStamp >= 1_000_000_000_000 && rawStamp <= 9_999_999_999_999 {
		// 13 digits: already in milliseconds
		stamp = common.Stamp(rawStamp)
	} else if rawStamp >= 1_000_000_000 && rawStamp <= 9_999_999_999 {
		// 10 digits: convert seconds to milliseconds
		stamp = common.Stamp(rawStamp * 1000)
	} else {
		// Handles the original stamp == 0 check and invalid lengths
		return false, 0, 0
	}

	// 3. Extract Value using a type switch
	var val float64
	switch v := values[1].(type) {
	case float64:
		val = v
	case int64:
		val = float64(v)
	case int:
		val = float64(v)
	case string:
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return false, 0, 0
		}
		val = parsed
	default:
		return false, 0, 0
	}

	return true, stamp, val
}

func (m *AlphaModel) loadHosts(names *common.Names, attributes *common.Attributes, q string, from, to time.Time) (*common.Hosts, error) {

	promData, err := m.loadData(q, from, to)
	if err != nil {
		return nil, err
	}

	hosts := common.NewHosts()
	if promData == nil {
		return hosts, nil
	}

	for _, dr := range promData.Result {

		name := dr.Labels[common.HostName]
		if utils.IsEmpty(name) {
			continue
		}
		nameOn := dr.Labels[common.HostOn]
		findOn := !utils.IsEmpty(nameOn)

		for _, v := range dr.Values {

			found, stamp, _ := m.getStampedValue(v)
			if !found {
				continue
			}

			hostLbs := maps.Clone(dr.Labels)

			var on *common.Host
			if findOn {
				nameHash := names.AddOrUpdate(nameOn)
				on = hosts.Find(stamp, nameHash)
				if on == nil {

					lbs := maps.Clone(hostLbs)
					lbs[common.HostName] = nameOn
					delete(lbs, common.HostOn)

					// delete all key apart from name
					for k := range maps.Keys(hostLbs) {
						if k == common.HostName {
							continue
						}
						delete(hostLbs, k)
					}
					labelsHash := attributes.AddOrUpdate(lbs)

					on = common.NewHost(nameHash, labelsHash, nil)
					hosts.AddOrUpdate(stamp, on)
				}
			}

			namehash := names.AddOrUpdate(name)
			host := hosts.Find(stamp, namehash)
			if host == nil {
				labelsHash := attributes.AddOrUpdate(hostLbs)
				host = common.NewHost(namehash, labelsHash, on)
			}
			hosts.AddOrUpdate(stamp, host)
		}
	}
	return hosts, nil
}

func (m *AlphaModel) gatherHostsByQuery(names *common.Names, attributes *common.Attributes, query string, from, to time.Time) (*common.Hosts, error) {

	when := time.Now()

	m.debug("Hosts gathering (%s / %s) ...", from, to)
	m.debug("Hosts gathering started => %s", query)

	hosts, err := m.loadHosts(names, attributes, query, from, to)
	if err != nil {
		return nil, err
	}

	ts, hs := hosts.Sizes()
	if ts > 0 {
		m.debug("Hosts gathering finished (%d over %d timeseries) in %s", hs, ts, time.Since(when))
	} else {
		m.debug("Hosts gathering finished (no data) in %s", time.Since(when))
	}
	return hosts, nil
}

func (m *AlphaModel) gatherHostsBySpan(data *AlphaModelData, query string, from, to time.Time, span time.Duration) error {

	tt := make(map[time.Time]time.Time)
	t1 := from

	for t1.Unix() < to.Unix() {

		t2 := t1.Add(span)
		tt[t1] = t2
		t1 = t2
	}

	gr := &errgroup.Group{}
	gr.SetLimit(m.options.Concurrency)

	mp := &sync.Map{}

	for t1, t2 := range tt {

		gr.Go(func() error {

			hsts, err := m.gatherHostsByQuery(data.names, data.attributes, query, t1, t2)
			if err != nil {
				return err
			}
			mp.Store(t1, hsts)
			return nil
		})
	}

	err := gr.Wait()
	if err != nil {
		return err
	}

	for t1 := range tt {

		v, ok := mp.Load(t1)
		if ok {
			d, ok := v.(*common.Hosts)
			if !ok {
				continue
			}
			data.hosts.Merge(d)
		}
	}
	return nil
}

func (m *AlphaModel) loadApplications(names *common.Names, attributes *common.Attributes, q string, from, to time.Time) (*common.Applications, error) {

	promData, err := m.loadData(q, from, to)
	if err != nil {
		return nil, err
	}

	apps := common.NewApplications()
	if promData == nil {
		return apps, nil
	}

	for _, dr := range promData.Result {

		name := dr.Labels[common.ApplicationName]
		if utils.IsEmpty(name) {
			continue
		}

		labelsHash := attributes.AddOrUpdate(dr.Labels)

		for _, v := range dr.Values {

			found, stamp, _ := m.getStampedValue(v)
			if !found {
				continue
			}

			nameHash := names.AddOrUpdate(name)
			app := apps.Find(stamp, nameHash)
			if app == nil {
				app = common.NewApplication(nameHash, labelsHash)
			}
			apps.AddOrUpdate(stamp, app)
		}
	}
	return apps, nil
}

func (m *AlphaModel) gatherApplicationsByQuery(names *common.Names, attributes *common.Attributes, query string, from, to time.Time) (*common.Applications, error) {

	when := time.Now()

	m.debug("Applications gathering (%s / %s) ...", from, to)
	m.debug("Applications gathering started => %s", query)

	apps, err := m.loadApplications(names, attributes, query, from, to)
	if err != nil {
		return nil, err
	}

	ts, hs := apps.Sizes()
	if ts > 0 {
		m.debug("Applications gathering finished (%d over %d timeseries) in %s", hs, ts, time.Since(when))
	} else {
		m.debug("Applications gathering finished (no data) in %s", time.Since(when))
	}
	return apps, nil
}

func (m *AlphaModel) gatherApplicationsBySpan(data *AlphaModelData, query string, from, to time.Time, span time.Duration) error {

	tt := make(map[time.Time]time.Time)
	t1 := from

	for t1.Unix() < to.Unix() {

		t2 := t1.Add(span)
		tt[t1] = t2
		t1 = t2
	}

	gr := &errgroup.Group{}
	gr.SetLimit(m.options.Concurrency)

	mp := &sync.Map{}

	for t1, t2 := range tt {

		gr.Go(func() error {

			apps, err := m.gatherApplicationsByQuery(data.names, data.attributes, query, t1, t2)
			if err != nil {
				return err
			}
			mp.Store(t1, apps)
			return nil
		})
	}

	err := gr.Wait()
	if err != nil {
		return err
	}

	for t1 := range tt {

		v, ok := mp.Load(t1)
		if ok {
			d, ok := v.(*common.Applications)
			if !ok {
				continue
			}
			data.applications.Merge(d)
		}
	}
	return nil
}

func (m *AlphaModel) loadModelData(data *AlphaModelData, q string, from, to time.Time) (AlphaModelSeries, error) {

	promData, err := m.loadData(q, from, to)
	if err != nil {
		return nil, err
	}

	modelData := AlphaModelSeries{}
	if promData == nil {
		return modelData, nil
	}

	for _, dr := range promData.Result {

		for _, v := range dr.Values {

			found, stamp, value := m.getStampedValue(v)
			if !found {
				continue
			}

			v := AlphaModelSeriesValue{
				Hash:  data.attributes.AddOrUpdate(dr.Labels),
				Value: value,
			}
			modelData[stamp] = append(modelData[stamp], v)
		}
	}
	return modelData, nil
}

func (m *AlphaModel) gatherSignals(data *AlphaModelData, queries map[common.SignalKind]string, from, to time.Time) (map[common.SignalKind]AlphaModelSeries, error) {

	gr := &errgroup.Group{}
	gr.SetLimit(m.options.Concurrency)

	mp := &sync.Map{}

	// gather signals
	for k, q := range queries {

		gr.Go(func() error {

			data, err := m.loadModelData(data, q, from, to)
			if err != nil {
				return err
			}

			mp.Store(k, data)
			return nil
		})
	}

	err := gr.Wait()
	if err != nil {
		return nil, err
	}

	r := make(map[common.SignalKind]AlphaModelSeries)

	for k := range queries {

		v, ok := mp.Load(k)
		if ok {
			d, ok := v.(AlphaModelSeries)
			if ok {
				r[k] = d
			}
		}
	}
	return r, nil
}

func (m *AlphaModel) prepareQuery(q string, labels map[string]string) string {

	s := q
	for k, v := range labels {
		s = strings.ReplaceAll(s, fmt.Sprintf(".%s", k), v)
	}
	return s
}

func (m *AlphaModel) prepareQueries(queries map[common.SignalKind]string, labels map[string]string) map[common.SignalKind]string {

	r := make(map[common.SignalKind]string)

	for k, q := range queries {
		v := m.prepareQuery(q, labels)
		if !utils.IsEmpty(v) {
			r[k] = v
		}
	}
	return r
}

func (m *AlphaModel) gatherSignalsByQueries(data *AlphaModelData, name string, queries map[common.SignalKind]string, labels map[string]string, from, to time.Time) ([]map[common.SignalKind]AlphaModelSeries, error) {

	when := time.Now()

	signals := []map[common.SignalKind]AlphaModelSeries{}

	m.debug("Signals gathering %s (%s / %s)...", name, from, to)

	if len(labels) > 0 {

		gr := &errgroup.Group{}
		gr.SetLimit(m.options.Concurrency)

		mp := &sync.Map{}
		arr := m.sliceCommonLabels(nil, labels)

		for k, lbs := range arr {

			gr.Go(func() error {

				qrs := m.prepareQueries(queries, lbs)
				for k, q := range qrs {
					m.debug("Signals gathering %s %s started => %s", name, common.SignalKindToString(k), q)
				}

				r, err := m.gatherSignals(data, qrs, from, to)
				if err != nil {
					return err
				}

				infos := []string{}
				for k, v := range r {
					infos = append(infos, fmt.Sprintf("%s: %d", common.SignalKindToString(k), len(v)))
				}
				if len(infos) > 0 {
					m.debug("Signals gathering %s finished%s in %s", name, fmt.Sprintf(" (%s)", strings.Join(infos, ", ")), time.Since(when))
				} else {
					m.debug("Signals gathering %s finished (no data) in %s", name, time.Since(when))
				}

				mp.Store(k, r)
				return nil
			})
		}

		err := gr.Wait()
		if err != nil {
			return nil, err
		}

		for k := range arr {

			v, ok := mp.Load(k)
			if ok {
				r, ok := v.(map[common.SignalKind]AlphaModelSeries)
				if ok {
					signals = append(signals, r)
				}
			}
		}

	} else {

		queries := m.prepareQueries(queries, nil)
		for k, q := range queries {
			m.debug("Signals gathering %s %s started => %s", name, common.SignalKindToString(k), q)
		}

		r, err := m.gatherSignals(data, queries, from, to)
		if err != nil {
			return nil, err
		}

		infos := []string{}
		for k, v := range r {
			infos = append(infos, fmt.Sprintf("%s: %d", common.SignalKindToString(k), len(v)))
		}
		if len(infos) > 0 {
			m.debug("Signals gathering %s finished%s in %s", name, fmt.Sprintf(" (%s)", strings.Join(infos, ", ")), time.Since(when))
		} else {
			m.debug("Signals gathering %s finished (no data) in %s", name, time.Since(when))
		}

		signals = append(signals, r)
	}
	return signals, nil
}

func (m *AlphaModel) reduceSignals(arr []map[common.SignalKind]AlphaModelSeries) map[common.SignalKind]AlphaModelSeries {

	r := make(map[common.SignalKind]AlphaModelSeries)

	for _, v := range arr {

		for k, d := range v {

			rk := r[k]
			if rk == nil {
				rk = d
			} else {

				for dk, dd := range d {

					rkt := rk[dk]
					if rkt == nil {
						rkt = dd
					} else {
						rkt = append(rkt, dd...)
					}
					rk[dk] = rkt
				}
			}
			r[k] = rk
		}
	}
	return r
}

func (m *AlphaModel) gatherSignalsBySpan(name string, data *AlphaModelData,
	queries map[common.SignalKind]string, labels map[string]string,
	from, to time.Time, span time.Duration) (map[common.SignalKind]AlphaModelSeries, error) {

	qs := make(map[common.SignalKind]string)
	for i, v := range queries {
		if utils.IsEmpty(v) {
			continue
		}
		qs[i] = v
	}

	if len(qs) == 0 {
		return nil, nil
	}

	tt := make(map[time.Time]time.Time)
	t1 := from

	for t1.Unix() < to.Unix() {

		t2 := t1.Add(span)
		tt[t1] = t2
		t1 = t2
	}

	gr := &errgroup.Group{}
	gr.SetLimit(m.options.Concurrency)

	mp := &sync.Map{}

	for t1, t2 := range tt {

		gr.Go(func() error {

			sqd, err := m.gatherSignalsByQueries(data, name, qs, labels, t1, t2)
			if err != nil {
				return err
			}
			d := m.reduceSignals(sqd)
			mp.Store(t1, d)
			return nil
		})
	}

	err := gr.Wait()
	if err != nil {
		return nil, err
	}

	dd := []map[common.SignalKind]AlphaModelSeries{}

	for t1 := range tt {

		v, ok := mp.Load(t1)
		if ok {
			d, ok := v.(map[common.SignalKind]AlphaModelSeries)
			if !ok {
				continue
			}
			dd = append(dd, d)
		}
	}

	r := m.reduceSignals(dd)

	return r, nil
}

func (m *AlphaModel) string2Time(ts string) time.Time {

	t, err := time.Parse(time.RFC3339Nano, ts)
	if err == nil {
		return t
	} else {

		d, err := time.ParseDuration(ts)
		if err == nil {
			td := time.Now().Add(d)
			return td
		}
	}

	i, err := strconv.Atoi(ts)
	if err != nil {
		return time.Now()
	}

	t = time.Unix(int64(i), 0)
	return t
}

func (m *AlphaModel) getApplicationHost(
	data *AlphaModelData, stamp common.Stamp,
	appName, hostName string, appHash, hostHash common.Hash,
	appHostHash common.Hash) bool {

	if utils.IsEmpty(appName) {
		return false
	}

	// find application & hosts in the dictionaries
	app := data.applications.FindWithTolerance(stamp, appHash, m.options.AppTolerance)
	host := data.hosts.FindWithTolerance(stamp, hostHash, m.options.HostTolerance)

	// find application signal in measurements
	if app == nil || host == nil {
		signal := data.measurements.FindApplicationSignalWithTolerance(stamp, appHostHash, m.options.AppSignalTolerance)
		if app == nil && signal != nil {
			appHash = signal.GetApplication()
			app = data.applications.Find(stamp, appHash)
		}
		if host == nil && signal != nil {
			hostHash = signal.GetHost()
			host = data.hosts.Find(stamp, hostHash)
		}
	}

	// create application if there are no
	if app == nil {
		appLbs := make(map[string]string)
		appLbs[common.ApplicationName] = appName
		app = common.NewApplication(appHash, data.attributes.AddOrUpdate(appLbs))
		data.applications.AddOrUpdate(stamp, app)
	}

	// create host if there are no
	if host == nil && !utils.IsEmpty(hostName) {
		hostLbs := make(map[string]string)
		hostLbs[common.HostName] = hostName
		host = common.NewHost(hostHash, data.attributes.AddOrUpdate(hostLbs), nil)
		data.hosts.AddOrUpdate(stamp, host)
	}

	return true
}

type applicationSignalsOverDataCallback = func(as *common.ApplicationSignal, kind common.SignalKind, value float64, hash common.Hash)

func (m *AlphaModel) applicationSignalsOverData(
	in map[common.SignalKind]AlphaModelSeries,
	out *AlphaModelData,
	callback applicationSignalsOverDataCallback) {

	for kind, iv := range in {
		for stamp, data := range iv {
			for _, v := range data {

				lbs := out.attributes.Find(v.Hash)
				if lbs == nil {
					continue
				}

				appName := lbs[common.ApplicationSignalName]
				appHash := out.names.AddOrUpdate(appName)

				hostName := lbs[common.ApplicationSignalHost]
				hostHash := out.names.AddOrUpdate(hostName)

				appHost := out.measurements.BuildApplicationSignalName(appName, hostName)
				appHostHash := out.names.AddOrUpdate(appHost)

				as := out.measurements.FindApplicationSignal(stamp, appHostHash)
				if as == nil {
					found := m.getApplicationHost(out, stamp, appName, hostName, appHash, hostHash, appHostHash)
					if !found {
						continue
					}
					as = common.NewApplicationSignal(appHostHash, appHash, hostHash)
				}
				out.measurements.AddOrUpdate(stamp, as)
				callback(as, kind, v.Value, v.Hash)
			}
		}
	}

}

func (m *AlphaModel) getHost(
	data *AlphaModelData, stamp common.Stamp, hostName string, hostHash common.Hash) bool {

	if utils.IsEmpty(hostName) {
		return false
	}

	// find hosts in the dictionaries
	host := data.hosts.FindWithTolerance(stamp, hostHash, m.options.HostTolerance)

	// find host signal in measurements
	if host == nil {
		signal := data.measurements.FindHostSignalWithTolerance(stamp, hostHash, m.options.HostSignalTolerance)
		if signal != nil {
			hostHash = signal.GetName()
			host = data.hosts.Find(stamp, hostHash)
		}
	}

	// create host if there are no
	if host == nil {
		hostLbs := make(map[string]string)
		hostLbs[common.HostName] = hostName
		host = common.NewHost(hostHash, data.attributes.AddOrUpdate(hostLbs), nil)
		data.hosts.AddOrUpdate(stamp, host)
	}

	return true
}

type hostSignalsOverDataCallback = func(hs *common.HostSignal, kind common.SignalKind, value float64, labels common.Hash)

func (m *AlphaModel) hostSignalsOverData(
	in map[common.SignalKind]AlphaModelSeries,
	out *AlphaModelData,
	callback hostSignalsOverDataCallback) {

	for kind, iv := range in {
		for stamp, data := range iv {
			for _, v := range data {

				lbs := out.attributes.Find(v.Hash)
				if lbs == nil {
					continue
				}

				host := lbs[common.HostSignalHost]
				hash := out.names.AddOrUpdate(host)

				hs := out.measurements.FindHostSignal(stamp, hash)
				if hs == nil {
					found := m.getHost(out, stamp, host, hash)
					if !found {
						continue
					}
					hs = common.NewHostSignal(hash)
				}
				out.measurements.AddOrUpdate(stamp, hs)
				callback(hs, kind, v.Value, v.Hash)
			}
		}
	}

}

func (m *AlphaModel) train(data *AlphaModelData) error {

	/* this could be saved to file */
	//  0. gather application and host infos +++
	//  1. gather incoming traffic, errors, latency per application +++
	//  2. gather outgoing traffic, errors, latency per application +++
	//  3. gather saturation per application +++
	//  4. make application signals (set application and host) based on incomings +++
	//  5. add application signals (set application and host) based on outgoings & saturation +++
	//  6. add saturation to host signals +++
	//  7. add frontends & backends to application signals ---
	//  8. load downtime periods for exclusion per application
	//  9. prepare application signals weighted data: rps, errors, latency, saturation for iforest per kind
	// 10. train iforest model based on weighted data and calculate anomaly bound per each application signals
	// 11. train model on moving window 4 weeks

	/* this could be checked periodically or on incoming requests */
	// 0. load model and handle requests
	// 1. make prediction over each application signals on scheduler or by request
	// 2. find out anomaly in applcation signals, check outgoing dependecies, make predictions by them as well
	// 3. provide metrics regarding anomalies
	// 4. send events, trigger AI & mcp (could be via chatbot) to find out why on certain application (raw logs, metrics and errors)

	if utils.IsEmpty(m.options.Prometheus.From) {
		return fmt.Errorf("Cannot train due to from time is not defined")
	}

	from := m.string2Time(m.options.Prometheus.From)

	to := time.Now()
	if !utils.IsEmpty(m.options.Prometheus.To) {
		to = m.string2Time(m.options.Prometheus.To)
	}

	span := to.Sub(from)
	if !utils.IsEmpty(m.options.Span) {
		s, err := time.ParseDuration(m.options.Span)
		if err == nil {
			span = s
		}
	}

	m.info("Gathering hosts (%s / %s, span: %s)...", from, to, span)

	err := m.gatherHostsBySpan(data, m.options.HostQuery, from, to, span)
	if err != nil {
		return err
	}

	m.info("Gathering applications (%s / %s, span: %s)...", from, to, span)

	err = m.gatherApplicationsBySpan(data, m.options.AppQuery, from, to, span)
	if err != nil {
		return err
	}

	m.info("Gathering signals (%s / %s, span: %s)...", from, to, span)

	appLabels := utils.MapGetKeyValuesEx(m.options.AppSignalCommonLabels, ";", "=")

	queries := make(map[common.SignalKind]string)
	queries[common.SignalTraffic] = m.options.AppSignalInTrafficQuery
	queries[common.SignalErrors] = m.options.AppSignalInErrorsQuery
	queries[common.SignalLatency] = m.options.AppSignalInLatencyQuery

	mData, err := m.gatherSignalsBySpan("apps incoming", data, queries, appLabels, from, to, span)
	if err != nil {
		return err
	}

	// fill up app incomings
	m.applicationSignalsOverData(mData, data,
		func(as *common.ApplicationSignal, kind common.SignalKind, value float64, hash common.Hash) {

			switch kind {
			case common.SignalTraffic:
				as.IncomingTraffic.AddOrUpdate(value, hash, data.attributes)
			case common.SignalErrors:
				as.IncomingErrors.AddOrUpdate(value, hash)
			case common.SignalLatency:
				as.IncomingLatency.AddOrUpdate(value, hash)
			}
		})

	queries = make(map[common.SignalKind]string)
	queries[common.SignalTraffic] = m.options.AppSignalOutTrafficQuery
	queries[common.SignalErrors] = m.options.AppSignalOutErrorsQuery
	queries[common.SignalLatency] = m.options.AppSignalOutLatencyQuery
	queries[common.SignalSaturation] = m.options.AppSignalSaturationQuery

	mData, err = m.gatherSignalsBySpan("apps outgoing & saturation", data, queries, appLabels, from, to, span)
	if err != nil {
		return err
	}

	// fill up app outgoings and saturations
	m.applicationSignalsOverData(mData, data,
		func(as *common.ApplicationSignal, kind common.SignalKind, value float64, hash common.Hash) {

			switch kind {
			case common.SignalTraffic:
				as.OutgoingTraffic.AddOrUpdate(value, hash, data.attributes)
			case common.SignalErrors:
				as.OutgoingErrors.AddOrUpdate(value, hash)
			case common.SignalLatency:
				as.OutgoingLatency.AddOrUpdate(value, hash)
			case common.SignalSaturation:
				as.Saturation.AddOrUpdate(value, hash, data.attributes)
			}
		})

	hostLabels := utils.MapGetKeyValuesEx(m.options.HostSignalCommonLabels, ";", "=")

	queries = make(map[common.SignalKind]string)
	queries[common.SignalSaturation] = m.options.HostSignalSaturationQuery

	mData, err = m.gatherSignalsBySpan("hosts", data, queries, hostLabels, from, to, span)
	if err != nil {
		return err
	}

	// fill up host saturations
	m.hostSignalsOverData(mData, data,
		func(hs *common.HostSignal, kind common.SignalKind, value float64, labels common.Hash) {

			switch kind {
			case common.SignalSaturation:
				hs.Saturation.AddOrUpdate(value, labels, data.attributes)
			}
		})

	//last := measurements.LastStamp()

	/*
		// all last application signals
		appSignals := measurements.ApplicationSignalsByNames(last, nil)
		if !utils.IsEmpty(appSignals) {

			for _, as := range appSignals {

				n := as.Name()
				// show frontends
				frontedns := as.Frontends(last)
				for _, f := range frontedns {
					m.debug("Frontend %s => %s", f.Name(), n)
				}

				// show backends
				backends := as.Backends(last)
				for _, b := range backends {
					m.debug("Backend %s => %s", n, b.Name())
				}
			}
		}
	*/

	/*
			// all last host signals
			hostSignals := measurements.HostSignals(last, nil)
			if !utils.IsEmpty(hostSignals) {

				for _, hs := range hostSignals {
					m.debug(hs.Name())
				}
			}

			// all last applications
			apps := measurements.Applications(last, nil)
			if !utils.IsEmpty(apps) {

				for _, a := range apps {
					m.debug("Application %s", a.Name())
				}
			}

			// all last hosts
			hsts := measurements.Hosts(last, nil)
			if !utils.IsEmpty(hsts) {

				for _, h := range hsts {
					m.debug("Host %s", h.Name())
				}
			}



		deps := measurements.DependenciesByNames(last, []string{"other"})
		if !utils.IsEmpty(deps) {

			for a, arr := range deps.Items() {
				hosts := m.hostNames(measurements, last, a)
				m.debugDependecies(measurements, last, a.Name(), "", arr, hosts)
			}
		}*/

	return nil
}

/*
func (m *AlphaModel) hostNames(measurements *common.Measurements, stamp common.Stamp, app *common.Application) []string {

		hosts := measurements.Hosts(stamp, []*common.Application{app})

		arr := []string{}
		for _, h := range hosts {
			hName := h.Name()
			if !utils.Contains(arr, hName) {
				arr = append(arr, hName)
			}
		}
		if len(arr) > 0 {
			return arr
		}

		return []string{}
	}

func (m *AlphaModel) debugDependecies(measurements *common.Measurements, stamp common.Stamp, name, parent string, deps *common.Dependencies, hosts []string) {

		if deps == nil {
			m.debug("Dependency graph %s => %s (%s)", name, parent, strings.Join(hosts, ","))
			return
		}
		for a, arr := range deps.Items() {

			aName := a.Name()
			if parent != "" {
				aName = fmt.Sprintf("%s/%s", parent, aName)
			}
			hosts := m.hostNames(measurements, stamp, a)
			m.debugDependecies(measurements, stamp, name, aName, arr, hosts)
		}
	}
*/
func (m *AlphaModel) Train(wg *sync.WaitGroup) {

	wg.Add(1)
	go func(swg *sync.WaitGroup) {

		defer swg.Done()

		m.info("Training...")

		data := &AlphaModelData{
			attributes:   common.NewAttributes(),
			names:        common.NewNames(),
			hosts:        common.NewHosts(),
			applications: common.NewApplications(),
			measurements: common.NewMeasurements(),
		}

		file := &AlphaModelFile{
			model: m,
			path:  m.options.File,
			data:  data,
		}

		// load from file if there is file
		if utils.FileExists(file.path) && !m.options.FileRewrite {
			m.info("Loading from file %s...", file.path)
			when := time.Now()
			err := file.Load()
			if err != nil {
				m.error("Loading from file has error: %s", err)
			} else {
				m.info("Loading from file finished in %s", time.Since(when))
			}
		}

		when := time.Now()
		err := m.train(data)
		if err != nil {
			m.error("Training finished with error: %s", err)
			return
		}
		m.info("Training finished in %s", time.Since(when))

		// for optimization reasons
		//d, _ := time.ParseDuration("45s")
		//time.Sleep(d)

		// save to file if it's needed
		if !utils.IsEmpty(file.path) {
			m.info("Saving to file %s...", file.path)
			when := time.Now()
			err := file.Save(AlphaModelFileVersionV1)
			if err != nil {
				m.error("Saving to file has error: %s", err)
			} else {
				m.info("Saving to file finished in %s", time.Since(when))
			}
		}

	}(wg)
}

func NewAlphaModel(options AlphaModelOptions, observability *common.Observability) *AlphaModel {

	return &AlphaModel{
		options:       options,
		observability: observability,
		logger:        observability.Logs(),
	}
}
