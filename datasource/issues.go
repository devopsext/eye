package datasource

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
	toolsVendors "github.com/devopsext/tools/vendors"
	"github.com/devopsext/utils"
)

type PrometheusIssuesOptions struct {
	Name       string
	Schedule   string
	Prometheus toolsVendors.PrometheusOptions
	StartName  string
	EndName    string
	Names      string
	Pattern    string
	TimeFormat string
	Preload    bool
}

type PrometheusIssueHash struct {
	start common.Stamp
	end   common.Stamp
	hash  common.Hash
}

type PrometheusIssueHashes struct {
	mu     sync.Mutex
	hashes []*PrometheusIssueHash
}

type PrometheusIssueNames struct {
	start common.Stamp
	end   common.Stamp
	names []string
}

type PrometheusIssues struct {
	mu              sync.Mutex
	muIssues        sync.Mutex
	options         PrometheusIssuesOptions
	scheduleOptions common.ScheduleOptions
	observability   *common.Observability
	logger          sreCommon.Logger
	issues          []PrometheusIssueNames
}

// PrometheusIssueHashes

func (ph *PrometheusIssueHashes) AddHashes(start, end common.Stamp, hashes []common.Hash) {
	ph.mu.Lock()
	defer ph.mu.Unlock()

	for _, h := range hashes {
		ph.hashes = append(ph.hashes, &PrometheusIssueHash{
			start: start,
			end:   end,
			hash:  h,
		})
	}
}

func (ph *PrometheusIssueHashes) Reduce(hashes []common.Hash, names *common.Names, limits common.DataSourceDataLimits) common.DataSourceIssues {
	return ph
}

func (ph *PrometheusIssueHashes) Exists(hash common.Hash, stamp common.Stamp) bool {
	ph.mu.Lock()
	defer ph.mu.Unlock()

	for _, issue := range ph.hashes {

		if (stamp == issue.start || stamp > issue.start) &&
			(stamp == issue.end || stamp < issue.end) &&
			(issue.hash == hash) {
			return true
		}
	}
	return false
}

func NewPrometheusIssueHashes() *PrometheusIssueHashes {
	return &PrometheusIssueHashes{
		//
	}
}

// PrometheusIssues

func (pi *PrometheusIssues) Name() string {
	return pi.options.Name
}

func (pi *PrometheusIssues) ScheduleOptions() common.ScheduleOptions {
	return pi.scheduleOptions
}

func (pi *PrometheusIssues) loadDataOpts(opts toolsVendors.PrometheusOptions) (*common.PrometheusResponseData, error) {

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

func (pi *PrometheusIssues) Reduce(filter []common.Hash, names *common.Names, limits common.DataSourceDataLimits) common.DataSourceIssues {

	pi.muIssues.Lock()
	defer pi.muIssues.Unlock()

	start := limits.First()
	end := limits.Last()

	r := NewPrometheusIssueHashes()
	for _, issue := range pi.issues {

		if (issue.start > end) || (issue.end < start) {
			continue
		}

		hashes := []common.Hash{}
		for _, n := range issue.names {
			hash := names.FindByName(n)
			if hash == common.Hash(0) {
				continue
			}
			hashes = append(hashes, hash)
		}
		hashes = common.Unique(hashes)
		if len(hashes) == 0 {
			continue
		}
		r.AddHashes(issue.start, issue.end, hashes)
	}
	return r
}

func (pi *PrometheusIssues) Exists(hash common.Hash, stamp common.Stamp) bool {
	return false
}

func (pi *PrometheusIssues) gatherIssues() ([]PrometheusIssueNames, error) {

	r := []PrometheusIssueNames{}

	names := common.RemoveEmptyStrings(strings.Split(pi.options.Names, ","))
	if len(names) == 0 {
		return r, nil
	}

	promData, err := pi.loadDataOpts(pi.options.Prometheus)
	if err != nil {
		return nil, err
	}

	var re *regexp.Regexp

	if !utils.IsEmpty(pi.options.Pattern) {
		re, err = regexp.Compile(pi.options.Pattern)
		if err != nil {
			return r, err
		}
	}

	for _, dr := range promData.Result {

		start := dr.Labels[pi.options.StartName]
		if utils.IsEmpty(start) {
			continue
		}
		startTime, err := time.Parse(pi.options.TimeFormat, start)
		if err != nil {
			continue
		}
		startTime = startTime.UTC()

		end := dr.Labels[pi.options.EndName]
		if utils.IsEmpty(end) {
			continue
		}
		endTime, err := time.Parse(pi.options.TimeFormat, end)
		if err != nil {
			continue
		}
		endTime = endTime.UTC()

		startStamp := common.TimeToStamp(startTime)
		endStamp := common.TimeToStamp(endTime)

		values := []string{}
		for _, n := range names {

			v := strings.TrimSpace(dr.Labels[n])
			if utils.IsEmpty(v) {
				continue
			}
			if re != nil {
				arr := re.FindAllString(v, -1)
				values = append(values, arr...)
			} else {
				values = append(values, v)
			}

		}
		values = common.Unique(values)
		if len(values) == 0 {
			continue
		}
		r = append(r, PrometheusIssueNames{
			start: startStamp,
			end:   endStamp,
			names: values,
		})
	}
	return r, nil
}

func (pi *PrometheusIssues) Load() {

	when := time.Now()
	pi.logger.Info("Gathering issues...")
	issues, err := pi.gatherIssues()
	if err != nil {
		pi.logger.Error("Gathering issues finished with error: %s", err)
		return
	}
	pi.issues = issues
	pi.logger.Info("Gathering issues found %d finished in %s", len(issues), time.Since(when))

}

func (pi *PrometheusIssues) Start(wg *sync.WaitGroup) {

	if !pi.mu.TryLock() {
		return
	}
	defer pi.mu.Unlock()

	wg.Add(1)
	defer wg.Done()

	pi.Load()
}

func NewPrometheusIssues(options PrometheusIssuesOptions, observability *common.Observability) *PrometheusIssues {

	return &PrometheusIssues{
		options: options,
		scheduleOptions: common.ScheduleOptions{
			Schedule:    options.Schedule,
			Immediately: !options.Preload,
		},
		observability: observability,
		logger:        observability.Logs(),
	}
}
