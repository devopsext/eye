package server

import (
	"strings"
	"sync"
	"time"

	"github.com/devopsext/eye/common"
	sreCommon "github.com/devopsext/sre/common"
	"github.com/devopsext/utils"
	"github.com/go-co-op/gocron/v2"
)

type Scheduler struct {
	schedules *common.Schedules
	logger    sreCommon.Logger
	meter     sreCommon.Meter
}

const (
	ScheduleOnce = "once"
)

func (s *Scheduler) startOnce(wg *sync.WaitGroup, schedule common.Schedule) {

	wg.Add(1)
	go func() {
		defer wg.Done()
		schedule.Start(wg)
	}()
}

func (sr *Scheduler) Start(wg *sync.WaitGroup) {

	sr.logger.Info("Start train server...")

	opts := []gocron.SchedulerOption{}
	opts = append(opts, gocron.WithLocation(time.UTC))

	scheduler, err := gocron.NewScheduler(opts...)
	if err != nil {
		sr.logger.Panic(err)
	}

	for _, s := range sr.schedules.Items() {

		if utils.IsEmpty(s) {
			continue
		}
		name := s.Name()
		schedule := s.Schedule()

		if utils.IsEmpty(schedule) {
			sr.logger.Debug("Scheduler skipped %s schedule", name)
			continue
		}

		if schedule == ScheduleOnce {
			sr.startOnce(wg, s)
			continue
		}

		var def gocron.JobDefinition

		if len(strings.Split(schedule, " ")) == 1 {

			d, err := time.ParseDuration(schedule)
			if err != nil {
				sr.logger.Error("Scheduler cannot schedule %s model %s, error: %s", schedule, name, err)
				continue
			}
			def = gocron.DurationJob(d)

		} else {
			def = gocron.CronJob(schedule, false)
		}

		task := gocron.NewTask(s.Start, wg)

		opts := []gocron.JobOption{}
		opts = append(opts, gocron.WithName(name))
		opts = append(opts, gocron.JobOption(gocron.WithStartImmediately()))
		opts = append(opts, gocron.WithSingletonMode(gocron.LimitModeReschedule))

		job, err := scheduler.NewJob(def, task, opts...)
		if err != nil {
			sr.logger.Error("Scheduler cannot schedule %s model %s, error: %s", schedule, name, err)
			continue
		}
		sr.logger.Info("Scheduler scheduled model %s with id %s", name, job.ID())
	}

	scheduler.Start()
	sr.logger.Info("Scheduler started.")
}

func NewScheduler(schedules *common.Schedules, observability *common.Observability) *Scheduler {

	meter := observability.Metrics()

	return &Scheduler{
		schedules: schedules,
		logger:    observability.Logs(),
		meter:     meter,
	}
}
