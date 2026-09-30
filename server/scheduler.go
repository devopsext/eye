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
	ScheduleOnce       = "once"
	ScheduleContinuous = "continuous"
)

func (s *Scheduler) once(wg *sync.WaitGroup, schedule common.Schedule) {

	wg.Add(1)
	go func() {
		defer wg.Done()
		schedule.Start(wg)
	}()
}

func (sr *Scheduler) Start(wg *sync.WaitGroup) {

	// filter all schedules
	filtered := []common.Schedule{}
	for _, item := range sr.schedules.Items() {

		if utils.IsEmpty(item) {
			continue
		}
		name := item.Name()
		schedule := item.Schedule()

		if utils.IsEmpty(schedule) {
			sr.logger.Debug("Scheduler skipped %s schedule", name)
			continue
		}
		filtered = append(filtered, item)
	}

	if len(filtered) == 0 {
		sr.logger.Debug("Scheduler has not started")
		return
	}

	sr.logger.Info("Scheduler starting...")

	opts := []gocron.SchedulerOption{}
	opts = append(opts, gocron.WithLocation(time.UTC))

	scheduler, err := gocron.NewScheduler(opts...)
	if err != nil {
		sr.logger.Panic(err)
	}

	for _, s := range filtered {

		name := s.Name()
		schedule := s.Schedule()

		if schedule == ScheduleOnce {
			sr.once(wg, s)
			continue
		}

		var def gocron.JobDefinition
		task := gocron.NewTask(s.Start, wg)

		opts := []gocron.JobOption{}
		opts = append(opts, gocron.WithName(name))
		opts = append(opts, gocron.JobOption(gocron.WithStartImmediately()))
		limitMode := gocron.LimitModeReschedule

		if len(strings.Split(schedule, " ")) == 1 {

			if schedule == ScheduleContinuous {
				def = gocron.DurationJob(1 * time.Millisecond)
				limitMode = gocron.LimitModeWait
			} else {
				d, err := time.ParseDuration(schedule)
				if err != nil {
					sr.logger.Error("Scheduler cannot schedule %s model %s, error: %s", schedule, name, err)
					continue
				}
				def = gocron.DurationJob(d)
			}
		} else {
			def = gocron.CronJob(schedule, false)
		}

		opts = append(opts, gocron.WithSingletonMode(limitMode))

		job, err := scheduler.NewJob(def, task, opts...)
		if err != nil {
			sr.logger.Error("Scheduler cannot schedule %s model %s, error: %s", schedule, name, err)
			continue
		}
		sr.logger.Info("Scheduler scheduled %s with id %s", name, job.ID())
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
