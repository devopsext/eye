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

type TrainServer struct {
	models *common.Models
	logger sreCommon.Logger
	meter  sreCommon.Meter
}

const (
	TrainOnce = "once"
)

func (t *TrainServer) runOnce(wg *sync.WaitGroup, model common.Model) {

	wg.Add(1)
	go func() {
		defer wg.Done()
		model.Train(wg)
	}()
}

func (t *TrainServer) Start(wg *sync.WaitGroup) {

	t.logger.Info("Start train server...")

	opts := []gocron.SchedulerOption{}
	opts = append(opts, gocron.WithLocation(time.UTC))

	scheduler, err := gocron.NewScheduler(opts...)
	if err != nil {
		t.logger.Panic(err)
	}

	for _, m := range t.models.Items() {

		if utils.IsEmpty(m) {
			continue
		}
		name := m.Name()
		schedule := m.Schedule()

		if utils.IsEmpty(schedule) {
			t.logger.Debug("Train server skipped %s model", name)
			continue
		}

		if schedule == TrainOnce {
			t.runOnce(wg, m)
			continue
		}

		var def gocron.JobDefinition

		if len(strings.Split(schedule, " ")) == 1 {

			d, err := time.ParseDuration(schedule)
			if err != nil {
				t.logger.Error("Train server cannot schedule %s model %s, error: %s", schedule, name, err)
				continue
			}
			def = gocron.DurationJob(d)

		} else {
			def = gocron.CronJob(schedule, false)
		}

		task := gocron.NewTask(m.Train, wg)

		opts := []gocron.JobOption{}
		opts = append(opts, gocron.WithName(name))
		opts = append(opts, gocron.JobOption(gocron.WithStartImmediately()))
		opts = append(opts, gocron.WithSingletonMode(gocron.LimitModeReschedule))

		job, err := scheduler.NewJob(def, task, opts...)
		if err != nil {
			t.logger.Error("Train server cannot schedule %s model %s, error: %s", schedule, name, err)
			continue
		}
		t.logger.Info("Train server scheduled model %s with id %s", name, job.ID())
	}

	scheduler.Start()
	t.logger.Info("Train server started.")
}

func NewTrainServer(models *common.Models, observability *common.Observability) *TrainServer {

	meter := observability.Metrics()

	return &TrainServer{
		models: models,
		logger: observability.Logs(),
		meter:  meter,
	}
}
