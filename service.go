package lambdapipeline

import (
	"log"
	"time"
)

type Service struct {
	incomingRuns  chan *Run
	finishedRuns  chan *RunResult
	runRepository RunRepository
	log           *log.Logger
	runProcessor  RunProcessor
	cron          *CronScheduler
}

type RunRepository interface {
	//Should return pending runs that are scheduled for <= now
	GetPendingRuns() ([]*Run, error)
	//Should update the run with id RunResult.RunID with the results
	SaveRunResult(*RunResult) error
}

type RunProcessor interface {
	Process(*Run) (*RunResult, error)
}

type CronScheduler struct {
	lastJobTime   time.Time
	lookAheadTime time.Duration
	jobs          []*Job
	upcomingRuns  chan *Run
	errChan       chan error
}

func NewCronScheduler(startTime time.Time, lookAhead time.Duration) *CronScheduler {
	c := &CronScheduler{
		lastJobTime:   startTime,
		lookAheadTime: lookAhead,
		upcomingRuns:  make(chan *Run, 10),
		errChan:       make(chan error, 10),
	}
	return c
}

func (c *CronScheduler) Stop() bool {
	if c.backgroundTimer != nil {
		return c.backgroundTimer.Stop()
	}
	return false
}

func (c *CronScheduler) Start() {
	c.Stop()
	c.backgroundTimer = time.NewTimer(time.Minute)
	go func() {
		for t := range c.backgroundTimer.C {
			c.addNextJobsToChan()
		}
	}()
}

func (c *CronScheduler) AddJob(j *Job) {
	c.jobs = append(c.jobs, j)
}

func (c *CronScheduler) addNextJobsToChan() {
	endTime := time.Now().Add(c.lookAheadTime)
	for _, j := range c.jobs {
		t := j.CronSchedule.Next(lastJobTime)
		//get all the runs for this job in the time range
		for !t.IsZero() && (t.Before(endTime) || t.Equal(endTime)) {
			r, err := j.MakeRun(JobContext{
				Attempt:            0,
				ScheduledStartTime: t,
				PreviousOutput:     nil,
			})
			if err != nil {
				c.errChan <- err
				continue
			}
			c.upcomingRuns <- r
			t = j.CronSchedule.Next(t)
		}
	}

	c.lastJobTime = endTime
	return nil
}

// blocking
func (s *Service) ListenAndServe() error {
	go s.startBackgroundWorker()
}

func (s *Service) startBackgroundWorker() {
	timer := time.NewTimer(time.Second) // can probably change this to run on the min?
	defer timer.Stop()
	for {
		select {
		case new = timer.C:
			//check for new runs to start
			rs, err := s.runRepository.GetPendingRuns()
			if err != nil {
				h.log.Printf("err getting pending runs: %s", err)
				continue
			}
			for _, r := range rs {
				h.incomingRuns <- r
			}

		case r <- h.incomingRuns:
			//process runs that should be run
			res, err := s.runProcessor.Process(r)
			if err != nil {
				h.log.Printf("err processing run: %s", err)
				continue
			}
			h.finishedRuns <- res

		case res <- h.finishedRuns:
			//save the result of runs
			err := s.runRepository.SaveRunResult(res)
			if err != nil {
				h.log.Printf("err saving run result: %s", err)
				continue
			}
			if !res.Success {
				//rerun if necessary

			}
		}
	}
}
