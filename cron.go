package pipeline

import "time"
import "github.com/gorhill/cronexpr"

type CronScheduler struct {
	Runs chan *Run

	//note: if the errors placed on this channel are not read fast enough
	//  some error messages could become lost
	Errors chan error

	lastJobTime      time.Time
	lookAheadTime    time.Duration
	backgroundTicker *time.Ticker
	jobs             map[JobID]*Job
	crons            map[JobID]*cronexpr.Expression
}

func NewCronScheduler(startTime time.Time, lookAhead time.Duration) *CronScheduler {
	c := &CronScheduler{
		lastJobTime:   startTime,
		lookAheadTime: lookAhead,
		Runs:          make(chan *Run, 100),
		Errors:        make(chan error, 100),
		jobs:          map[JobID]*Job{},
		crons:         map[JobID]*cronexpr.Expression{},
	}
	return c
}

func (c *CronScheduler) Stop() {
	if c.backgroundTicker != nil {
		c.backgroundTicker.Stop()
	}
}

func (c *CronScheduler) Start() {
	c.Stop()
	c.backgroundTicker = time.NewTicker(time.Second)
	go func() {
		for range c.backgroundTicker.C {
			c.addNextJobsToChan()
		}
	}()
}

const ErrCronSchedulerJobAlreadyAdded = Err("job already added to cron")

func (c *CronScheduler) AddJob(j *Job) error {
	expr, err := j.CronSchedule.Expression()
	if err != nil {
		return err
	}
	if _, ok := c.jobs[j.ID]; ok {
		return ErrCronSchedulerJobAlreadyAdded
	}
	c.jobs[j.ID] = j
	c.crons[j.ID] = expr
	return nil
}

func (c *CronScheduler) writeErr(e error) {
	if e == nil {
		return
	}
	//attempts to write to channel
	//if channel is blocked or nil, the error will be lost
	select {
	case c.Errors <- e:
	default:
	}
}

func (c *CronScheduler) addNextJobsToChan() {
	endTime := time.Now().Add(c.lookAheadTime)
	for jID, j := range c.jobs {
		t := c.crons[jID].Next(c.lastJobTime)
		//get all the runs for this job in the time range
		for !t.IsZero() && (t.Before(endTime) || t.Equal(endTime)) {
			r, err := j.MakeRun(JobContext{
				Attempt:            0,
				ScheduledStartTime: t,
				PreviousOutput:     []byte("{}"),
			})
			if err != nil {
				c.writeErr(err)
				continue
			}
			c.Runs <- r
			t = c.crons[jID].Next(t)
		}
	}

	c.lastJobTime = endTime
}
