package lambdapipeline

import (
	"fmt"
	"time"
)

type CronScheduler struct {
	Runs chan *Run

	//note: if the errors placed on this channel are not read fast enough
	//  some error messages could become lost
	Errors chan error

	lastJobTime      time.Time
	lookAheadTime    time.Duration
	backgroundTicker *time.Ticker
	jobs             []*Job
}

func NewCronScheduler(startTime time.Time, lookAhead time.Duration) *CronScheduler {
	c := &CronScheduler{
		lastJobTime:      startTime,
		lookAheadTime:    lookAhead,
		backgroundTicker: time.NewTicker(time.Second),
		Runs:             make(chan *Run, 100),
		Errors:           make(chan error, 100),
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

func (c *CronScheduler) AddJob(j *Job) {
	c.jobs = append(c.jobs, j)
}

func (c *CronScheduler) writeErr(e error) {
	//attempts to write to channel
	//if channel is blocked or nil, the error will be lost
	select {
	case c.Errors <- e:
	default:
	}
}

func (c *CronScheduler) addNextJobsToChan() {
	fmt.Println("running job check")
	endTime := time.Now().Add(c.lookAheadTime)
	for _, j := range c.jobs {
		fmt.Printf("running job check for %s\n", j.ID)
		t := j.CronSchedule.Next(c.lastJobTime)
		//get all the runs for this job in the time range
		for !t.IsZero() && (t.Before(endTime) || t.Equal(endTime)) {
			fmt.Printf("job within time: %s\n ", t)
			r, err := j.MakeRun(JobContext{
				Attempt:            0,
				ScheduledStartTime: t,
				PreviousOutput:     nil,
			})
			if err != nil {
				c.writeErr(err)
				continue
			}
			c.Runs <- r
			t = j.CronSchedule.Next(t)
		}
	}

	c.lastJobTime = endTime
}
