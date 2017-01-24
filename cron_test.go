package lambdapipeline

import (
	"testing"
	"time"

	"github.com/gorhill/cronexpr"
)

func TestGetJobsForNextHour(t *testing.T) {
	c := NewCronScheduler(time.Now(), time.Hour)
	c.AddJob(&Job{
		ID:           "test job",
		CronSchedule: cronexpr.MustParse("* * * * *"),
	})
	c.Start()
	time.AfterFunc(time.Second*10, func() {
		close(c.Runs)
	})
	res := countRuns(c.Runs)
	if res != 60 {
		t.Errorf("expected 60 runs, got %d", res)
	}
}

func countRuns(c chan *Run) int {
	total := 0
	for range c {
		total++
	}
	return total
}
