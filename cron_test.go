package pipeline

import (
	"testing"
	"time"
)

func TestGetJobsForNextHour(t *testing.T) {
	c := NewCronScheduler(time.Now(), time.Hour)
	c.AddJob(&Job{
		ID:           123,
		CronSchedule: CronSchedule("* * * * *"),
	})
	c.Start()
	time.AfterFunc(time.Second*2, func() {
		close(c.Runs)
	})
	cnt := countRuns(c.Runs)
	if cnt != 60 {
		t.Errorf("expected 60 runs, got %d", cnt)
	}
	close(c.Errors)
	select {
	case err, ok := <-c.Errors:
		if ok {
			t.Errorf("unexpected err: %s", err)
		}
	default:
	}
}

func countRuns(c chan *Run) int {
	total := 0
	for range c {
		total++
	}
	return total
}
