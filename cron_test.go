package pipeline

import (
	"testing"
	"time"
)

//TODO: this is not deterministic, need to refactor
func TestGetJobsForNextHour(t *testing.T) {
	c := NewCronScheduler(time.Now(), time.Hour)
	err := c.AddJob(&Job{
		ID: 123,
		Triggers: TriggerEvents{
			CronSchedule: CronSchedule("* * * * *"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	c.Start()
	time.AfterFunc(time.Second*2, func() {
		c.Stop()
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

func TestInvalidJobs(t *testing.T) {
	c := NewCronScheduler(time.Now(), time.Hour)
	//add this job so that we can test duplicate errors
	err := c.AddJob(&Job{ID: 123, Triggers: TriggerEvents{CronSchedule: *NewCronSchedule("* * * * *")}})
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	tests := []struct {
		name        string
		job         *Job
		expectedErr error
	}{
		{
			name:        "nil job",
			job:         nil,
			expectedErr: ErrCronSchedulerInvalidJob,
		},
		{
			name:        "nil CronSchedule",
			job:         &Job{},
			expectedErr: ErrCronSchedulerInvalidCronSchedule,
		},
		{
			name: "empty cron schedule",
			job: &Job{
				Triggers: TriggerEvents{
					CronSchedule: "",
				},
			},
			expectedErr: ErrCronSchedulerInvalidCronSchedule,
		},
		{
			name: "empty cron schedule",
			job: &Job{
				ID: 123,
				Triggers: TriggerEvents{
					CronSchedule: *NewCronSchedule("* * * * *"),
				},
			},
			expectedErr: ErrCronSchedulerJobAlreadyAdded,
		},
	}

	for _, test := range tests {
		if err := c.AddJob(test.job); err != test.expectedErr {
			t.Errorf("(%s) want err: '%s', got '%s'", test.name, test.expectedErr, err)
		}
	}
}

func countRuns(c chan *Run) int {
	total := 0
	for range c {
		total++
	}
	return total
}
