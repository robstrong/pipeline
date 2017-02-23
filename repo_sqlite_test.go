package pipeline

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"reflect"
	"testing"
)

const testDBPath = "./test.db"

func getRepo(t *testing.T) *SQLiteRepo {
	db, err := sql.Open("sqlite3", testDBPath)
	if err != nil {
		t.Fatal(err)
	}
	r := NewSQLiteRepo(db)
	err = r.MigrateDB()
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestSQLiteJob(t *testing.T) {
	r := getRepo(t)
	defer os.Remove(testDBPath)
	tests := []struct {
		name     string
		input    *CreateJobInput
		expected *Job
	}{
		{
			name: "use all fields",
			input: &CreateJobInput{
				Name: "test1",
				Processor: ProcessorConfig{
					Type:   "processortype",
					Config: map[string]string{"user": "jdoe"},
				},
				Retryer: RetryerConfig{
					Type:   "retryertype",
					Config: map[string]string{"retries": "2"},
				},
				InputPayloadTemplate: []byte("payload"),
				CronSchedule:         NewCronSchedule("* * * * *"),
			},
			expected: &Job{
				ID:   1,
				Name: "test1",
				Processor: ProcessorConfig{
					Type:   "processortype",
					Config: map[string]string{"user": "jdoe"},
				},
				Retryer: RetryerConfig{
					Type:   "retryertype",
					Config: map[string]string{"retries": "2"},
				},
				InputPayloadTemplate: []byte("payload"),
				Triggers: TriggerEvents{
					CronSchedule: CronSchedule("* * * * *"),
				},
			},
		},
		{
			name: "nil cron",
			input: &CreateJobInput{
				Name: "test1",
				Processor: ProcessorConfig{
					Type:   "processortype",
					Config: map[string]string{"user": "jdoe"},
				},
				Retryer: RetryerConfig{
					Type:   "retryertype",
					Config: map[string]string{"retries": "2"},
				},
				InputPayloadTemplate: []byte("payload"),
				Triggers: TriggerEvents{
					CronSchedule: nil,
				},
			},
			expected: &Job{
				ID:   2,
				Name: "test1",
				Processor: ProcessorConfig{
					Type:   "processortype",
					Config: map[string]string{"user": "jdoe"},
				},
				Retryer: RetryerConfig{
					Type:   "retryertype",
					Config: map[string]string{"retries": "2"},
				},
				InputPayloadTemplate: []byte("payload"),
				CronSchedule:         CronSchedule(""),
			},
		},
	}

	for _, test := range tests {
		jID, err := r.CreateJob(test.input)
		if err != nil {
			t.Fatal(err)
		}

		jobs, err := r.GetJobs(&GetJobsInput{JobIDs: []JobID{jID}})
		if err != nil {
			t.Fatal(err)
		}

		if len(jobs) != 1 {
			t.Errorf("expected 1 job, got %d", len(jobs))
			t.FailNow()
		}
		job := jobs[0]
		if !reflect.DeepEqual(test.expected, job) {
			t.Errorf("expected %+v, got %+v", test.expected, job)
		}
	}
}
