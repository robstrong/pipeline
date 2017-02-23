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
	defer func() {
		err := os.Remove(testDBPath)
		if err != nil {
			t.Logf("err removing db: ", err)
		}
	}()

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
				Triggers: &TriggerEventsInput{
					CronSchedule: NewCronSchedule("* * * * *"),
					JobSuccess:   []JobID{JobID(3)},
					JobFailure:   []JobID{JobID(2)},
				},
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
					JobSuccess:   []JobID{JobID(3)},
					JobFailure:   []JobID{JobID(2)},
				},
			},
		},
		{
			name: "nil triggers",
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
				Triggers: &TriggerEventsInput{
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
				Triggers: TriggerEvents{
					CronSchedule: CronSchedule(""),
				},
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
			t.Errorf("%s: expected 1 job, got %d", test.name, len(jobs))
			t.FailNow()
		}
		job := jobs[0]
		if !reflect.DeepEqual(test.expected, job) {
			t.Errorf("%s: expected %+v, got %+v", test.name, test.expected, job)
		}
	}
}
