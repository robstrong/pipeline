package pipeline

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"reflect"
	"testing"
)

const testDBPath = "./test.db"

type testRepo struct {
	*SQLiteRepo
	t      *testing.T
	DBPath string
}

func NewTestRepo(t *testing.T) *testRepo {
	db, err := sql.Open("sqlite3", testDBPath)
	if err != nil {
		t.Fatal(err)
	}
	r := NewSQLiteRepo(db)
	err = r.MigrateDB()
	if err != nil {
		t.Fatal(err)
	}
	return &testRepo{
		SQLiteRepo: r,
		t:          t,
		DBPath:     testDBPath,
	}
}

func (r *testRepo) Close() {
	err := os.Remove(r.DBPath)
	if err != nil {
		r.t.Logf("err removing db: ", err)
	}
}

func TestSQLiteCreateJob(t *testing.T) {
	r := NewTestRepo(t)
	defer r.Close()

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

func TestSQLiteUpdateJob(t *testing.T) {
	tests := []struct {
		name      string
		createJob *CreateJobInput
		updateJob *UpdateJobInput
		expected  *Job
	}{
		{
			name: "test all fields",
			createJob: &CreateJobInput{
				Name: "test1",
			},
			updateJob: &UpdateJobInput{
				Name: StrPtr("test2"),
			},
			expected: &Job{
				Name: "test2",
			},
		},
	}

	r := NewTestRepo(t)
	defer r.Close()
	defer func() {
	}()
	for _, test := range tests {
		id, err := r.CreateJob(test.createJob)
		if err != nil {
			t.Fatal(err)
		}
		upd := test.updateJob
		upd.JobID = id
		err = r.UpdateJob(upd)
		if err != nil {
			t.Fatal(err)
		}
		job, err := r.GetJobs(&GetJobsInput{JobIDs: []JobID{id}})
		if err != nil {
			t.Fatal(err)
		}
		test.expected.ID = upd.JobID
		if !reflect.DeepEqual(test.expected, job[0]) {
			t.Errorf("%s: expected %s\ngot: %s", test.name, test.expected, job[0])
		}
	}
}
