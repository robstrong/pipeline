package pipeline

import (
	"database/sql"
	"os"
	"reflect"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const testDBPath = "./test.db"

type testRepo struct {
	*SQLiteRepo
	t      *testing.T
	DBPath string
}

func newTestRepo(t *testing.T) *testRepo {
	_ = os.Remove(testDBPath)
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
	r := newTestRepo(t)
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
				ProcessorConfig: ProcessorConfig{
					Type:   "processortype",
					Config: map[string]string{"user": "jdoe"},
				},
				RetryerConfig: RetryerConfig{
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
				ProcessorConfig: ProcessorConfig{
					Type:   "processortype",
					Config: map[string]string{"user": "jdoe"},
				},
				RetryerConfig: RetryerConfig{
					Type:   "retryertype",
					Config: map[string]string{"retries": "2"},
				},
				InputPayloadTemplate: []byte("payload"),
				Triggers: TriggerEvents{
					CronSchedule: CronSchedule(""),
					JobSuccess:   []JobID{},
					JobFailure:   []JobID{},
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
				Processor: ProcessorConfig{
					Type:   "ptype",
					Config: map[string]string{"p": "config"},
				},
				InputPayloadTemplate: []byte("inputtempl"),
				Retryer: RetryerConfig{
					Type:   "rconfig",
					Config: map[string]string{"r": "config"},
				},
				Triggers: &TriggerEventsInput{
					CronSchedule: NewCronSchedule("* * *"),
					JobSuccess:   []JobID{JobID(3)},
					JobFailure:   []JobID{JobID(2)},
				},
			},
			updateJob: &UpdateJobInput{
				Name: StringPtr("test2"),
				Processor: &ProcessorConfig{
					Type:   "ptype2",
					Config: map[string]string{"p": "config2"},
				},
				InputPayloadTemplate: []byte("inputtempl2"),
				Retryer: &RetryerConfig{
					Type:   "rconfig2",
					Config: map[string]string{"r": "config2"},
				},
				Triggers: &TriggerEventsInput{
					CronSchedule: NewCronSchedule("* * *2"),
					JobSuccess:   []JobID{JobID(4)},
					JobFailure:   []JobID{JobID(5)},
				},
			},
			expected: &Job{
				Name: "test2",
				ProcessorConfig: ProcessorConfig{
					Type:   "ptype2",
					Config: map[string]string{"p": "config2"},
				},
				InputPayloadTemplate: []byte("inputtempl2"),
				RetryerConfig: RetryerConfig{
					Type:   "rconfig2",
					Config: map[string]string{"r": "config2"},
				},
				Triggers: TriggerEvents{
					CronSchedule: CronSchedule("* * *2"),
					JobSuccess:   []JobID{JobID(4)},
					JobFailure:   []JobID{JobID(5)},
				},
			},
		},
		{
			name: "test values not changed",
			createJob: &CreateJobInput{
				Name: "test1",
				Processor: ProcessorConfig{
					Type:   "ptype",
					Config: map[string]string{"p": "config"},
				},
				InputPayloadTemplate: []byte("inputtempl"),
				Retryer: RetryerConfig{
					Type:   "rconfig",
					Config: map[string]string{"r": "config"},
				},
				Triggers: &TriggerEventsInput{
					CronSchedule: NewCronSchedule("* * *"),
					JobSuccess:   []JobID{JobID(3)},
					JobFailure:   []JobID{JobID(2)},
				},
			},
			updateJob: &UpdateJobInput{
				Name:                 nil,
				Processor:            nil,
				InputPayloadTemplate: nil,
				Retryer:              nil,
				Triggers: &TriggerEventsInput{
					CronSchedule: nil,
					JobSuccess:   nil,
					JobFailure:   nil,
				},
			},
			expected: &Job{
				Name: "test1",
				ProcessorConfig: ProcessorConfig{
					Type:   "ptype",
					Config: map[string]string{"p": "config"},
				},
				InputPayloadTemplate: []byte("inputtempl"),
				RetryerConfig: RetryerConfig{
					Type:   "rconfig",
					Config: map[string]string{"r": "config"},
				},
				Triggers: TriggerEvents{
					CronSchedule: CronSchedule("* * *"),
					JobSuccess:   []JobID{JobID(3)},
					JobFailure:   []JobID{JobID(2)},
				},
			},
		},
		{
			name: "unset triggers",
			createJob: &CreateJobInput{
				Name: "test1",
				Processor: ProcessorConfig{
					Type:   "ptype",
					Config: map[string]string{"p": "config"},
				},
				InputPayloadTemplate: []byte("inputtempl"),
				Retryer: RetryerConfig{
					Type:   "rconfig",
					Config: map[string]string{"r": "config"},
				},
				Triggers: &TriggerEventsInput{
					CronSchedule: NewCronSchedule("* * *"),
					JobSuccess:   []JobID{JobID(3)},
					JobFailure:   []JobID{JobID(2)},
				},
			},
			updateJob: &UpdateJobInput{
				Name:                 nil,
				Processor:            nil,
				InputPayloadTemplate: nil,
				Retryer:              nil,
				Triggers: &TriggerEventsInput{
					CronSchedule: NewCronSchedule(""),
					JobSuccess:   []JobID{},
					JobFailure:   []JobID{},
				},
			},
			expected: &Job{
				Name: "test1",
				ProcessorConfig: ProcessorConfig{
					Type:   "ptype",
					Config: map[string]string{"p": "config"},
				},
				InputPayloadTemplate: []byte("inputtempl"),
				RetryerConfig: RetryerConfig{
					Type:   "rconfig",
					Config: map[string]string{"r": "config"},
				},
				Triggers: TriggerEvents{
					CronSchedule: CronSchedule(""),
					JobSuccess:   []JobID{},
					JobFailure:   []JobID{},
				},
			},
		},
	}

	r := newTestRepo(t)
	defer r.Close()
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
		jobs, err := r.GetJobs(&GetJobsInput{JobIDs: []JobID{id}})
		if err != nil {
			t.Fatal(err)
		}
		if len(jobs) != 1 {
			t.Fatalf("expected 1 job, got %d", len(jobs))
		}
		test.expected.ID = upd.JobID
		if !reflect.DeepEqual(test.expected, jobs[0]) {
			t.Errorf("%s: expected %s\ngot: %s", test.name, test.expected, jobs[0])
		}
	}
}

func TestParseGroupedJobIDs(t *testing.T) {
	tests := []struct {
		name    string
		input   *string
		want    []JobID
		wantErr bool
	}{
		{
			name:  "empty string",
			input: StringPtr(""),
			want:  []JobID{},
		},
		{
			name:  "nil string",
			input: nil,
			want:  []JobID{},
		},
		{
			name:  "single id",
			input: StringPtr("1"),
			want:  []JobID{1},
		},
		{
			name:  "two ids",
			input: StringPtr("1,2"),
			want:  []JobID{1, 2},
		},
		{
			name:    "invalid ids",
			input:   StringPtr("1,f"),
			wantErr: true,
		},
		{
			name:    "missing ids",
			input:   StringPtr("1,,2"),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		got, err := parseGroupedJobIDs(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. parseGroupedJobIDs() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. parseGroupedJobIDs() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestSQLiteCreateRun(t *testing.T) {
	r := newTestRepo(t)
	defer r.Close()

	tests := []struct {
		name     string
		input    *CreateRunInput
		expected *Run
	}{
		{
			name: "set all fields",
			input: &CreateRunInput{
				JobID: JobID(1),
				Processor: ProcessorConfig{
					Type:   "ptype",
					Config: map[string]string{"p": "config"},
				},
				Status:             RunStatusPending,
				ScheduledStartTime: time.Date(2017, time.Month(2), 24, 9, 37, 2, 1, time.UTC),
				Attempt:            1,
				Input:              []byte("input"),
			},
			expected: &Run{
				JobID: JobID(1),
				ProcessorConfig: ProcessorConfig{
					Type:   "ptype",
					Config: map[string]string{"p": "config"},
				},
				Status:             RunStatusPending,
				StatusDetail:       "",
				ScheduledStartTime: time.Date(2017, time.Month(2), 24, 9, 37, 2, 1, time.UTC),
				StartTime:          nil,
				EndTime:            nil,
				Attempt:            1,
				Success:            false,
				Input:              []byte("input"),
				Output:             nil,
				Log:                nil,
			},
		},
	}

	for _, test := range tests {
		rID, err := r.CreateRun(test.input)
		if err != nil {
			t.Fatal(err)
		}
		test.expected.RunID = rID

		runs, err := r.GetRuns(&GetRunsInput{RunID: &rID})
		if err != nil {
			t.Fatal(err)
		}

		if len(runs) != 1 {
			t.Errorf("%s: expected 1 run, got %d", test.name, len(runs))
			t.FailNow()
		}
		run := runs[0]
		if !reflect.DeepEqual(test.expected, run) {
			t.Errorf("%s: expected %+v, got %+v", test.name, test.expected, run)
		}
	}
}

func TestSQLiteGetRuns(t *testing.T) {
	tests := []struct {
		name       string
		createRuns []*CreateRunInput
		query      *GetRunsInput
		expected   []*Run
	}{
		{
			name: "runs by job id",
			createRuns: []*CreateRunInput{
				&CreateRunInput{
					JobID:  JobID(1),
					Input:  []byte("r1"),
					Status: RunStatusPending,
				},
				&CreateRunInput{
					JobID:  JobID(1),
					Input:  []byte("r2"),
					Status: RunStatusPending,
				},
				&CreateRunInput{
					JobID:  JobID(2),
					Input:  []byte("r3"),
					Status: RunStatusPending,
				},
			},
			query: &GetRunsInput{
				JobID: JobIDPtr(1),
			},
			expected: []*Run{
				&Run{
					RunID:  RunID(1),
					JobID:  JobID(1),
					Input:  []byte("r1"),
					Status: RunStatusPending,
				},
				&Run{
					RunID:  RunID(2),
					JobID:  JobID(1),
					Input:  []byte("r2"),
					Status: RunStatusPending,
				},
			},
		},
		{
			name: "run by run id",
			createRuns: []*CreateRunInput{
				&CreateRunInput{
					JobID:  JobID(1),
					Input:  []byte("r1"),
					Status: RunStatusPending,
				},
				&CreateRunInput{
					JobID:  JobID(1),
					Input:  []byte("r2"),
					Status: RunStatusPending,
				},
				&CreateRunInput{
					JobID:  JobID(2),
					Input:  []byte("r3"),
					Status: RunStatusPending,
				},
			},
			query: &GetRunsInput{
				RunID: RunIDPtr(2),
			},
			expected: []*Run{
				&Run{
					RunID:  RunID(2),
					JobID:  JobID(1),
					Input:  []byte("r2"),
					Status: RunStatusPending,
				},
			},
		},
		{
			name: "by status",
			createRuns: []*CreateRunInput{
				&CreateRunInput{
					JobID:  JobID(1),
					Input:  []byte("r1"),
					Status: RunStatusPending,
				},
				&CreateRunInput{
					JobID:  JobID(1),
					Input:  []byte("r2"),
					Status: RunStatusComplete,
				},
				&CreateRunInput{
					JobID:  JobID(2),
					Input:  []byte("r3"),
					Status: RunStatusRunning,
				},
			},
			query: &GetRunsInput{
				Status: RunStatusPtr(RunStatusComplete),
			},
			expected: []*Run{
				&Run{
					RunID:  RunID(2),
					JobID:  JobID(1),
					Input:  []byte("r2"),
					Status: RunStatusComplete,
				},
			},
		},
	}
	for _, test := range tests {
		r := newTestRepo(t)

		for _, c := range test.createRuns {
			_, err := r.CreateRun(c)
			if err != nil {
				t.Fatal(err)
			}
		}

		runs, err := r.GetRuns(test.query)
		if err != nil {
			t.Fatal(err)
		}

		if len(runs) != len(test.expected) {
			t.Errorf("%s: expected %d run, got %d", test.name, len(test.expected), len(runs))
			t.FailNow()
		}
		for i, run := range runs {
			if !reflect.DeepEqual(test.expected[i], run) {
				t.Errorf("%s: expected %+v, got %+v", test.name, test.expected[i], run)
			}
		}
		r.Close()
	}
}
