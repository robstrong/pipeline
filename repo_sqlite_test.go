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
					JobSuccess:   JobIDs{JobID(3)},
					JobFailure:   JobIDs{JobID(2)},
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
					JobSuccess:   JobIDs{JobID(3)},
					JobFailure:   JobIDs{JobID(2)},
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
					JobSuccess:   JobIDs{},
					JobFailure:   JobIDs{},
				},
			},
		},
	}

	for _, test := range tests {
		jID, err := r.CreateJob(test.input)
		if err != nil {
			t.Fatal(err)
		}

		jobs, err := r.GetJobs(&GetJobsInput{JobIDs: JobIDs{jID}})
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
					JobSuccess:   JobIDs{JobID(3)},
					JobFailure:   JobIDs{JobID(2)},
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
					JobSuccess:   JobIDs{JobID(4)},
					JobFailure:   JobIDs{JobID(5)},
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
					JobSuccess:   JobIDs{JobID(4)},
					JobFailure:   JobIDs{JobID(5)},
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
					JobSuccess:   JobIDs{JobID(3)},
					JobFailure:   JobIDs{JobID(2)},
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
					JobSuccess:   JobIDs{JobID(3)},
					JobFailure:   JobIDs{JobID(2)},
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
					JobSuccess:   JobIDs{JobID(3)},
					JobFailure:   JobIDs{JobID(2)},
				},
			},
			updateJob: &UpdateJobInput{
				Name:                 nil,
				Processor:            nil,
				InputPayloadTemplate: nil,
				Retryer:              nil,
				Triggers: &TriggerEventsInput{
					CronSchedule: NewCronSchedule(""),
					JobSuccess:   JobIDs{},
					JobFailure:   JobIDs{},
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
					JobSuccess:   JobIDs{},
					JobFailure:   JobIDs{},
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
		jobs, err := r.GetJobs(&GetJobsInput{JobIDs: JobIDs{id}})
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
		want    JobIDs
		wantErr bool
	}{
		{
			name:  "empty string",
			input: StringPtr(""),
			want:  JobIDs{},
		},
		{
			name:  "nil string",
			input: nil,
			want:  JobIDs{},
		},
		{
			name:  "single id",
			input: StringPtr("1"),
			want:  JobIDs{1},
		},
		{
			name:  "two ids",
			input: StringPtr("1,2"),
			want:  JobIDs{1, 2},
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
				ProcessorConfig: ProcessorConfig{
					Type:   "ptype",
					Config: map[string]string{"p": "config"},
				},
				Status:             RunStatusPtr(RunStatusPending),
				StatusDetail:       StringPtr("status detail"),
				ScheduledStartTime: time.Date(2017, time.Month(2), 24, 9, 37, 2, 1, time.UTC),
				StartTime:          TimePtr(time.Date(2017, time.Month(3), 24, 9, 37, 2, 1, time.UTC)),
				EndTime:            TimePtr(time.Date(2017, time.Month(4), 24, 9, 37, 2, 1, time.UTC)),
				Success:            BoolPtr(true),
				Attempt:            IntPtr(1),
				Input:              []byte("input"),
				Output:             []byte("output"),
				Log:                []byte("log"),
			},
			expected: &Run{
				JobID: JobID(1),
				ProcessorConfig: ProcessorConfig{
					Type:   "ptype",
					Config: map[string]string{"p": "config"},
				},
				Status:             RunStatusPending,
				StatusDetail:       "status detail",
				ScheduledStartTime: time.Date(2017, time.Month(2), 24, 9, 37, 2, 1, time.UTC),
				StartTime:          TimePtr(time.Date(2017, time.Month(3), 24, 9, 37, 2, 1, time.UTC)),
				EndTime:            TimePtr(time.Date(2017, time.Month(4), 24, 9, 37, 2, 1, time.UTC)),
				Attempt:            1,
				Success:            true,
				Input:              []byte("input"),
				Output:             []byte("output"),
				Log:                []byte("log"),
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
					Status: RunStatusPtr(RunStatusPending),
				},
				&CreateRunInput{
					JobID:  JobID(1),
					Input:  []byte("r2"),
					Status: RunStatusPtr(RunStatusPending),
				},
				&CreateRunInput{
					JobID:  JobID(2),
					Input:  []byte("r3"),
					Status: RunStatusPtr(RunStatusPending),
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
					Status: RunStatusPtr(RunStatusPending),
				},
				&CreateRunInput{
					JobID:  JobID(1),
					Input:  []byte("r2"),
					Status: RunStatusPtr(RunStatusPending),
				},
				&CreateRunInput{
					JobID:  JobID(2),
					Input:  []byte("r3"),
					Status: RunStatusPtr(RunStatusPending),
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
					Status: RunStatusPtr(RunStatusPending),
				},
				&CreateRunInput{
					JobID:  JobID(1),
					Input:  []byte("r2"),
					Status: RunStatusPtr(RunStatusComplete),
				},
				&CreateRunInput{
					JobID:  JobID(2),
					Input:  []byte("r3"),
					Status: RunStatusPtr(RunStatusPending),
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
		{
			name: "by start time before",
			createRuns: []*CreateRunInput{
				&CreateRunInput{
					JobID:     JobID(1),
					Input:     []byte("r1"),
					StartTime: TimePtr(time.Date(2017, time.Month(2), 28, 12, 13, 14, 15, time.UTC)),
				},
				&CreateRunInput{
					JobID:     JobID(1),
					Input:     []byte("r2"),
					StartTime: TimePtr(time.Date(2017, time.Month(2), 28, 12, 13, 14, 17, time.UTC)),
				},
				&CreateRunInput{
					JobID:     JobID(2),
					Input:     []byte("r3"),
					StartTime: TimePtr(time.Date(2017, time.Month(2), 28, 12, 13, 14, 19, time.UTC)),
				},
			},
			query: &GetRunsInput{
				StartTimeBefore: TimePtr(time.Date(2017, time.Month(2), 28, 12, 13, 14, 18, time.UTC)),
			},
			expected: []*Run{
				&Run{
					RunID:     RunID(1),
					JobID:     JobID(1),
					Input:     []byte("r1"),
					Status:    RunStatusPending,
					StartTime: TimePtr(time.Date(2017, time.Month(2), 28, 12, 13, 14, 15, time.UTC)),
				},
				&Run{
					RunID:     RunID(2),
					JobID:     JobID(1),
					Input:     []byte("r2"),
					Status:    RunStatusPending,
					StartTime: TimePtr(time.Date(2017, time.Month(2), 28, 12, 13, 14, 17, time.UTC)),
				},
			},
		},
		{
			name: "order by",
			createRuns: []*CreateRunInput{
				&CreateRunInput{
					JobID:   JobID(1),
					Input:   []byte("r1"),
					EndTime: TimePtr(time.Date(2017, time.Month(2), 28, 12, 13, 14, 15, time.UTC)),
				},
				&CreateRunInput{
					JobID:   JobID(1),
					Input:   []byte("r2"),
					EndTime: TimePtr(time.Date(2017, time.Month(2), 28, 12, 13, 14, 19, time.UTC)),
				},
				&CreateRunInput{
					JobID:   JobID(1),
					Input:   []byte("r3"),
					EndTime: TimePtr(time.Date(2017, time.Month(2), 28, 12, 13, 14, 17, time.UTC)),
				},
			},
			query: &GetRunsInput{
				OrderBy: StringPtr("end_time"),
			},
			expected: []*Run{
				&Run{
					RunID:   RunID(1),
					JobID:   JobID(1),
					Input:   []byte("r1"),
					Status:  RunStatusPending,
					EndTime: TimePtr(time.Date(2017, time.Month(2), 28, 12, 13, 14, 15, time.UTC)),
				},
				&Run{
					RunID:   RunID(3),
					JobID:   JobID(1),
					Input:   []byte("r3"),
					Status:  RunStatusPending,
					EndTime: TimePtr(time.Date(2017, time.Month(2), 28, 12, 13, 14, 17, time.UTC)),
				},
				&Run{
					RunID:   RunID(2),
					JobID:   JobID(1),
					Input:   []byte("r2"),
					Status:  RunStatusPending,
					EndTime: TimePtr(time.Date(2017, time.Month(2), 28, 12, 13, 14, 19, time.UTC)),
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
			t.Errorf("%s:\nexpected %d run\ngot %d", test.name, len(test.expected), len(runs))
			t.FailNow()
		}
		for i, run := range runs {
			if !reflect.DeepEqual(test.expected[i], run) {
				t.Errorf("%s:\nexpected %+v\ngot %+v", test.name, test.expected[i], run)
			}
		}
		r.Close()
	}
}

func TestSQLiteUpdateRun(t *testing.T) {
	tests := []struct {
		name      string
		createRun *CreateRunInput
		updateRun *UpdateRunInput
		expected  *Run
	}{
		{
			name: "test all fields",
			createRun: &CreateRunInput{
				ProcessorConfig: ProcessorConfig{
					Type:   "type",
					Config: map[string]string{"key": "val"},
				},
				Status:             RunStatusPtr(RunStatusPending),
				StatusDetail:       StringPtr("detail"),
				ScheduledStartTime: time.Date(1985, 1, 19, 2, 37, 12, 4, time.UTC),
				Attempt:            IntPtr(1),
				StartTime:          TimePtr(time.Date(1986, 1, 19, 2, 37, 12, 4, time.UTC)),
				EndTime:            TimePtr(time.Date(1987, 1, 19, 2, 37, 12, 4, time.UTC)),
				Success:            BoolPtr(false),
				Input:              []byte("in"),
				Output:             []byte("out"),
				Log:                []byte("log"),
			},
			updateRun: &UpdateRunInput{
				ProcessorConfig: &ProcessorConfig{
					Type:   "type2",
					Config: map[string]string{"key2": "val2"},
				},
				Status:             RunStatusPtr(RunStatusComplete),
				StatusDetail:       StringPtr("detail2"),
				ScheduledStartTime: TimePtr(time.Date(2015, 1, 19, 2, 37, 12, 4, time.UTC)),
				Attempt:            IntPtr(2),
				StartTime:          TimePtr(time.Date(2016, 1, 19, 2, 37, 12, 4, time.UTC)),
				EndTime:            TimePtr(time.Date(2017, 1, 19, 2, 37, 12, 4, time.UTC)),
				Success:            BoolPtr(true),
				Input:              []byte("in2"),
				Output:             []byte("out2"),
				Log:                []byte("log2"),
			},
			expected: &Run{
				ProcessorConfig: ProcessorConfig{
					Type:   "type2",
					Config: map[string]string{"key2": "val2"},
				},
				Status:             RunStatusComplete,
				StatusDetail:       "detail2",
				ScheduledStartTime: time.Date(2015, 1, 19, 2, 37, 12, 4, time.UTC),
				Attempt:            2,
				StartTime:          TimePtr(time.Date(2016, 1, 19, 2, 37, 12, 4, time.UTC)),
				EndTime:            TimePtr(time.Date(2017, 1, 19, 2, 37, 12, 4, time.UTC)),
				Success:            true,
				Input:              []byte("in2"),
				Output:             []byte("out2"),
				Log:                []byte("log2"),
			},
		},
	}
	r := newTestRepo(t)
	defer r.Close()
	for _, test := range tests {
		id, err := r.CreateRun(test.createRun)
		if err != nil {
			t.Fatal(err)
		}
		upd := test.updateRun
		upd.RunID = id
		err = r.UpdateRun(upd)
		if err != nil {
			t.Fatal(err)
		}
		jobs, err := r.GetRuns(&GetRunsInput{RunID: &id})
		if err != nil {
			t.Fatal(err)
		}
		if len(jobs) != 1 {
			t.Fatalf("expected 1 job, got %d", len(jobs))
		}
		test.expected.RunID = upd.RunID
		if !reflect.DeepEqual(test.expected, jobs[0]) {
			t.Errorf("%s: expected %s\ngot: %s", test.name, test.expected, jobs[0])
		}
	}
}
