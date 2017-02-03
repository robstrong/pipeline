package pipeline

import (
	"encoding/json"
	"time"

	"github.com/gorhill/cronexpr"
)

type Repository interface {
	GetJobs(*GetJobsInput) ([]*Job, error)
	CreateJob(j *CreateJobInput) (*Job, error)
	UpdateJob(j *UpdateJobInput) error

	GetRuns(*GetRunsInput) ([]*Run, error)
	CreateRun(*CreateRunInput) (*Run, error)
	UpdateRun(*UpdateRunInput) error
}

type GetJobsInput struct {
	JobIDs []JobID
}

type GetRunsInput struct {
	JobID           *JobID
	Status          *RunStatus
	StartTimeBefore *time.Time //causes OrderBy to be set to 'startTime'
	OrderBy         *string    //startTime or createdTime
}

type CreateRunInput struct {
	JobID              *JobID
	Processor          RunProcessor
	Status             RunStatus
	StatusDetail       *string
	ScheduledStartTime *time.Time
	StartTime          *time.Time
	EndTime            *time.Time
	Attempt            *int
	Success            *bool
	Input              *json.RawMessage
	Output             *json.RawMessage
	Log                []byte
}
type UpdateRunInput struct {
	RunID        RunID
	Processor    RunProcessor
	Status       *RunStatus
	StatusDetail *string
	StartTime    *time.Time
	EndTime      *time.Time
	Attempt      *int
	Success      *bool
	Input        json.RawMessage
	Output       json.RawMessage
	Log          []byte
}

type CreateJobInput struct {
	Name                 string
	Processor            RunProcessor
	InputPayloadTemplate json.RawMessage
	Retryer              Retryer
	CronSchedule         *cronexpr.Expression
	RunAfter             []*JobID
}

type UpdateJobInput struct {
	Name                 *string
	Processor            RunProcessor
	InputPayloadTemplate json.RawMessage
	Retryer              Retryer
	CronSchedule         *cronexpr.Expression
	RunAfter             []*JobID
}
