package pipeline

import "time"

type Repository interface {
	GetJobs(*GetJobsInput) ([]*Job, error)
	CreateJob(j *CreateJobInput) (JobID, error)
	UpdateJob(j *UpdateJobInput) error

	GetRuns(*GetRunsInput) ([]*Run, error)
	CreateRun(*CreateRunInput) (RunID, error)
	UpdateRun(*UpdateRunInput) error
}

type GetJobsInput struct {
	JobIDs JobIDs
}

type GetRunsInput struct {
	JobID           *JobID
	RunID           *RunID
	Status          *RunStatus
	StartTimeBefore *time.Time //causes OrderBy to be set to 'startTime'
	OrderBy         *string    //start_time or created_time
}

type CreateRunInput struct {
	JobID              JobID
	Processor          ProcessorConfig
	Status             RunStatus
	StatusDetail       *string
	ScheduledStartTime time.Time
	Attempt            int
	StartTime          *time.Time
	EndTime            *time.Time
	Success            *bool
	Input              []byte
	Output             []byte
	Log                []byte
}

type UpdateRunInput struct {
	RunID        RunID
	Status       *RunStatus
	StatusDetail *string
	StartTime    *time.Time
	EndTime      *time.Time
	Success      *bool
	Output       []byte
	Log          []byte
}

type CreateJobInput struct {
	Name                 string
	Processor            ProcessorConfig
	InputPayloadTemplate []byte
	Retryer              RetryerConfig
	Triggers             *TriggerEventsInput
}

type TriggerEventsInput struct {
	CronSchedule *CronSchedule
	JobSuccess   JobIDs
	JobFailure   JobIDs
}

type UpdateJobInput struct {
	JobID                JobID
	Name                 *string
	Processor            *ProcessorConfig
	InputPayloadTemplate []byte
	Retryer              *RetryerConfig
	Triggers             *TriggerEventsInput
}
