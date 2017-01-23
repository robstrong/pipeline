package lambdapipeline

import (
	"bytes"
	"encoding/json"
	"html/template"
	"time"

	"github.com/gorhill/cronexpr"
)

type JobID string

type Job struct {
	ID                   JobID
	Name                 string
	LambdaFunction       string
	InputPayloadTemplate []byte
	Retryer              Retryer
	CronSchedule         *cronexpr.Expression
	RunAfter             []JobID
	DoNotOverlap         bool //if true, another run won't be started until the previous runs have completed
}

func (j *Job) MakeRun(jc JobContext) (*Run, error) {
	in, err := renderInput(j.InputPayloadTemplate, jc.PreviousOutput)
	if err != nil {
		return nil, err
	}
	return &Run{
		JobID:              j.ID,
		Attempt:            jc.Attempt + 1,
		ScheduledStartTime: jc.ScheduledStartTime,
		Input:              in,
	}, nil
}

func renderInput(tmpl []byte, data []byte) ([]byte, error) {
	var f interface{}
	err := json.Unmarshal(data, &f)
	if err != nil {
		return nil, err
	}

	t := template.Must(template.New("test").Parse(string(tmpl)))
	out := &bytes.Buffer{}
	err = t.Execute(out, f)
	if err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

type RunStatus string

const (
	RunStatusPending  RunStatus = "pending"
	RunStatusRunning  RunStatus = "running"
	RunStatusComplete RunStatus = "complete"
)

type Run struct {
	JobID              JobID
	RunID              string
	Status             RunStatus
	ScheduledStartTime time.Time
	StartTime          time.Time
	EndTime            time.Time
	Attempt            int
	Success            bool
	Input              []byte
	Output             []byte
	Log                []byte
}

type Retryer func(JobContext) bool

type DefaultRetryer struct {
	NumRetries int
}

func (r DefaultRetryer) ShouldRetry(c JobContext) bool {
	if c.Attempt >= r.NumRetries {
		return false
	}
	return true
}

type JobContext struct {
	Attempt            int       //starts at 0
	ScheduledStartTime time.Time //time job is scheduled to start
	PreviousOutput     []byte    //output from previous job
}

type CronSchedule struct {
	Minute     int
	Hour       int
	DayOfMonth int
	Month      int
	DayOfWeek  int
}

type RunResult struct {
	RunID   string
	Output  []byte
	Success bool
}
