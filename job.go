package pipeline

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"strconv"
	"time"

	"github.com/gorhill/cronexpr"
)

type JobID uint64
type JobIDs []JobID

func (js *JobIDs) Scan(src interface{}) error {
	if src == nil {
		*js = JobIDs{}
		return nil
	}
	srcBytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("job ids: unexpected type: %T", src)
	}
	srcString := string(srcBytes)
	jobs, err := parseGroupedJobIDs(&srcString)
	if err != nil {
		return err
	}
	*js = jobs
	return nil
}

func JobIDPtr(i uint64) *JobID {
	jid := JobID(i)
	return &jid
}

func (j JobID) String() string {
	return strconv.FormatInt(int64(j), 10)
}
func (j JobID) Bytes() []byte {
	return []byte(strconv.FormatUint(uint64(j), 10))
}

func MakeInts(js []JobID) []int64 {
	is := make([]int64, len(js))
	for i, j := range js {
		is[i] = int64(j)
	}
	return is
}

type Job struct {
	ID                   JobID
	Name                 string
	ProcessorConfig      ProcessorConfig
	InputPayloadTemplate []byte
	RetryerConfig        RetryerConfig
	Triggers             TriggerEvents
	//DoNotOverlap         bool //if true, another run won't be started until the previous runs have completed
}

type TriggerEvents struct {
	CronSchedule CronSchedule
	JobSuccess   JobIDs
	JobFailure   JobIDs
}

type CronSchedule string

func (c *CronSchedule) Scan(src interface{}) error {
	srcBytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("cron schedule: unexpected type: %T", src)
	}
	*c = CronSchedule(string(srcBytes))
	return nil
}

func NewCronSchedule(s string) *CronSchedule {
	cs := CronSchedule(s)
	return &cs
}

func (c CronSchedule) Expression() (*cronexpr.Expression, error) {
	return cronexpr.Parse(string(c))
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

func (j *Job) String() string {
	d, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(d)
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

func (r RunStatus) String() string {
	return string(r)
}

const (
	RunStatusPending  RunStatus = "pending"
	RunStatusRunning  RunStatus = "running"
	RunStatusComplete RunStatus = "complete"
)

func RunStatusPtr(r RunStatus) *RunStatus {
	return &r
}

func RunStatusFromString(s string) (RunStatus, error) {
	switch s {
	case RunStatusPending.String():
		return RunStatusPending, nil
	case RunStatusComplete.String():
		return RunStatusComplete, nil
	case RunStatusRunning.String():
		return RunStatusRunning, nil
	}
	return "", errors.New("invalid run status: " + s)
}

type RunID uint64

func RunIDPtr(r uint64) *RunID {
	rid := RunID(r)
	return &rid
}

func (r RunID) String() string {
	return strconv.FormatInt(int64(r), 10)
}

func (r RunID) Bytes() []byte {
	return []byte(strconv.FormatInt(int64(r), 10))
}

type Run struct {
	RunID              RunID
	JobID              JobID
	ProcessorConfig    ProcessorConfig
	Status             RunStatus
	StatusDetail       string
	ScheduledStartTime time.Time
	StartTime          *time.Time
	EndTime            *time.Time
	Attempt            int
	Success            bool
	Input              []byte
	Output             []byte
	Log                []byte
}

func (r *Run) String() string {
	js, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return ""
	}
	return string(js)
}

type Retryer interface {
	ShouldRetry(JobContext) bool
}

type DefaultRetryer struct {
	NumRetries int
}

func (r DefaultRetryer) ShouldRetry(c JobContext) bool {
	return c.Attempt < r.NumRetries
}

type JobContext struct {
	Attempt            int             //starts at 0
	ScheduledStartTime time.Time       //time job is scheduled to start
	PreviousOutput     json.RawMessage //output from previous job
}
