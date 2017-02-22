package pipeline

import (
	"bytes"
	"encoding/json"
	"html/template"
	"strconv"
	"time"

	"github.com/gorhill/cronexpr"
)

type JobID uint64

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
	Processor            ProcessorConfig
	InputPayloadTemplate []byte
	Retryer              Retryer
	CronSchedule         CronSchedule
	//DoNotOverlap         bool //if true, another run won't be started until the previous runs have completed
}

type CronSchedule string

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

const (
	RunStatusPending  RunStatus = "pending"
	RunStatusRunning  RunStatus = "running"
	RunStatusComplete RunStatus = "complete"
)

func (r RunStatus) Ptr() *RunStatus {
	return &r
}

type RunID uint64

func (r RunID) String() string {
	return strconv.FormatInt(int64(r), 10)
}

func (r RunID) Bytes() []byte {
	return []byte(strconv.FormatInt(int64(r), 10))
}

type Run struct {
	RunID              RunID
	JobID              JobID
	Processor          RunProcessor
	Status             RunStatus
	StatusDetail       string
	ScheduledStartTime time.Time
	StartTime          time.Time
	EndTime            time.Time
	Attempt            int
	Success            bool
	Input              json.RawMessage
	Output             json.RawMessage
	Log                []byte
}

type Serializer interface {
	Serialize() ([]byte, error)
	Deserialize([]byte) error
}
type Retryer interface {
	Serializer
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
