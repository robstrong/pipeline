package pipeline

import (
	"encoding/json"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/lambda"
)

type RunProcessor interface {
	Process(json.RawMessage) (*RunResult, error)
}

type RunResult struct {
	RunID   RunID
	Output  json.RawMessage
	Detail  string
	Success bool
	Log     []byte
}

type LambdaProcessor struct {
	FunctionName string
	LambdaClient *lambda.Lambda
}

func (p *LambdaProcessor) MarshalJSON() ([]byte, error) {
	return nil, nil
}
func (p *LambdaProcessor) UnmarshalJSON([]byte) error {
	return nil
}

func (p *LambdaProcessor) Process(m json.RawMessage) (*RunResult, error) {
	out, err := p.LambdaClient.Invoke(
		&lambda.InvokeInput{
			FunctionName: &p.FunctionName,
			LogType:      aws.String("RequestResponse"),
			Payload:      []byte(m),
		},
	)
	if err != nil {
		return nil, err
	}
	if out.FunctionError != nil {
		return &RunResult{
			Output:  json.RawMessage(out.Payload),
			Success: false,
			Detail:  "lambda function error: " + *out.FunctionError,
			Log:     []byte(*out.LogResult),
		}, nil
	}
	return &RunResult{
		Output:  json.RawMessage(out.Payload),
		Success: true,
		Log:     []byte(*out.LogResult),
	}, nil
}

type DebugProcessor struct {
	log *log.Logger
}

func NewDebugProcessor(w io.Writer) *DebugProcessor {
	return &DebugProcessor{
		log: log.New(os.Stdout, "", log.LstdFlags),
	}
}

func (p *DebugProcessor) Process(m json.RawMessage) (*RunResult, error) {
	return &RunResult{Success: true}, nil
}

/*
func (p *DebugProcessor) MarshalJSON() ([]byte, error) {
		type Alias DebugProcessor
	aux := struct {
		*Alias
	}{
		Alias: (*Alias)(p),
	}
	if err := json.Unmarshal(d, &aux); err != nil {
		return err
	}
	if p == nil {
		return nil, nil
	}
	return json.Marshal(p)
}

func (p *DebugProcessor) UnmarshalJSON(d []byte) error {
	if p == nil {
		p = &DebugProcessor{}
	}
	return json.Unmarshal(d, p)
}
*/
