package pipeline

import (
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/lambda"
)

type LambdaProcessor struct {
	FunctionName string
	LambdaClient *lambda.Lambda
}

func (p *LambdaProcessor) Process(inputJSON []byte) (*RunResult, error) {
	out, err := p.LambdaClient.Invoke(
		&lambda.InvokeInput{
			FunctionName: &p.FunctionName,
			LogType:      aws.String("RequestResponse"),
			Payload:      []byte(inputJSON),
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
