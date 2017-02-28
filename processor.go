package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
)

type RunProcessor interface {
	Process(inputJSON []byte) (*RunResult, error)
}

type RetryerConfig struct {
	Type   string
	Config map[string]string
}

func (r *RetryerConfig) Scan(src interface{}) error {
	srcBytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("retryer config: unexpected src type: %T", src)
	}
	retryerConfig := RetryerConfig{}
	if err := json.Unmarshal(srcBytes, &retryerConfig); err != nil {
		return err
	}
	*r = retryerConfig
	return nil
}

type ProcessorConfig struct {
	Type   string
	Config map[string]string
}

func (p *ProcessorConfig) Scan(src interface{}) error {
	srcBytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("processor config: unexpected src type: %T", src)
	}
	procConfig := ProcessorConfig{}
	if err := json.Unmarshal(srcBytes, &procConfig); err != nil {
		return err
	}
	*p = procConfig
	return nil
}

type ProcessorMaker func(config map[string]string) (RunProcessor, error)
type ProcessorFactory map[string]ProcessorMaker

func (pf ProcessorFactory) Add(processorType string, f ProcessorMaker) {
	if pf == nil {
		pf = ProcessorFactory{}
	}
	pf[processorType] = f
}

func (pf ProcessorFactory) Make(c ProcessorConfig) (RunProcessor, error) {
	if pf == nil {
		return nil, errors.New("ProcessorFactory not initialized")
	}
	m, ok := pf[c.Type]
	if !ok {
		return nil, errors.New("ProcessorMaker not found: " + c.Type)
	}
	return m(c.Config)
}

type RunResult struct {
	RunID   RunID
	Output  json.RawMessage
	Detail  string
	Success bool
	Log     []byte
}

type DebugProcessor struct {
	log *log.Logger
}

func NewDebugProcessor(w io.Writer) *DebugProcessor {
	return &DebugProcessor{
		log: log.New(os.Stdout, "", log.LstdFlags),
	}
}

func (p *DebugProcessor) Process(m []byte) (*RunResult, error) {
	return &RunResult{Success: true}, nil
}
