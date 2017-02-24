package pipeline

import (
	"encoding/json"
	"errors"
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
type ProcessorConfig struct {
	Type   string
	Config map[string]string
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

func (p *DebugProcessor) Process(m json.RawMessage) (*RunResult, error) {
	return &RunResult{Success: true}, nil
}

func (p *DebugProcessor) Serialize() ([]byte, error) {
	return json.Marshal(p)
}

func (p *DebugProcessor) Deserialize(d []byte) error {
	if p == nil {
		p = &DebugProcessor{}
	}
	return json.Unmarshal(d, p)
}
