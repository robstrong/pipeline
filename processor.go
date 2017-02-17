package pipeline

import (
	"encoding/json"
	"io"
	"log"
	"os"
)

type RunProcessor interface {
	Init() error
	Process(json.RawMessage) (*RunResult, error)
	Serialize() ([]byte, error)
	Deserialize([]byte) error
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
