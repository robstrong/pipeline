package lambdapipeline

import (
	"log"
	"time"
)

type Service struct {
	incomingRuns  chan *Run
	finishedRuns  chan *RunResult
	runRepository RunRepository
	log           *log.Logger
	runProcessor  RunProcessor
}

type RunRepository interface {
	//Should return pending runs that are scheduled for <= now
	GetPendingRuns() ([]*Run, error)
	//Should update the run with id RunResult.RunID with the results
	SaveRunResult(*RunResult) error
}

type RunProcessor interface {
	Process(*Run) (*RunResult, error)
}

// blocking
func (s *Service) ListenAndServe() error {
	go s.startBackgroundWorker()
}

func (s *Service) startBackgroundWorker() {
	timer := time.NewTimer(time.Second) // can probably change this to run on the min?
	defer timer.Stop()
	for {
		select {
		case new = timer.C:
			//check for new runs to start
			rs, err := s.runRepository.GetPendingRuns()
			if err != nil {
				h.log.Printf("err getting pending runs: %s", err)
				continue
			}
			for _, r := range rs {
				h.incomingRuns <- r
			}

		case r <- h.incomingRuns:
			//process runs that should be run
			res, err := s.runProcessor.Process(r)
			if err != nil {
				h.log.Printf("err processing run: %s", err)
				continue
			}
			h.finishedRuns <- res

		case res <- h.finishedRuns:
			//save the result of runs
			err := s.runRepository.SaveRunResult(res)
			if err != nil {
				h.log.Printf("err saving run result: %s", err)
				continue
			}
		}
	}
}
