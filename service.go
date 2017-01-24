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
	cron          *CronScheduler
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
	return nil
}

func (s *Service) startBackgroundWorker() {
	ticker := time.NewTicker(time.Second) // can probably change this to run on the min?
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			//check for new runs to start
			rs, err := s.runRepository.GetPendingRuns()
			if err != nil {
				s.log.Printf("err getting pending runs: %s", err)
				continue
			}
			for _, r := range rs {
				s.incomingRuns <- r
			}

		case r := <-s.incomingRuns:
			//process runs that should be run
			res, err := s.runProcessor.Process(r)
			if err != nil {
				s.log.Printf("err processing run: %s", err)
				continue
			}
			s.finishedRuns <- res

		case res := <-s.finishedRuns:
			//save the result of runs
			err := s.runRepository.SaveRunResult(res)
			if err != nil {
				s.log.Printf("err saving run result: %s", err)
				continue
			}
			if !res.Success {
				//rerun if necessary

			}
		}
	}
}
