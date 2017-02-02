package pipeline

import (
	"log"
	"time"
)

type Service struct {
	incomingRuns chan *Run
	finishedRuns chan *RunResult
	repo         Repository
	log          *log.Logger
	cron         *CronScheduler
}

func NewService(r Repository) *Service {
	return &Service{
		repo: r,
		cron: NewCronScheduler(time.Now(), time.Hour),
	}
}

// blocking
func (s *Service) ListenAndServe() error {
	go s.startBackgroundWorker()
	return nil
}

func TimePtr(t time.Time) *time.Time {
	return &t
}

func (s *Service) startBackgroundWorker() {
	ticker := time.NewTicker(time.Second) // can probably change this to run on the min?
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			//check for new runs to start
			rs, err := s.repo.GetRuns(&GetRunsInput{
				Status:          RunStatusPending.Ptr(),
				StartTimeBefore: TimePtr(time.Now()),
			})
			if err != nil {
				s.log.Printf("err getting pending runs: %s", err)
				continue
			}
			for _, r := range rs {
				s.incomingRuns <- r
			}

		case r := <-s.incomingRuns:
			//process runs that should be run
			res, err := r.Processor.Process(r.Input)
			if err != nil {
				s.log.Printf("err processing run: %s", err)
				continue
			}
			s.finishedRuns <- res

		case res := <-s.finishedRuns:
			//save the result of runs
			err := s.repo.UpdateRun(&UpdateRunInput{
				RunID:        RunID(res.RunID),
				Output:       res.Output,
				StatusDetail: &res.Detail,
				Log:          res.Log,
				Success:      &res.Success,
			})
			if err != nil {
				s.log.Printf("err saving run result: %s", err)
				continue
			}
			if res.Success {
				//rerun if necessary
			} else {

			}
		}
	}
}
