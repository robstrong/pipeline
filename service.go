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

type Repository interface {
	GetJobs() ([]*Job, error)
	GetRecentRuns(start time.Time) ([]*Run, error)
	GetRunsForJob(id JobID) ([]*Run, error)
	CreateJob(j CreateJob) error
	UpdateJob(j UpdateJob) error
	ScheduleRun(id JobID, start time.Time) (*Run, error)
	//Should return pending runs that are scheduled for <= now
	GetPendingRuns() ([]*Run, error)
	//Should update the run with id RunResult.RunID with the results
	SaveRunResult(*RunResult) error
}

type CreateJob struct {
	Name string
}

type UpdateJob struct {
	Name *string
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
			rs, err := s.repo.GetPendingRuns()
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
			err := s.repo.SaveRunResult(res)
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
