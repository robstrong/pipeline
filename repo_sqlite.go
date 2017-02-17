package pipeline

import "database/sql"

type SQLiteRepo struct {
	conn *sql.DB
}

func (s *SQLiteRepo) MigrateDB() error {
	s.conn.Exec("CREATE TABLE jobs ")
}

func placeholders(num int) string {

}
func (s *SQLiteRepo) GetJobs(in *GetJobsInput) ([]*Job, error) {
	s.conn.Query("SELECT * FROM jobs WHERE jobs.id IN (" + placeholders(len(in.JobIDs)) + "")", MakeInts(in.JobIDs))
}
func (s *SQLiteRepo) CreateJob(j *CreateJobInput) (*Job, error) {

}
func (s *SQLiteRepo) UpdateJob(j *UpdateJobInput) error {

}

func (s *SQLiteRepo) GetRuns(*GetRunsInput) ([]*Run, error) {

}
func (s *SQLiteRepo) CreateRun(*CreateRunInput) (*Run, error) {

}
func (s *SQLiteRepo) UpdateRun(*UpdateRunInput) error {

}
