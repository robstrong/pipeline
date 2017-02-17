package pipeline

import (
	"database/sql"
	sq "github.com/Masterminds/squirrel"
	"strings"
)

type SQLiteRepo struct {
	conn *sql.DB
}

func (s *SQLiteRepo) MigrateDB() error {
	s.conn.Exec("CREATE TABLE jobs ")
}

func placeholders(num int) string {
	sl := make([]string, num)
	for i := 0; i < num; i++ {
		sl[i] = "?"
	}
	return strings.Join(sl, ", ")
}

func (s *SQLiteRepo) GetJobs(in *GetJobsInput) ([]*Job, error) {
	//build SQL
	sqQuery := sq.Select("id", "name", "input_payload_template", "processor", "retryer").
		From("jobs").
		Where(sq.Eq{"jobs.id": MakeInts(in.JobIDs)})
	query, args, err := sqQuery.ToSql()
	if err != nil {
		return nil, err
	}
	//run query
	rows, err := s.conn.Query(query, args)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []*Job{}
	for rows.Next() {
		job := Job{}
		var processor, retryer string
		rows.Scan(
			&job.ID,
			&job.Name,
			&job.InputPayloadTemplate,
			&processor,
			&retryer,
		)
		//TODO: add logic for deserializing processor/retryer
		jobs = append(jobs, &job)
	}
	if rows.Err() != nil {
		return nil, err
	}
	return jobs, nil
}

func (s *SQLiteRepo) CreateJob(j *CreateJobInput) (JobID, error) {
	insertSQL, args, err := sq.Insert("jobs").
		Columns("name", "processor", "input_payload_template", "retryer", "cron_schedule").
		Values("name", j.Name).
		Values("processor", j.Processor).
		Values("input_payload_template", j.InputPayloadTemplate).
		Values("retryer", j.Retryer).
		Values("cron_schedule", j.CronSchedule).
		ToSql()
	if err != nil {
		return 0, err
	}
	res, err := s.conn.Exec(insertSQL, args)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return JobID(id), nil
}

func (s *SQLiteRepo) UpdateJob(j *UpdateJobInput) error {

}

func (s *SQLiteRepo) GetRuns(in *GetRunsInput) ([]*Run, error) {
	//build SQL
	runsQuery := sq.Select(
		"id",
		"job_id",
		"status",
		"scheduled_start_time",
		"start_time",
		"end_time",
		"attempt",
		"success",
		"input",
		"output",
		"log",
		"processor",
	).
		From("runs")
	if in.JobID != nil {
		runsQuery.Where(sq.Eq{"job_id": *in.JobID})
	}
	if in.Status != nil {
		runsQuery.Where(sq.Eq{"status": *in.Status})
	}
	if in.StartTimeBefore != nil {
		runsQuery.Where(sq.Lt{"start_time": *in.StartTimeBefore})
		runsQuery.OrderBy("start_time")
	}
	if in.OrderBy != nil && in.StartTimeBefore == nil {
		runsQuery.OrderBy(*in.OrderBy)
	} else {
		runsQuery.OrderBy("start_time")
	}
	query, args, err := runsQuery.ToSql()
	if err != nil {
		return nil, err
	}
	//run query
	rows, err := s.conn.Query(query, args)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	runs := []*Run{}
	for rows.Next() {
		run := Run{}
		var processor string
		rows.Scan(
			&run.RunID,
			&run.JobID,
			&run.Status,
			&run.ScheduledStartTime,
			&run.StartTime,
			&run.EndTime,
			&run.Attempt,
			&run.Success,
			&run.Input,
			&run.Output,
			&run.Log,
			&processor,
		)
		//TODO: add logic for deserializing processor
		runs = append(runs, &run)
	}
	if rows.Err() != nil {
		return nil, err
	}
	return runs, nil

}
func (s *SQLiteRepo) CreateRun(*CreateRunInput) (*Run, error) {

}
func (s *SQLiteRepo) UpdateRun(*UpdateRunInput) error {

}
