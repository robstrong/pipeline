package pipeline

import (
	"database/sql"
	"encoding/json"
	sq "github.com/Masterminds/squirrel"
)

type SQLiteRepo struct {
	DB *sql.DB
}

func NewSQLiteRepo(c *sql.DB) *SQLiteRepo {
	return &SQLiteRepo{DB: c}
}

func (s *SQLiteRepo) MigrateDB() error {
	_, err := s.DB.Exec(`
	CREATE TABLE IF NOT EXISTS jobs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT,
		input_payload_template TEXT,
		processor_config TEXT,
		retryer_config TEXT,
		cron_schedule TEXT
	)`)
	if err != nil {
		return err
	}
	_, err = s.DB.Exec(`
	CREATE TABLE IF NOT EXISTS runs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		job_id INT NOT NULL,
		processor_config TEXT NOT NULL,
        status TEXT NOT NULL,
		status_detail TEXT NOT NULL,
		scheduled_start_time DATETIME NOT NULL,
		start_time DATETIME NOT NULL,
		end_time DATETIME NOT NULL,
		attempt INT NOT NULL,
		success BOOL NOT NULL,
		input TEXT NOT NULL,
		output TEXT,
		log TEXT,
		FOREIGN KEY (job_id) REFERENCES jobs(id)
	)`)
	if err != nil {
		return err
	}
	_, err = s.DB.Exec(`
	CREATE TABLE IF NOT EXISTS job_triggers (
		job_id INT NOT NULL,
		job_to_trigger_id INT NOT NULL,
		event_type TEXT NOT NULL,
		FOREIGN KEY (job_id) REFERENCES jobs(id)
		FOREIGN KEY (job_to_trigger_id) REFERENCES jobs(id)
	)`)
	return err
}

func (s *SQLiteRepo) GetJobs(in *GetJobsInput) ([]*Job, error) {
	//build SQL
	sqQuery := sq.Select(
		"id",
		"name",
		"input_payload_template",
		"processor_config",
		"retryer_config",
		"cron_schedule",
	).
		From("jobs").
		Where(sq.Eq{"jobs.id": MakeInts(in.JobIDs)})
	query, args, err := sqQuery.ToSql()
	if err != nil {
		return nil, err
	}
	//run query
	rows, err := s.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []*Job{}
	for rows.Next() {
		job := Job{}
		var processor, retryer []byte
		var cronSchedule string
		err := rows.Scan(
			&job.ID,
			&job.Name,
			&job.InputPayloadTemplate,
			&processor,
			&retryer,
			&cronSchedule,
		)
		if err != nil {
			return nil, err
		}
		procConfig := ProcessorConfig{}
		if err := json.Unmarshal(processor, &procConfig); err != nil {
			return nil, err
		}
		retryConfig := RetryerConfig{}
		if err := json.Unmarshal(retryer, &retryConfig); err != nil {
			return nil, err
		}
		job.Processor = procConfig
		job.Retryer = retryConfig
		job.Triggers.CronSchedule = CronSchedule(cronSchedule)
		jobs = append(jobs, &job)
	}
	if rows.Err() != nil {
		return nil, err
	}
	return jobs, nil
}

func (s *SQLiteRepo) CreateJob(j *CreateJobInput) (JobID, error) {
	processor, err := json.Marshal(j.Processor)
	if err != nil {
		return 0, err
	}
	retryer, err := json.Marshal(j.Retryer)
	if err != nil {
		return 0, err
	}
	cronSchedule := ""
	var jobSuccess []JobID
	var jobFailure []JobID
	if j.Triggers != nil {
		if j.Triggers.CronSchedule != nil {
			cronSchedule = string(*j.Triggers.CronSchedule)
		}
		if len(j.Triggers.JobSuccess) > 0 {
			jobSuccess = j.Triggers.JobSuccess
		}
		if len(j.Triggers.JobFailure) > 0 {
			jobFailure = j.Triggers.JobFailure
		}
	}
	//insert job
	id, err := s.insertJob(j.Name, cronSchedule, processor, j.InputPayloadTemplate, retryer)
	//insert job triggers
	err := s.insertJobTriggers(jobSuccess, jobFailure)
	return JobID(id), nil
}

func (s *SQLiteRepo) insertJob(name, cronSchedule string, processor, inputPayload, retryer []byte) (int64, error) {
	insert := sq.Insert("jobs").
		Columns("name", "processor_config", "input_payload_template", "retryer_config", "cron_schedule").
		Values(name, processor, inputPayload, retryer, cronSchedule)
	insertSQL, args, err := insert.ToSql()
	if err != nil {
		return 0, err
	}
	res, err := s.DB.Exec(insertSQL, args...)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	return id, err
}

func (s *SQLiteRepo) insertJobTriggers(jobSuccess, jobFailure []JobID) error {

}

func (s *SQLiteRepo) UpdateJob(j *UpdateJobInput) error {
	update := sq.Update("jobs").Where(sq.Eq{"job_id": j.JobID})
	if j.Name != nil {
		update.Set("name", *j.Name)
	}
	if j.InputPayloadTemplate != nil {
		update.Set("input_payload_template", j.InputPayloadTemplate)
	}
	if j.Processor != nil {
		d, err := json.Marshal(j.Processor)
		if err != nil {
			return err
		}
		update.Set("processor", d)
	}
	if j.Retryer != nil {
		d, err := j.Retryer.Serialize()
		if err != nil {
			return err
		}
		update.Set("retryer", d)
	}
	if j.CronSchedule != nil {
		update.Set("cron_schedule", j.CronSchedule)
	}
	updateSQL, args, err := update.ToSql()
	if err != nil {
		return err
	}
	_, err = s.DB.Exec(updateSQL, args)
	return err
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
	rows, err := s.DB.Query(query, args)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	runs := []*Run{}
	for rows.Next() {
		run := Run{}
		var processor string
		err := rows.Scan(
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
		if err != nil {
			return nil, err
		}
		//TODO: add logic for deserializing processor
		runs = append(runs, &run)
	}
	if rows.Err() != nil {
		return nil, err
	}
	return runs, nil

}
func (s *SQLiteRepo) CreateRun(in *CreateRunInput) (RunID, error) {
	insertSQL, args, err := sq.Insert("runs").
		Columns("name", "processor", "input_payload_template", "retryer", "cron_schedule").
		Values("processor", in.Processor).
		Values("status", in.Status).
		Values("scheduled_start_time", in.ScheduledStartTime).
		Values("attempt", in.Attempt).
		Values("input", in.Input).
		ToSql()
	if err != nil {
		return 0, err
	}
	res, err := s.DB.Exec(insertSQL, args)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return RunID(id), nil
}
func (s *SQLiteRepo) UpdateRun(in *UpdateRunInput) error {
	update := sq.Update("runs").Where(sq.Eq{"run_id": in.RunID})
	if in.Status != nil {
		update.Set("status", *in.Status)
	}
	if in.StatusDetail != nil {
		update.Set("status_detail", in.StatusDetail)
	}
	if in.StartTime != nil {
		update.Set("start_time", *in.StartTime)
	}
	if in.EndTime != nil {
		update.Set("end_time", *in.EndTime)
	}
	if in.Success != nil {
		update.Set("success", *in.Success)
	}
	if in.Output != nil {
		update.Set("output", in.Output)
	}
	if in.Log != nil {
		update.Set("log", in.Log)
	}
	updateSQL, args, err := update.ToSql()
	if err != nil {
		return err
	}
	_, err = s.DB.Exec(updateSQL, args)
	return err
}
