package pipeline

import (
	"database/sql"
	"encoding/json"
	sq "github.com/Masterminds/squirrel"
	"github.com/pkg/errors"
	"strconv"
	"strings"
)

const (
	JobTriggerEventTypeSuccess = "success"
	JobTriggerEventTypeFailure = "failure"
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
		status_detail TEXT,
		scheduled_start_time DATETIME NOT NULL,
		start_time DATETIME,
		end_time DATETIME,
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
		job_id_to_trigger INT NOT NULL,
		event_type TEXT NOT NULL,
		FOREIGN KEY (job_id) REFERENCES jobs(id)
		FOREIGN KEY (job_id_to_trigger) REFERENCES jobs(id)
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
		"group_concat(sucesses.job_id_to_trigger) AS success_job_ids",
		"group_concat(failures.job_id_to_trigger) AS failure_job_ids",
	).
		From("jobs").
		LeftJoin("job_triggers sucesses ON jobs.id = sucesses.job_id AND sucesses.event_type = ?", JobTriggerEventTypeSuccess).
		LeftJoin("job_triggers failures ON jobs.id = failures.job_id AND failures.event_type = ?", JobTriggerEventTypeFailure).
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
		successIds := StringPtr("")
		failureIds := StringPtr("")
		err := rows.Scan(
			&job.ID,
			&job.Name,
			&job.InputPayloadTemplate,
			&processor,
			&retryer,
			&cronSchedule,
			&successIds,
			&failureIds,
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
		job.ProcessorConfig = procConfig
		job.RetryerConfig = retryConfig
		job.Triggers.JobSuccess, err = parseGroupedJobIDs(successIds)
		if err != nil {
			return nil, err
		}
		job.Triggers.JobFailure, err = parseGroupedJobIDs(failureIds)
		if err != nil {
			return nil, err
		}
		job.Triggers.CronSchedule = CronSchedule(cronSchedule)
		jobs = append(jobs, &job)
	}
	if rows.Err() != nil {
		return nil, err
	}
	return jobs, nil
}

func StringPtr(s string) *string {
	return &s
}

func parseGroupedJobIDs(s *string) ([]JobID, error) {
	ids := []JobID{}
	if s == nil {
		return ids, nil
	}
	if *s == "" {
		return ids, nil
	}
	parts := strings.Split(*s, ",")
	for _, jobId := range parts {
		jobIdInt, err := strconv.ParseInt(jobId, 10, 64)
		if err != nil {
			return nil, err
		}
		ids = append(ids, JobID(jobIdInt))
	}
	return ids, nil
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
	err = s.insertJobTriggers(id, jobSuccess, jobFailure)
	if err != nil {
		return 0, err
	}
	return JobID(id), nil
}

func (s *SQLiteRepo) insertJobTriggers(jobID int64, jobSuccess, jobFailure []JobID) error {
	if len(jobSuccess) == 0 && len(jobFailure) == 0 {
		return nil
	}
	insert := sq.Insert("job_triggers").Columns("job_id", "job_id_to_trigger", "event_type")
	for _, j := range jobSuccess {
		insert = insert.Values(jobID, uint64(j), JobTriggerEventTypeSuccess)
	}
	for _, j := range jobFailure {
		insert = insert.Values(jobID, uint64(j), JobTriggerEventTypeFailure)
	}
	insertSQL, args, err := insert.ToSql()
	if err != nil {
		return err
	}
	_, err = s.DB.Exec(insertSQL, args...)
	return err
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

func (s *SQLiteRepo) UpdateJob(j *UpdateJobInput) error {
	//update jobs table
	update := sq.Update("jobs").Where(sq.Eq{"id": uint64(j.JobID)})
	fieldChanged := false
	if j.Name != nil {
		update = update.Set("name", *j.Name)
		fieldChanged = true
	}
	if j.InputPayloadTemplate != nil {
		update = update.Set("input_payload_template", j.InputPayloadTemplate)
		fieldChanged = true
	}
	if j.Processor != nil {
		d, err := json.Marshal(j.Processor)
		if err != nil {
			return errors.Wrap(err, "update job: err marshalling processor")
		}
		update = update.Set("processor_config", d)
		fieldChanged = true
	}
	if j.Retryer != nil {
		retryer, err := json.Marshal(j.Retryer)
		if err != nil {
			return errors.Wrap(err, "update job: err marshalling retryer")
		}
		update = update.Set("retryer_config", retryer)
		fieldChanged = true
	}
	if j.Triggers != nil && j.Triggers.CronSchedule != nil {
		update = update.Set("cron_schedule", string(*j.Triggers.CronSchedule))
		fieldChanged = true
	}
	if fieldChanged {
		updateSQL, args, err := update.ToSql()
		if err != nil {
			return errors.Wrap(err, "update job: err generating sql")
		}
		_, err = s.DB.Exec(updateSQL, args...)
		if err != nil {
			return errors.Wrap(err, "update job: err running query")
		}
	}

	//update job_triggers table
	var successes, failures []JobID
	if j.Triggers != nil && j.Triggers.JobSuccess != nil {
		//delete previous success triggers
		if err := s.deleteJobTriggers(j.JobID, JobTriggerEventTypeSuccess); err != nil {
			return errors.Wrap(err, "update job: err deleting failure triggers")
		}
		successes = j.Triggers.JobSuccess
	}
	if j.Triggers != nil && j.Triggers.JobFailure != nil {
		//delete previous failure triggers
		if err := s.deleteJobTriggers(j.JobID, JobTriggerEventTypeFailure); err != nil {
			return errors.Wrap(err, "update job: err deleting failure triggers")
		}
		failures = j.Triggers.JobFailure
	}
	err := s.insertJobTriggers(int64(j.JobID), successes, failures)
	if err != nil {
		return errors.Wrap(err, "update job: error inserting job triggers")
	}
	return nil
}

func (s *SQLiteRepo) deleteJobTriggers(id JobID, eventType string) error {
	sql, args, err := sq.Delete("job_triggers").
		Where(sq.Eq{"job_id": uint64(id)}).
		Where(sq.Eq{"event_type": eventType}).
		ToSql()
	if err != nil {
		return errors.Wrap(err, "delete job triggers: err creating sql")
	}
	_, err = s.DB.Exec(sql, args...)
	if err != nil {
		return errors.Wrap(err, "delete job triggers: err executing query")
	}
	return nil
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
		"processor_config",
	).
		From("runs")
	if in.JobID != nil {
		runsQuery = runsQuery.Where(sq.Eq{"job_id": *in.JobID})
	}
	if in.RunID != nil {
		runsQuery = runsQuery.Where(sq.Eq{"id": *in.RunID})
	}
	if in.Status != nil {
		runsQuery = runsQuery.Where(sq.Eq{"status": *in.Status})
	}
	if in.StartTimeBefore != nil {
		runsQuery = runsQuery.Where(sq.Lt{"start_time": *in.StartTimeBefore})
		runsQuery = runsQuery.OrderBy("start_time")
	}
	if in.OrderBy != nil && in.StartTimeBefore == nil {
		runsQuery = runsQuery.OrderBy(*in.OrderBy)
	} else {
		runsQuery = runsQuery.OrderBy("start_time")
	}
	query, args, err := runsQuery.ToSql()
	if err != nil {
		return nil, err
	}
	//run query
	rows, err := s.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	runs := []*Run{}
	for rows.Next() {
		run := Run{}
		var procConfig []byte
		var status string
		err := rows.Scan(
			&run.RunID,
			&run.JobID,
			&status,
			&run.ScheduledStartTime,
			&run.StartTime,
			&run.EndTime,
			&run.Attempt,
			&run.Success,
			&run.Input,
			&run.Output,
			&run.Log,
			&procConfig,
		)
		if err != nil {
			return nil, err
		}
		//make runstatus
		rs, err := RunStatusFromString(status)
		if err != nil {
			return nil, errors.Wrap(err, "get runs: error getting run status")
		}
		run.Status = rs
		//unmarshal ProcessorConfig
		pConfig := ProcessorConfig{}
		err = json.Unmarshal(procConfig, &pConfig)
		if err != nil {
			return nil, errors.Wrap(err, "get runs: err unmarshalling processor config")
		}
		run.ProcessorConfig = pConfig

		runs = append(runs, &run)
	}
	if rows.Err() != nil {
		return nil, err
	}
	return runs, nil

}
func (s *SQLiteRepo) CreateRun(in *CreateRunInput) (RunID, error) {
	procConfig, err := json.Marshal(in.Processor)
	if err != nil {
		return 0, errors.Wrap(err, "create run: err marshalling processor config")
	}
	insertSQL, args, err := sq.Insert("runs").
		Columns("job_id", "processor_config", "status", "scheduled_start_time", "attempt", "input", "success").
		Values(uint64(in.JobID), procConfig, in.Status, in.ScheduledStartTime, in.Attempt, in.Input, false).
		ToSql()
	if err != nil {
		return 0, errors.Wrap(err, "create run: err creating sql")
	}
	res, err := s.DB.Exec(insertSQL, args...)
	if err != nil {
		return 0, errors.Wrap(err, "create run: err executing query")
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
