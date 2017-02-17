package pipeline

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"testing"
)

func TestSQLiteJob(t *testing.T) {
	db, err := sql.Open("sqlite3", "./test.db")
	defer os.Remove("./test.db")
	if err != nil {
		t.Fatal(err)
	}
	r := NewSQLiteRepo(db)
	err = r.MigrateDB()
	if err != nil {
		t.Fatal(err)
	}
	cs := CronSchedule("* * * * *")
	jID, err := r.CreateJob(&CreateJobInput{
		Name: "test1",
		Processor: ProcessorConfig{
			Type:   "processortype",
			Config: []byte("pconfig"),
		},
		InputPayloadTemplate: []byte("payload"),
		CronSchedule:         &cs,
	})
	if err != nil {
		t.Fatal(err)
	}

	jobs, err := r.GetJobs(&GetJobsInput{JobIDs: []JobID{jID}})
	if err != nil {
		t.Fatal(err)
	}

	if len(jobs) != 1 {
		t.Errorf("expected 1 job, got %d", len(jobs))
		t.FailNow()
	}
	job := jobs[0]

	if job.Name != "test1" {
		t.Errorf("name does not match expected")
	}
}
