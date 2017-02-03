package pipeline

import "testing"
import "reflect"

func TestJobCreateAndGet(t *testing.T) {
	path := "/tmp/bolttest.db"
	db, err := NewBoltDB(path)
	if err != nil {
		t.Fatal(err)
	}
	j1, err := db.CreateJob(&CreateJobInput{
		Name:                 "job name",
		InputPayloadTemplate: nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("fetching %d", j1.ID)
	js, err := db.GetJobs(&GetJobsInput{
		JobIDs: []JobID{j1.ID},
	})
	if len(js) != 1 {
		t.Errorf("job not found")
		t.FailNow()
	}
	if !reflect.DeepEqual(j1, js[0]) {
		t.Errorf("expected to be equal:\nj1: %s\nj2: %s", j1, js[0])
	}
}
