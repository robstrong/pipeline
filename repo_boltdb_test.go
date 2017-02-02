package pipeline

import "testing"

func TestJobCreateAndGet(t *testing.T) {
	path := "/tmp/bolttest.db"
	db, err := NewBoltDB(path)
	if err != nil {
		t.Fatal(err)
	}
	j, err := db.CreateJob(&CreateJobInput{})
	if err != nil {
		t.Fatal(err)
	}
}
