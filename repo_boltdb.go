package pipeline

import (
	"encoding/json"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
)

const (
	JobsBucket = Bucket("jobs")
	RunsBucket = Bucket("runs")
)

type Bucket string

func (b Bucket) Bytes() []byte {
	return []byte(b)
}

type BoltDB struct {
	*bolt.DB
	jobBucket *bolt.Bucket
	runBucket *bolt.Bucket
}

func NewBoltDB(path string) (*BoltDB, error) {
	bdb, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	b := &BoltDB{
		DB: bdb,
	}
	err = b.MigrateDB()
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (b *BoltDB) MigrateDB() error {
	var err error
	return b.DB.Update(func(tx *bolt.Tx) error {
		b.jobBucket, err = tx.CreateBucketIfNotExists(JobsBucket.Bytes())
		if err != nil {
			return err
		}
		b.runBucket, err = tx.CreateBucketIfNotExists(RunsBucket.Bytes())
		if err != nil {
			return err
		}
		return nil
	})
}

func (b *BoltDB) CreateJob(c *CreateJobInput) (*Job, error) {
	if c == nil {
		return nil, errors.New("invalid CreateJobInput")
	}

	var j Job
	//save
	err := b.DB.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(JobsBucket.Bytes())
		id, _ := bkt.NextSequence()
		j = Job{
			ID:                   JobID(id),
			Name:                 c.Name,
			InputPayloadTemplate: c.InputPayloadTemplate,
			Processor:            &DebugProcessor{},
		}
		d, err := json.Marshal(j)
		if err != nil {
			return errors.Wrap(err, "could not marshal Job")
		}
		fmt.Printf("Put: %s, d: %s\n", j.ID.Bytes(), d)
		return bkt.Put(j.ID.Bytes(), d)
	})
	if err != nil {
		return nil, err
	}

	return &j, nil
}

func (b *BoltDB) GetJobs(in *GetJobsInput) ([]*Job, error) {
	if in == nil {
		return nil, errors.New("invalid GetJobsInput")
	}

	var jobs []*Job
	err := b.DB.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(JobsBucket.Bytes())
		for _, jid := range in.JobIDs {
			res := bkt.Get(jid.Bytes())
			job := Job{}
			err := json.Unmarshal(res, &job)
			if err != nil {
				//TODO: log error
				continue
			}
			jobs = append(jobs, &job)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return jobs, nil
}
