package pipeline

import (
	"encoding/json"

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

	var id uint64
	j := Job{
		ID:                   JobID(id),
		Name:                 c.Name,
		InputPayloadTemplate: c.InputPayloadTemplate,
	}
	d, err := json.Marshal(j)
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal Job")
	}
	//save
	err = b.DB.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(JobsBucket.Bytes())
		id, _ = bkt.NextSequence()
		return bkt.Put(j.ID.Bytes(), d)
	})

	j.ID = JobID(id)
	return &j, nil
}
