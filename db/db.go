package db

import (
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// Database is an open bolt database
type Database struct {
	db       *bolt.DB
	readOnly bool
}

var defaultBucket []byte = []byte("default")
var replicaBucket []byte = []byte("replica")

// NewDatabase returns an instance of a database that we can work with.
func NewDatabase(dbPath string, readOnly bool) (db *Database, closeFunc func() error, err error) {
	boltDb, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, nil, err
	}
	closeFunc = boltDb.Close
	// boltDb.NoSync = true
	db = &Database{db: boltDb, readOnly: readOnly}

	if err := db.createBucket(); err != nil {
		closeFunc()
		return nil, nil, fmt.Errorf("creating default bucket: %w", err)
	}

	return db, closeFunc, nil
}

func (d *Database) createBucket() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(defaultBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(replicaBucket); err != nil {
			return err
		}
		return nil
	})
}

// SetKey sets the key to the requested value into the default database or returns an error.
func (d *Database) SetKey(key []byte, value []byte) error {
	if d.readOnly {
		return errors.New("replica database does not support setting key")
	}
	return d.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(defaultBucket).Put(key, value); err != nil {
			return nil
		}
		if err := tx.Bucket(replicaBucket).Put(key, value); err != nil {
			return nil
		}
		return nil
	})
}

// GetNextKeyForReplication returns the key and value for the keys that have
// been changed / updated and have not yet been applied to replicas
func (d *Database) GetNextKeyForReplication() (key []byte, value []byte, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicaBucket)
		key, value = b.Cursor().First()
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return key, value, nil
}

// GetKey gets the value to the requested key from the default database or returns an error.
func (d *Database) GetKey(key []byte) ([]byte, error) {
	var result []byte
	d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		result = b.Get(key)
		return nil
	})
	return result, nil
}

func (d *Database) DeleteExtraKeys(isExtra func(key []byte) bool) error {
	var keys [][]byte
	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		b.ForEach(func(k, v []byte) error {
			if isExtra(k) {
				keys = append(keys, k)
			}
			return nil
		})
		return nil
	})
	if err != nil {
		return err
	}
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		for _, k := range keys {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}
