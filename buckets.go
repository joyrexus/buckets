package buckets

import (
	"bytes"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

// DB type contains a bolt database.
type DB struct {
	*bolt.DB
}

// Open creates/opens a bolt database at the specified path.
// The returned DB inherits all methods from `bolt.DB`.
func Open(path string) (*DB, error) {
	config := &bolt.Options{Timeout: 1 * time.Second}
	db, err := bolt.Open(path, 0600, config)
	if err != nil {
		return nil, fmt.Errorf("Couldn't open %s: %s", path, err)
	}
	return &DB{db}, nil
}

// New creates/opens a named bucket.
func (db *DB) New(name string) (*bucket, error) {
	bn := []byte(name)
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bn)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &bucket{db, bn}, nil
}

// Delete removes the named bucket.
func (db *DB) Delete(name string) error {
	return db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(name))
	})
}

// Bucket is a type containing a bolt db and a bucket name.
type Bucket struct {
	db   *DB
	name []byte
}

// Put inserts value `v` with key `k`.
func (b *Bucket) Put(k, v []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(b.name).Put(k, v)
	})
}

// Delete removes key `k`.
func (b *Bucket) Delete(k []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(b.name).Delete(k)
	})
}

// Get retrieves the value for key `k`.
func (b *Bucket) Get(k []byte) ([]byte, error) {
	var value []byte
	err := b.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(b.name).Get(k)
		if v != nil {
			value = make([]byte, len(v))
			copy(value, v)
		}
		return nil
	})
	return value, err
}

// Map will apply `do` on each key/value pair.
func (b *Bucket) Map(do func(k, v []byte) error) error {
	return b.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(b.name).ForEach(do)
	})
}

// PrefixMap will apply `do` on each k/v pair of keys with prefix.
func (b *Bucket) PrefixMap(pre []byte, do func(k, v []byte) error) error {
	return b.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(b.name).Cursor()
		for k, v := c.Seek(pre); bytes.HasPrefix(k, pre); k, v = c.Next() {
			do(k, v)
		}
		return nil
	})
}
