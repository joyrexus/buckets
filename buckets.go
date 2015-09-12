package buckets

import (
	"bytes"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

// A DB wraps a bolt database.
type DB struct {
	*bolt.DB
}

// OpenDB creates/opens a bolt database at the specified path.
// The returned DB inherits all methods from `bolt.DB`.
func OpenDB(path string) (*DB, error) {
	config := &bolt.Options{Timeout: 1 * time.Second}
	db, err := bolt.Open(path, 0600, config)
	if err != nil {
		return nil, fmt.Errorf("Couldn't open %s: %s", path, err)
	}
	return &DB{db}, nil
}

// New creates/opens a named bucket.
func (db *DB) New(name string) (*Bucket, error) {
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
	return &Bucket{db, bn}, nil
}

// Delete removes the named bucket.
func (db *DB) Delete(name string) error {
	return db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(name))
	})
}

// A Bucket contains a db and a bucket name.
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

// MapPrefix will apply `do` on each k/v pair of keys with prefix.
func (b *Bucket) MapPrefix(do func(k, v []byte) error, pre []byte) error {
	return b.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(b.name).Cursor()
		for k, v := c.Seek(pre); bytes.HasPrefix(k, pre); k, v = c.Next() {
			do(k, v)
		}
		return nil
	})
}

// MapRange will apply `do` on each k/v pair of keys within range.
func (b *Bucket) MapRange(do func(k, v []byte) error, min, max []byte) error {
	return b.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(b.name).Cursor()
		for k, v := c.Seek(min); isBefore(k, max); k, v = c.Next() {
			do(k, v)
		}
		return nil
	})
}

// A PrefixScanner scans a bucket for keys with a given prefix.
type PrefixScanner struct {
	bk     *Bucket
	prefix []byte
}

// NewPrefixScanner initializes a new prefix scanner.
func (b *Bucket) NewPrefixScanner(pre []byte) (*PrefixScanner, error) {
	return &PrefixScanner{b, pre}, nil
}

// Keys will return a slice of keys with prefix.
func (ps *PrefixScanner) Keys() ([][]byte, error) {
	var keys [][]byte
	err := ps.bk.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(ps.bk.name).Cursor()
		pre := ps.prefix
		for k, _ := c.Seek(pre); bytes.HasPrefix(k, pre); k, _ = c.Next() {
			keys = append(keys, k)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, err
}

// Values will return a slice of values for keys with prefix.
func (ps *PrefixScanner) Values() ([][]byte, error) {
	var values [][]byte
	err := ps.bk.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(ps.bk.name).Cursor()
		pre := ps.prefix
		for k, v := c.Seek(pre); bytes.HasPrefix(k, pre); k, v = c.Next() {
			values = append(values, v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return values, err
}

type Pair struct {
	Key []byte
	Value []byte
}

// Pairs will return a slice of key/value pairs for keys with prefix.
func (ps *PrefixScanner) Pairs() ([]Pair, error) {
	var pairs []Pair
	err := ps.bk.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(ps.bk.name).Cursor()
		pre := ps.prefix
		for k, v := c.Seek(pre); bytes.HasPrefix(k, pre); k, v = c.Next() {
			pairs = append(pairs, Pair{k, v})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return pairs, err
}

/* --- UTILITY FUNCS --- */

// isBefore checks whether `key` comes before `max`.
func isBefore(key, max []byte) bool {
	return key != nil && bytes.Compare(key, max) <= 0
}
