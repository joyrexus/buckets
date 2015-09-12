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

// A Pair holds a key/value pair.
type Pair struct {
	Key []byte
	Value []byte
}

// A Bucket holds a set of buckets.
type Bucket struct {
	*DB
	name []byte
}

// Put inserts value `v` with key `k`.
func (bk *Bucket) Put(k, v []byte) error {
	return bk.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bk.name).Put(k, v)
	})
}

// Delete removes key `k`.
func (bk *Bucket) Delete(k []byte) error {
	return bk.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bk.name).Delete(k)
	})
}

// Get retrieves the value for key `k`.
func (bk *Bucket) Get(k []byte) ([]byte, error) {
	var value []byte
	err := bk.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bk.name).Get(k)
		if v != nil {
			value = make([]byte, len(v))
			copy(value, v)
		}
		return nil
	})
	return value, err
}

// Map will apply `do` on each key/value pair.
func (bk *Bucket) Map(do func(k, v []byte) error) error {
	return bk.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bk.name).ForEach(do)
	})
}

// MapPrefix will apply `do` on each k/v pair of keys with prefix.
func (bk *Bucket) MapPrefix(do func(k, v []byte) error, pre []byte) error {
	return bk.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bk.name).Cursor()
		for k, v := c.Seek(pre); bytes.HasPrefix(k, pre); k, v = c.Next() {
			do(k, v)
		}
		return nil
	})
}

// MapRange will apply `do` on each k/v pair of keys within range.
func (bk *Bucket) MapRange(do func(k, v []byte) error, min, max []byte) error {
	return bk.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bk.name).Cursor()
		for k, v := c.Seek(min); isBefore(k, max); k, v = c.Next() {
			do(k, v)
		}
		return nil
	})
}

// A PrefixScanner scans a bucket for keys with a given prefix.
type PrefixScanner struct {
	*Bucket
	prefix []byte
}

// NewPrefixScanner initializes a new prefix scanner.
func (bk *Bucket) NewPrefixScanner(pre []byte) (*PrefixScanner, error) {
	return &PrefixScanner{bk, pre}, nil
}

// Keys will return a slice of keys with prefix.
func (ps *PrefixScanner) Keys() ([][]byte, error) {
	var keys [][]byte
	pre := ps.prefix
	err := ps.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(ps.name).Cursor()
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
	pre := ps.prefix
	err := ps.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(ps.name).Cursor()
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

// Pairs will return a slice of key/value pairs for keys with prefix.
func (ps *PrefixScanner) Pairs() ([]Pair, error) {
	var pairs []Pair
	pre := ps.prefix
	err := ps.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(ps.name).Cursor()
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

// A RangeScanner scans a bucket for keys within a given range.
type RangeScanner struct {
	*Bucket
	min []byte
	max []byte
}

// NewRangeScanner initializes a new range scanner.  It takes a `min` and a
// `max` key for specifying the range paramaters. 
func (bk *Bucket) NewRangeScanner(min, max []byte) (*RangeScanner, error) {
	return &RangeScanner{bk, min, max}, nil
}

// Keys will return a slice of keys within the range.
func (rs *RangeScanner) Keys() ([][]byte, error) {
	var keys [][]byte
	err := rs.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(rs.name).Cursor()
		for k, _ := c.Seek(rs.min); isBefore(k, rs.max); k, _ = c.Next() {
			keys = append(keys, k)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, err
}

// Values will return a slice of values for keys within the range.
func (rs *RangeScanner) Values() ([][]byte, error) {
	var values [][]byte
	err := rs.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(rs.name).Cursor()
		for k, v := c.Seek(rs.min); isBefore(k, rs.max); k, v = c.Next() {
			values = append(values, v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return values, err
}

// Pairs will return a slice of key/value pairs for keys within the range.
func (rs *RangeScanner) Pairs() ([]Pair, error) {
	var pairs []Pair
	err := rs.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(rs.name).Cursor()
		for k, v := c.Seek(rs.min); isBefore(k, rs.max); k, v = c.Next() {
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
