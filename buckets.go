package buckets

import (
	"bytes"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

// A buckets DB is a set of buckets.  It's basically a wrapper
// around a bolt database, embedding the bolt.DB type.
type DB struct {
	*bolt.DB
}

// Open creates/opens a buckets database at the specified path.
func Open(path string) (*DB, error) {
	config := &bolt.Options{Timeout: 1 * time.Second}
	db, err := bolt.Open(path, 0600, config)
	if err != nil {
		return nil, fmt.Errorf("Couldn't open %s: %s", path, err)
	}
	return &DB{db}, nil
}

// New creates/opens a named bucket.
func (db *DB) New(name []byte) (*Bucket, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(name)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &Bucket{db, name}, nil
}

// Delete removes the named bucket.
func (db *DB) Delete(name []byte) error {
	return db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(name)
	})
}

/* --- BUCKET--- */

// Bucket represents a collection of key/value pairs inside the database.
type Bucket struct {
	db   *DB
	Name []byte
}

// Put inserts value `v` with key `k`.
func (bk *Bucket) Put(k, v []byte) error {
	return bk.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bk.Name).Put(k, v)
	})
}

// Delete removes key `k`.
func (bk *Bucket) Delete(k []byte) error {
	return bk.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bk.Name).Delete(k)
	})
}

// Get retrieves the value for key `k`.
func (bk *Bucket) Get(k []byte) (value []byte, err error) {
	err = bk.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bk.Name).Get(k)
		if v != nil {
			value = make([]byte, len(v))
			copy(value, v)
		}
		return nil
	})
	return value, err
}

// Insert iterates over a slice of k/v pairs, putting each item in
// the bucket as part of a single transaction.
func (bk *Bucket) Insert(items []struct{Key, Value []byte}) error {
	return bk.db.Update(func(tx *bolt.Tx) error {
		for _, item := range items {
			tx.Bucket(bk.Name).Put(item.Key, item.Value)
		}
		return nil
	})
}

// Map applies `do` on each key/value pair.
func (bk *Bucket) Map(do func(k, v []byte) error) error {
	return bk.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bk.Name).ForEach(do)
	})
}

// MapPrefix applies `do` on each k/v pair of keys with prefix.
func (bk *Bucket) MapPrefix(do func(k, v []byte) error, pre []byte) error {
	return bk.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bk.Name).Cursor()
		for k, v := c.Seek(pre); bytes.HasPrefix(k, pre); k, v = c.Next() {
			do(k, v)
		}
		return nil
	})
}

// MapRange applies `do` on each k/v pair of keys within range.
func (bk *Bucket) MapRange(do func(k, v []byte) error, min, max []byte) error {
	return bk.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bk.Name).Cursor()
		for k, v := c.Seek(min); isBefore(k, max); k, v = c.Next() {
			do(k, v)
		}
		return nil
	})
}

// NewPrefixScanner initializes a new prefix scanner.
func (bk *Bucket) NewPrefixScanner(pre []byte) (*PrefixScanner, error) {
	return &PrefixScanner{bk.db, bk.Name, pre}, nil
}

// NewRangeScanner initializes a new range scanner.  It takes a `min` and a
// `max` key for specifying the range paramaters.
func (bk *Bucket) NewRangeScanner(min, max []byte) (*RangeScanner, error) {
	return &RangeScanner{bk.db, bk.Name, min, max}, nil
}

/* --- PREFIX SCANNER --- */

// A PrefixScanner scans a bucket for keys with a given prefix.
type PrefixScanner struct {
	db         *DB
	BucketName []byte
	prefix     []byte
}

// Map applies `do` on each key/value pair for keys with prefix.
func (ps *PrefixScanner) Map(do func(k, v []byte) error) error {
	pre := ps.prefix
	return ps.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(ps.BucketName).Cursor()
		for k, v := c.Seek(pre); bytes.HasPrefix(k, pre); k, _ = c.Next() {
			do(k, v)
		}
		return nil
	})
}

// Count returns a count of the keys with prefix.
func (ps *PrefixScanner) Count() (count int, err error) {
	pre := ps.prefix
	err = ps.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(ps.BucketName).Cursor()
		for k, _ := c.Seek(pre); bytes.HasPrefix(k, pre); k, _ = c.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		return count, err
	}
	return count, err
}

// Keys returns a slice of keys with prefix.
func (ps *PrefixScanner) Keys() (keys [][]byte, err error) {
	pre := ps.prefix
	err = ps.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(ps.BucketName).Cursor()
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

// Values returns a slice of values for keys with prefix.
func (ps *PrefixScanner) Values() (values [][]byte, err error) {
	pre := ps.prefix
	err = ps.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(ps.BucketName).Cursor()
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

// Items returns a slice of key/value pairs for keys with prefix.
func (ps *PrefixScanner) Items() (items []Item, err error) {
	pre := ps.prefix
	err = ps.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(ps.BucketName).Cursor()
		for k, v := c.Seek(pre); bytes.HasPrefix(k, pre); k, v = c.Next() {
			items = append(items, Item{k, v})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return items, err
}

// ItemMapping returns a map of key/value pairs for keys with prefix.
// This only works with buckets whose keys are byte-sliced strings.
func (ps *PrefixScanner) ItemMapping() (map[string][]byte, error) {
	pre := ps.prefix
	items := make(map[string][]byte)
	err := ps.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(ps.BucketName).Cursor()
		for k, v := c.Seek(pre); bytes.HasPrefix(k, pre); k, v = c.Next() {
			items[string(k)] = v
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return items, err
}

/* --- RANGE SCANNER --- */

// A RangeScanner scans a bucket for keys within a given range.
type RangeScanner struct {
	db         *DB
	BucketName []byte
	Min        []byte
	Max        []byte
}

// Map applies `do` on each key/value pair for keys within range.
func (rs *RangeScanner) Map(do func(k, v []byte) error) error {
	return rs.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(rs.BucketName).Cursor()
		for k, v := c.Seek(rs.Min); isBefore(k, rs.Max); k, v = c.Next() {
			do(k, v)
		}
		return nil
	})
}

// Count returns a count of the keys within the range.
func (rs *RangeScanner) Count() (count int, err error) {
	err = rs.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(rs.BucketName).Cursor()
		for k, _ := c.Seek(rs.Min); isBefore(k, rs.Max); k, _ = c.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		return count, err
	}
	return count, err
}

// Values returns a slice of values for keys within the range.
// Keys returns a slice of keys within the range.
func (rs *RangeScanner) Keys() (keys [][]byte, err error) {
	err = rs.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(rs.BucketName).Cursor()
		for k, _ := c.Seek(rs.Min); isBefore(k, rs.Max); k, _ = c.Next() {
			keys = append(keys, k)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, err
}

// Values returns a slice of values for keys within the range.
func (rs *RangeScanner) Values() (values [][]byte, err error) {
	err = rs.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(rs.BucketName).Cursor()
		for k, v := c.Seek(rs.Min); isBefore(k, rs.Max); k, v = c.Next() {
			values = append(values, v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return values, err
}

// Items returns a slice of key/value pairs for keys within the range.
// Note that the returned slice contains elements of type Item.
func (rs *RangeScanner) Items() (items []Item, err error) {
	err = rs.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(rs.BucketName).Cursor()
		for k, v := c.Seek(rs.Min); isBefore(k, rs.Max); k, v = c.Next() {
			items = append(items, Item{k, v})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return items, err
}

// ItemMapping returns a map of key/value pairs for keys within the range.
// This only works with buckets whose keys are byte-sliced strings.
func (rs *RangeScanner) ItemMapping() (map[string][]byte, error) {
	items := make(map[string][]byte)
	err := rs.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(rs.BucketName).Cursor()
		for k, v := c.Seek(rs.Min); isBefore(k, rs.Max); k, v = c.Next() {
			items[string(k)] = v
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return items, err
}

/* --- UTILITY FUNCTIONS --- */

// isBefore checks whether `key` comes before `max`.
func isBefore(key, max []byte) bool {
	return key != nil && bytes.Compare(key, max) <= 0
}
