package buckets

import (
	"bytes"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

// A DB is a bolt database with convenience methods for working with buckets.
//
// A DB embeds the exposed bolt.DB methods.
type DB struct {
	*bolt.DB
}

// Open creates/opens a buckets database at the specified path.
func Open(path string) (*DB, error) {
	config := &bolt.Options{Timeout: 1 * time.Second}
	db, err := bolt.Open(path, 0600, config)
	if err != nil {
		return nil, fmt.Errorf("couldn't open %s: %s", path, err)
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

/* -- ITEM -- */

// An Item holds a key/value pair.
type Item struct {
	Key   []byte
	Value []byte
}

/* -- BUCKET-- */

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

// PutNX (put-if-not-exists) inserts value `v` with key `k`
// if key doesn't exist.
func (bk *Bucket) PutNX(k, v []byte) error {
	v, err := bk.Get(k)
	if v != nil || err != nil {
		return err
	}
	return bk.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bk.Name).Put(k, v)
	})
}

// Insert iterates over a slice of k/v pairs, putting each item in
// the bucket as part of a single transaction.  For large insertions,
// be sure to pre-sort your items (by Key in byte-sorted order), which
// will result in much more efficient insertion times and storage costs.
func (bk *Bucket) Insert(items []struct{ Key, Value []byte }) error {
	return bk.db.Update(func(tx *bolt.Tx) error {
		for _, item := range items {
			tx.Bucket(bk.Name).Put(item.Key, item.Value)
		}
		return nil
	})
}

// InsertNX (insert-if-not-exists) iterates over a slice of k/v pairs,
// putting each item in the bucket as part of a single transaction.
// Unlike Insert, however, InsertNX will not update the value for an
// existing key.
func (bk *Bucket) InsertNX(items []struct{ Key, Value []byte }) error {
	return bk.db.Update(func(tx *bolt.Tx) error {
		for _, item := range items {
			v, _ := bk.Get(item.Key)
			if v == nil {
				tx.Bucket(bk.Name).Put(item.Key, item.Value)
			}
		}
		return nil
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

// Items returns a slice of key/value pairs.  Each k/v pair in the slice
// is of type Item (`struct{ Key, Value []byte }`).
func (bk *Bucket) Items() (items []Item, err error) {
	return items, bk.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bk.Name).Cursor()
		var key, value []byte
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if v != nil {
				key = make([]byte, len(k))
				copy(key, k)
				value = make([]byte, len(v))
				copy(value, v)
				items = append(items, Item{key, value})
			}
		}
		return nil
	})
}

// PrefixItems returns a slice of key/value pairs for all keys with
// a given prefix.  Each k/v pair in the slice is of type Item
// (`struct{ Key, Value []byte }`).
func (bk *Bucket) PrefixItems(pre []byte) (items []Item, err error) {
	err = bk.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bk.Name).Cursor()
		var key, value []byte
		for k, v := c.Seek(pre); bytes.HasPrefix(k, pre); k, v = c.Next() {
			if v != nil {
				key = make([]byte, len(k))
				copy(key, k)
				value = make([]byte, len(v))
				copy(value, v)
				items = append(items, Item{key, value})
			}
		}
		return nil
	})
	return items, err
}

// RangeItems returns a slice of key/value pairs for all keys within
// a given range.  Each k/v pair in the slice is of type Item
// (`struct{ Key, Value []byte }`).
func (bk *Bucket) RangeItems(min []byte, max []byte) (items []Item, err error) {
	err = bk.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bk.Name).Cursor()
		var key, value []byte
		for k, v := c.Seek(min); isBefore(k, max); k, v = c.Next() {
			if v != nil {
				key = make([]byte, len(k))
				copy(key, k)
				value = make([]byte, len(v))
				copy(value, v)
				items = append(items, Item{key, value})
			}
		}
		return nil
	})
	return items, err
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
func (bk *Bucket) NewPrefixScanner(pre []byte) *PrefixScanner {
	return &PrefixScanner{bk.db, bk.Name, pre}
}

// NewRangeScanner initializes a new range scanner.  It takes a `min` and a
// `max` key for specifying the range paramaters.
func (bk *Bucket) NewRangeScanner(min, max []byte) *RangeScanner {
	return &RangeScanner{bk.db, bk.Name, min, max}
}
