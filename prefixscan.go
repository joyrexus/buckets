package buckets

import (
	"bytes"

	"github.com/boltdb/bolt"
)

// A PrefixScanner scans a bucket for keys with a given prefix.
type PrefixScanner struct {
	db         *DB
	BucketName []byte
	Prefix     []byte
}

// Map applies `do` on each key/value pair for keys with prefix.
func (ps *PrefixScanner) Map(do func(k, v []byte) error) error {
	pre := ps.Prefix
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
	pre := ps.Prefix
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
	pre := ps.Prefix
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
	pre := ps.Prefix
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
	pre := ps.Prefix
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
	pre := ps.Prefix
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
