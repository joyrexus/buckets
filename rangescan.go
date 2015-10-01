package buckets

import "github.com/boltdb/bolt"

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
