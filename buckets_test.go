package buckets_test

import (
	"io/ioutil"
	"log"
	"os"

	"github.com/joyrexus/buckets"
)

type TestDB struct {
	*buckets.DB
}

// NewTestDB returns a TestDB using a temporary path.
func NewTestDB() *TestDB {
	bx, err := buckets.Open(tempfile())
	if err != nil {
		log.Fatalf("cannot open buckets database: %s", err)
	}
	// Return wrapped type.
	return &TestDB{bx}
}

// Close and delete buckets database.
func (db *TestDB) Close() {
	defer os.Remove(db.Path())
	db.DB.Close()
}

// tempfile returns a temporary file path.
func tempfile() string {
	f, err := ioutil.TempFile("", "bolt-")
	if err != nil {
		log.Fatalf("Could not create temp file: %s", err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
	if err := os.Remove(f.Name()); err != nil {
		log.Fatal(err)
	}
	return f.Name()
}
