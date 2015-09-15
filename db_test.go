package buckets_test

import (
	"os"
	"testing"

	"github.com/joyrexus/buckets"
)

// Ensure we can open/close a buckets db.
func TestOpen(t *testing.T) {
	db, err := buckets.Open(tempFilePath())
	if err != nil {
		t.Error(err.Error())
	}
	defer os.Remove(db.Path())
	defer db.Close()
}

// Ensure that we can create and delete a bucket.
func TestBucket(t *testing.T) {
	_, err := bx.New([]byte("things"))
	if err != nil {
		t.Error(err.Error())
	}

	if err := bx.Delete([]byte("things")); err != nil {
		t.Error(err.Error())
	}
}
