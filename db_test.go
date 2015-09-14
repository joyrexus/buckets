package buckets_test

import (
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/joyrexus/buckets"
)

// Ensure we can open/close a buckets db.
func TestOpen(t *testing.T) {
	bx, err := buckets.Open(tempFilePath())
	if err != nil {
		t.Error(err.Error())
	}
	defer os.Remove(bx.Path())
	defer bx.Close()
}

// Ensure that we can create and delete a bucket.
func TestBucket(t *testing.T) {
	bx, err := buckets.Open(tempFilePath())
	if err != nil {
		t.Error(err.Error())
	}
	defer os.Remove(bx.Path())
	defer bx.Close()

	_, err = bx.New([]byte("things"))
	if err != nil {
		t.Error(err.Error())
	}

	if err := bx.Delete([]byte("things")); err != nil {
		t.Error(err.Error())
	}
}

// tempFilePath returns a temporary file path.
func tempFilePath() string {
	f, _ := ioutil.TempFile("", "bolt-")
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
	if err := os.Remove(f.Name()); err != nil {
		log.Fatal(err)
	}
	return f.Name()
}
