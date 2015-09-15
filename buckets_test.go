package buckets_test

import (
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/joyrexus/buckets"
)

var (
	bx   *buckets.DB    // buckets db used across tests
	path string         // file path to temp db
)

// TestMain handles setup and teardown for our tests.
func TestMain(m *testing.M) {
	setup()
	result := m.Run()
	if err := teardown(); err != nil {
		log.Fatal(err)
	}
	os.Exit(result)
}

// setup creates a new bux db for testing.
func setup() {
	var err error
	path = tempFilePath()
	bx, err = buckets.Open(path)
	// log.Printf("Temp file created: %v", path)
	if err != nil {
		log.Fatal(err)
	}
}

// teardown closes the db and removes the dbfile.
func teardown() error {
	if err := os.Remove(bx.Path()); err != nil {
		return err
	}
	// log.Printf("Temp file removed: %v", path)
	if err := bx.Close(); err != nil {
		return err
	}
	return nil
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
