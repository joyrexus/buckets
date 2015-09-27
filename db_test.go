package buckets_test

import (
	"os"
	"testing"

	"github.com/joyrexus/buckets"
)

// Ensure we can open/close a buckets db.
func TestOpen(t *testing.T) {
	bx, err := buckets.Open(tempfile())
	if err != nil {
		t.Error(err.Error())
	}
	defer os.Remove(bx.Path())
	defer bx.Close()
}
