package buckets_test

import (
	"fmt"
	"os"

	"github.com/joyrexus/buckets"
)

func ExamplePut() {
	// Open the database.
	bx, _ := buckets.Open(tempFilePath())
	defer os.Remove(bx.Path())
	defer bx.Close()

	bucket := []byte("things")
	key, val := []byte("A"), []byte("alpha")

	// Create a new `things` bucket.
	things, _ := bx.New(bucket)

	// Put our key and value into the `things` bucket.
	things.Put(key, val)

	// Read value back in a different read-only transaction.
	got, _ := things.Get(key)

	fmt.Printf("The value of %q in `%s` is %q\n", key, bucket, got)

	// Output:
	// The value of "A" in `things` is "alpha"
}

/*
func ExamplePrefixScanner() {
	// Open the database.
	bx, _ := buckets.Open(tempFilePath())
	defer os.Remove(bx.Path())
	defer bx.Close()
}
*/
