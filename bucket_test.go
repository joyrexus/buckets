package buckets_test

import (
	"bytes"
	"fmt"
	"testing"
)

// Ensure we can put an item in a bucket.
func TestPut(t *testing.T) {
	things, err := bx.New([]byte("things"))
	if err != nil {
		t.Error(err.Error())
	}

	key, value := []byte("A"), []byte("alpha")

	// Put key/value into the `things` bucket.
	if err := things.Put(key, value); err != nil {
		t.Error(err.Error())
	}

	// Read value back in a different read-only transaction.
	got, err := things.Get(key)
	if err != nil && !bytes.Equal(got, value) {
		t.Error(err.Error())
	}
}

// Show we can put an item in a bucket and get it back out.
func ExampleBucket_Put() {
	// Create a new `things` bucket.
	bucket := []byte("things")
	things, _ := bx.New(bucket)

	// Put key/value into the `things` bucket.
	key, value := []byte("A"), []byte("alpha")
	if err := things.Put(key, value); err != nil {
		fmt.Println("could not insert items!")
	}

	// Read value back in a different read-only transaction.
	got, _ := things.Get(key)

	fmt.Printf("The value of %q in `%s` is %q\n", key, bucket, got)

	// Output:
	// The value of "A" in `things` is "alpha"
}

// Ensure that a bucket that gets a non-existent key returns nil.
func TestGetMissing(t *testing.T) {
	things, err := bx.New([]byte("things"))
	if err != nil {
		t.Error(err.Error())
	}

	key := []byte("missing")
	if got, _ := things.Get(key); got != nil {
		t.Errorf("not expecting value for key %q: got %q", key, got)
	}
}

// Ensure that we can delete stuff in a bucket.
func TestDelete(t *testing.T) {
	things, err := bx.New([]byte("things"))
	if err != nil {
		t.Error(err.Error())
	}

	k, v := []byte("foo"), []byte("bar")
	if err = things.Put(k, v); err != nil {
		t.Error(err.Error())
	}

	if err = things.Delete(k); err != nil {
		t.Error(err.Error())
	}
}

// Ensure we can insert items into a bucket and get them back out.
func TestInsert(t *testing.T) {
	paths, err := bx.New([]byte("paths"))

	// k, v pairs to put in `paths` bucket
	items := []struct {
		Key, Value []byte
	}{
		{[]byte("foo/"), []byte("foo")},
		{[]byte("foo/bar/"), []byte("bar")},
		{[]byte("foo/bar/baz/"), []byte("baz")},
		{[]byte("food/"), []byte("")},
		{[]byte("good/"), []byte("")},
		{[]byte("goo/"), []byte("")},
	}

	err = paths.Insert(items)
	if err != nil {
		t.Error(err.Error())
	}

	gotItems, err := paths.Items()
	if err != nil {
		t.Error(err.Error())
	}

	// expected k/v mapping
	expected := map[string][]byte{
		"foo/":         []byte("foo"),
		"foo/bar/":     []byte("bar"),
		"foo/bar/baz/": []byte("baz"),
		"food/":        []byte(""),
		"good/":        []byte(""),
		"goo/":         []byte(""),
	}

	for _, item := range gotItems {
		want := expected[string(item.Key)]
		if !bytes.Equal(item.Value, want) {
			t.Errorf("got %v, want %v", item.Value, want)
		}
	}
}

// Show we can insert items into a bucket and get them back out.
func ExampleBucket_Insert() {
	letters, _ := bx.New([]byte("letters"))

	// Setup items to insert in `paths` bucket.
	items := []struct {
		Key, Value []byte
	}{
		{[]byte("A"), []byte("alpha")},
		{[]byte("B"), []byte("beta")},
		{[]byte("C"), []byte("gamma")},
	}

	// Insert items into `letters` bucket.
	if err := letters.Insert(items); err != nil {
		fmt.Println("could not insert items!")
	}

	// Get items back out in separate read-only transaction.
	results, _ := letters.Items()

	for _, item := range results {
		fmt.Printf("%s -> %s\n", item.Key, item.Value)
	}

	// Output:
	// A -> alpha
	// B -> beta
	// C -> gamma
}
