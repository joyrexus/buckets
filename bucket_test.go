package buckets_test

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/joyrexus/buckets"
)

// Ensure that we can create and delete a bucket.
func TestBucket(t *testing.T) {
	bx := NewTestDB()
	defer bx.Close()

	_, err := bx.New([]byte("things"))
	if err != nil {
		t.Error(err.Error())
	}

	if err := bx.Delete([]byte("things")); err != nil {
		t.Error(err.Error())
	}
}

// Ensure we can put an item in a bucket.
func TestPut(t *testing.T) {
	bx := NewTestDB()
	defer bx.Close()

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
	bx, _ := buckets.Open(tempfile())
	defer os.Remove(bx.Path())
	defer bx.Close()

	// Create a new `things` bucket.
	bucket := []byte("things")
	things, _ := bx.New(bucket)

	// Put key/value into the `things` bucket.
	key, value := []byte("A"), []byte("alpha")
	if err := things.Put(key, value); err != nil {
		fmt.Printf("could not insert item: %v", err)
	}

	// Read value back in a different read-only transaction.
	got, _ := things.Get(key)

	fmt.Printf("The value of %q in `%s` is %q\n", key, bucket, got)

	// Output:
	// The value of "A" in `things` is "alpha"
}

// Ensure we don't overwrite existing items when using PutNX.
func TestPutNX(t *testing.T) {
	bx := NewTestDB()
	defer bx.Close()

	things, err := bx.New([]byte("things"))
	if err != nil {
		t.Error(err.Error())
	}

	key := []byte("A")
	a, b := []byte("alpha"), []byte("beta")

	// Put key/a-value into the `things` bucket.
	if err := things.PutNX(key, a); err != nil {
		t.Error(err.Error())
	}

	// Read value back in a different read-only transaction.
	got, err := things.Get(key)
	if err != nil && !bytes.Equal(got, a) {
		t.Error(err.Error())
	}

	// Try putting key/b-value into the `things` bucket.
	if err := things.PutNX(key, b); err != nil {
		t.Error(err.Error())
	}

	// Value for key should still be a, not b.
	got, err = things.Get(key)
	if err != nil && !bytes.Equal(got, a) {
		t.Error(err.Error())
	}
}

// Show we don't overwrite existing values when using PutNX.
func ExampleBucket_PutNX() {
	bx, _ := buckets.Open(tempfile())
	defer os.Remove(bx.Path())
	defer bx.Close()

	// Create a new `things` bucket.
	bucket := []byte("things")
	things, _ := bx.New(bucket)

	// Put key/value into the `things` bucket.
	key, value := []byte("A"), []byte("alpha")
	if err := things.Put(key, value); err != nil {
		fmt.Printf("could not insert item: %v", err)
	}

	// Read value back in a different read-only transaction.
	got, _ := things.Get(key)

	fmt.Printf("The value of %q in `%s` is %q\n", key, bucket, got)

	// Try putting another value with same key.
	things.PutNX(key, []byte("beta"))

	// Read value back in a different read-only transaction.
	got, _ = things.Get(key)

	fmt.Printf("The value of %q in `%s` is still %q\n", key, bucket, got)

	// Output:
	// The value of "A" in `things` is "alpha"
	// The value of "A" in `things` is still "alpha"
}

// Ensure that a bucket that gets a non-existent key returns nil.
func TestGetMissing(t *testing.T) {
	bx := NewTestDB()
	defer bx.Close()

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
	bx := NewTestDB()
	defer bx.Close()

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
	bx := NewTestDB()
	defer bx.Close()

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
	bx, _ := buckets.Open(tempfile())
	defer os.Remove(bx.Path())
	defer bx.Close()

	letters, _ := bx.New([]byte("letters"))

	// Setup items to insert in `letters` bucket.
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

// Ensure we can safely insert items into a bucket without overwriting
// existing items.
func TestInsertNX(t *testing.T) {
	bx := NewTestDB()
	defer bx.Close()

	bk, err := bx.New([]byte("test"))

	// Put k/v into the `bk` bucket.
	k, v := []byte("A"), []byte("alpha")
	if err := bk.Put(k, v); err != nil {
		t.Error(err.Error())
	}

	// k/v pairs to put-if-not-exists
	items := []struct {
		Key, Value []byte
	}{
		{[]byte("A"), []byte("ALPHA")}, // key exists, so don't update
		{[]byte("B"), []byte("beta")},
		{[]byte("C"), []byte("gamma")},
	}

	err = bk.InsertNX(items)
	if err != nil {
		t.Error(err.Error())
	}

	gotItems, err := bk.Items()
	if err != nil {
		t.Error(err.Error())
	}

	// expected items
	expected := []struct {
		Key, Value []byte
	}{
		{[]byte("A"), []byte("alpha")}, // existing value not updated
		{[]byte("B"), []byte("beta")},
		{[]byte("C"), []byte("gamma")},
	}

	for i, got := range gotItems {
		want := expected[i]
		if !bytes.Equal(got.Value, want.Value) {
			t.Errorf("key %q: got %v, want %v", got.Key, got.Value, want.Value)
		}
	}
}

// Ensure that we can get items for all keys with a given prefix.
func TestPrefixItems(t *testing.T) {
	bx := NewTestDB()
	defer bx.Close()

	// Create a new things bucket.
	things, err := bx.New([]byte("things"))
	if err != nil {
		t.Error(err.Error())
	}

	// Setup items to insert.
	items := []struct {
		Key, Value []byte
	}{
		{[]byte("A"), []byte("1")},   // `A` prefix match
		{[]byte("AA"), []byte("2")},  // match
		{[]byte("AAA"), []byte("3")}, // match
		{[]byte("AAB"), []byte("2")}, // match
		{[]byte("B"), []byte("O")},
		{[]byte("BA"), []byte("0")},
		{[]byte("BAA"), []byte("0")},
	}

	// Insert 'em.
	if err := things.Insert(items); err != nil {
		t.Error(err.Error())
	}

	// Now get each item whose key starts with "A".
	prefix := []byte("A")

	// Expected items for keys with prefix "A".
	expected := []struct {
		Key, Value []byte
	}{
		{[]byte("A"), []byte("1")},
		{[]byte("AA"), []byte("2")},
		{[]byte("AAA"), []byte("3")},
		{[]byte("AAB"), []byte("2")},
	}

	results, err := things.PrefixItems(prefix)
	if err != nil {
		t.Error(err.Error())
	}

	for i, want := range expected {
		got := results[i]
		if !bytes.Equal(got.Key, want.Key) {
			t.Errorf("got %v, want %v", got.Key, want.Key)
		}
		if !bytes.Equal(got.Value, want.Value) {
			t.Errorf("got %v, want %v", got.Value, want.Value)
		}
	}
}

// Show that we can get items for all keys with a given prefix.
func ExampleBucket_PrefixItems() {
	bx, _ := buckets.Open(tempfile())
	defer os.Remove(bx.Path())
	defer bx.Close()

	// Create a new things bucket.
	things, _ := bx.New([]byte("things"))

	// Setup items to insert.
	items := []struct {
		Key, Value []byte
	}{
		{[]byte("A"), []byte("1")},   // `A` prefix match
		{[]byte("AA"), []byte("2")},  // match
		{[]byte("AAA"), []byte("3")}, // match
		{[]byte("AAB"), []byte("2")}, // match
		{[]byte("B"), []byte("O")},
		{[]byte("BA"), []byte("0")},
		{[]byte("BAA"), []byte("0")},
	}

	// Insert 'em.
	if err := things.Insert(items); err != nil {
		fmt.Printf("could not insert items in `things` bucket: %v\n", err)
	}

	// Now get items whose key starts with "A".
	prefix := []byte("A")

	results, err := things.PrefixItems(prefix)
	if err != nil {
		fmt.Printf("could not get items with prefix %q: %v\n", prefix, err)
	}

	for _, item := range results {
		fmt.Printf("%s -> %s\n", item.Key, item.Value)
	}
	// Output:
	// A -> 1
	// AA -> 2
	// AAA -> 3
	// AAB -> 2
}

// Ensure we can get items for all keys within a given range.
func TestRangeItems(t *testing.T) {
	bx := NewTestDB()
	defer bx.Close()

	years, err := bx.New([]byte("years"))
	if err != nil {
		t.Error(err.Error())
	}

	// Setup items to insert in `years` bucket
	items := []struct {
		Key, Value []byte
	}{
		{[]byte("1970"), []byte("70")},
		{[]byte("1975"), []byte("75")},
		{[]byte("1980"), []byte("80")},
		{[]byte("1985"), []byte("85")},
		{[]byte("1990"), []byte("90")}, // min = 1990
		{[]byte("1995"), []byte("95")}, // min < 1995 < max
		{[]byte("2000"), []byte("00")}, // max = 2000
		{[]byte("2005"), []byte("05")},
		{[]byte("2010"), []byte("10")},
	}

	// Insert 'em.
	if err := years.Insert(items); err != nil {
		t.Error(err.Error())
	}

	// Now get each item whose key is in the 1990 to 2000 range.
	min := []byte("1990")
	max := []byte("2000")

	// Expected items within time range: 1990 <= key <= 2000.
	expected := []struct {
		Key, Value []byte
	}{
		{[]byte("1990"), []byte("90")}, // min = 1990
		{[]byte("1995"), []byte("95")}, // min < 1995 < max
		{[]byte("2000"), []byte("00")}, // max = 2000
	}

	// Get items for keys within min/max range.
	results, err := years.RangeItems(min, max)
	if err != nil {
		t.Error(err.Error())
	}

	for i, want := range expected {
		got := results[i]
		if !bytes.Equal(got.Key, want.Key) {
			t.Errorf("got %v, want %v", got.Key, want.Key)
		}
		if !bytes.Equal(got.Value, want.Value) {
			t.Errorf("got %v, want %v", got.Value, want.Value)
		}
	}
}

// Show that we get items for keys within a given range.
func ExampleBucket_RangeItems() {
	bx, _ := buckets.Open(tempfile())
	defer os.Remove(bx.Path())
	defer bx.Close()

	// Create a new bucket named "years".
	years, _ := bx.New([]byte("years"))

	// Setup items to insert in `years` bucket
	items := []struct {
		Key, Value []byte
	}{
		{[]byte("1970"), []byte("70")},
		{[]byte("1975"), []byte("75")},
		{[]byte("1980"), []byte("80")},
		{[]byte("1985"), []byte("85")},
		{[]byte("1990"), []byte("90")}, // min = 1990
		{[]byte("1995"), []byte("95")}, // min < 1995 < max
		{[]byte("2000"), []byte("00")}, // max = 2000
		{[]byte("2005"), []byte("05")},
		{[]byte("2010"), []byte("10")},
	}

	// Insert 'em.
	if err := years.Insert(items); err != nil {
		fmt.Printf("could not insert items in `years` bucket: %v\n", err)
	}

	// Time range: 1990 <= key <= 2000.
	min := []byte("1990")
	max := []byte("2000")

	results, err := years.RangeItems(min, max)
	if err != nil {
		fmt.Printf("could not get items within range: %v\n", err)
	}

	for _, item := range results {
		fmt.Printf("%s -> %s\n", item.Key, item.Value)
	}
	// Output:
	// 1990 -> 90
	// 1995 -> 95
	// 2000 -> 00
}
