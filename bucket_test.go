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

// Ensure that we can apply functions to each k/v pair.
func TestMap(t *testing.T) {
	// Delete any existing bucket named "letters".
	bx.Delete([]byte("letters"))

	// Create a new bucket.
	letters, err := bx.New([]byte("letters"))
	if err != nil {
		t.Error(err.Error())
	}

	// Setup items to insert.
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

	wantKeys := []string{"A", "B", "C"}
	wantValues := []string{"alpha", "beta", "gamma"}

	var keys, values []string
	do := func(k, v []byte) error {
		keys = append(keys, string(k))
		values = append(values, string(v))
		return nil
	}
	if err := letters.Map(do); err != nil {
		t.Error(err.Error())
	}

	for i, want := range wantKeys {
		if got := keys[i]; want != got {
			t.Errorf("got %v, want %v", got, want)
		}
	}
	for i, want := range wantValues {
		if got := values[i]; want != got {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

// Ensure that we can apply a function to the k/v pairs
// of keys with a given prefix.
func TestMapPrefix(t *testing.T) {
	things, err := bx.New([]byte("things"))
	if err != nil {
		t.Error(err.Error())
	}

	// k, v pairs to put in `things` bucket
	pairs := []struct {
		k, v string
	}{
		{"A", "1"}, // `A` prefix match
		{"B", "0"},
		{"AA", "2"},  // match
		{"AAA", "3"}, // match
		{"BA", "1"},
		{"BAA", "2"},
		{"AAB", "2"}, // match
	}

	for _, pair := range pairs {
		key, val := []byte(pair.k), []byte(pair.v)
		if err = things.Put(key, val); err != nil {
			t.Error(err.Error())
		}
	}

	prefix := []byte("A")
	wantKeys := []string{"A", "AA", "AAA", "AAB"}
	wantValues := []string{"1", "2", "3", "2"}

	// collect keys and values of matched keys in `do` func
	var keys, values []string

	// anon func to map over keys with prefix "A"
	do := func(k, v []byte) error {
		keys = append(keys, string(k))
		values = append(values, string(v))
		return nil
	}

	if err := things.MapPrefix(do, prefix); err != nil {
		t.Error(err.Error())
	}

	for i, want := range wantKeys {
		if got := keys[i]; want != got {
			t.Errorf("got %v, want %v", got, want)
		}
	}
	for i, want := range wantValues {
		if got := values[i]; want != got {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

// Ensure we can apply functions to the k/v pairs
// of keys within a given range.
func TestMapRange(t *testing.T) {
	years, err := bx.New([]byte("years"))
	if err != nil {
		t.Error(err.Error())
	}

	// k, v pairs to put in `years` bucket
	pairs := []struct {
		k, v string
	}{
		{"1970", "70"},
		{"1975", "75"},
		{"1980", "80"},
		{"1985", "85"},
		{"1990", "90"}, // min = 1990
		{"1995", "95"}, // min < 1995 < max
		{"2000", "00"}, // max = 2000
		{"2005", "05"},
		{"2010", "10"},
	}

	// put pairs in `years` bucket
	for _, pair := range pairs {
		key, val := []byte(pair.k), []byte(pair.v)
		if err = years.Put(key, val); err != nil {
			t.Error(err.Error())
		}
	}

	// time range to map over
	min := []byte("1990")
	max := []byte("2000")

	// expected keys and values
	wantKeys := []string{"1990", "1995", "2000"}
	wantValues := []string{"90", "95", "00"}

	// collect keys and values of matched keys in `do` func
	var keys, values []string

	// anon func to map over keys within time range
	do := func(k, v []byte) error {
		keys = append(keys, string(k))
		values = append(values, string(v))
		return nil
	}

	if err := years.MapRange(do, min, max); err != nil {
		t.Error(err.Error())
	}

	for i, want := range wantKeys {
		if got := keys[i]; want != got {
			t.Errorf("got %v, want %v", got, want)
		}
	}
	for i, want := range wantValues {
		if got := values[i]; want != got {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
