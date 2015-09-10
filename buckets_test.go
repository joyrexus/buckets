package buckets

import (
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

var (
	bx   *DB    // buckets db used across tests
	path string // file path to temp bux db
)

// TestOpen ensures that a bux db that can be opened/closed without error.
func TestOpen(t *testing.T) {
	temp := tempFilePath()
	b, err := Open(temp)
	if err != nil {
		t.Error(err.Error())
	}
	if err := b.Close(); err != nil {
		t.Error(err.Error())
	}
	if err := os.Remove(temp); err != nil {
		teardown()
		t.Fatal(err)
	}
}

// TestNew ensures that we can create a bucket.
func TestNew(t *testing.T) {
	if _, err := bx.New("things"); err != nil {
		t.Error(err.Error())
	}
}

// TestPut ensures that we can put stuff in a bucket.
func TestPut(t *testing.T) {
	things, err := bx.New("things")
	if err != nil {
		t.Error(err.Error())
	}

	// k, v pairs to put in `things` bucket
	pairs := []struct {
		k, v string
	}{
		{"A", "alpha"},
		{"B", "beta"},
		{"C", "gamma"},
	}

	for _, pair := range pairs {
		key, val := []byte(pair.k), []byte(pair.v)
		if err = things.Put(key, val); err != nil {
			t.Error(err.Error())
		}
	}
}

// TestGet ensures that we can get stuff from a bucket.
func TestGet(t *testing.T) {
	things, err := bx.New("things")
	if err != nil {
		t.Error(err.Error())
	}

	key := []byte("missing")
	if got, _ := things.Get(key); got != nil {
		t.Errorf("not expecting value for key %q: got %q", key, got)
	}

	// k, v pairs to get/check from `things` bucket
	pairs := []struct {
		k, v string
	}{
		{"A", "alpha"},
		{"B", "beta"},
		{"C", "gamma"},
	}

	for _, pair := range pairs {
		key, val := []byte(pair.k), []byte(pair.v)
		got, err := things.Get(key)
		if err != nil {
			t.Error(err.Error())
		}
		if !bytes.Equal(got, val) {
			t.Errorf("got %v, want %v", got, val)
		}
	}
}

// TestMap ensures that we can apply functions to each k/v pair.
func TestMap(t *testing.T) {
	things, err := bx.New("things")
	if err != nil {
		t.Error(err.Error())
	}

	wantKeys := []string{"A", "B", "C"}
	wantValues := []string{"alpha", "beta", "gamma"}

	var keys, values []string
	do := func(k, v []byte) error {
		keys = append(keys, string(k))
		values = append(values, string(v))
		return nil
	}
	if err := things.Map(do); err != nil {
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

// TestPrefixMap ensures that we can apply functions to each k/v pair.
func TestPrefixMap(t *testing.T) {
	things, err := bx.New("things")
	if err != nil {
		t.Error(err.Error())
	}

	// k, v pairs to put in `things` bucket
	pairs := []struct {
		k, v string
	}{
		{"A", "1"},   // `A` prefix match
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

	if err := things.PrefixMap(prefix, do); err != nil {
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

// TestDelete ensures that we can put stuff in a bucket.
func TestDelete(t *testing.T) {
	things, err := bx.New("things")
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

// TestDeleteBucket ensures that we can delete a bucket.
func TestDeleteBucket(t *testing.T) {
	if _, err := bx.New("foo"); err != nil {
		t.Error(err.Error())
	}
	if err := bx.Delete("foo"); err != nil {
		t.Error(err.Error())
	}
}

/* SETUP AND TEARDOWN LOGIC */

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
	bx, err = Open(path)
	// log.Printf("Temp file created: %v", path)
	if err != nil {
		log.Fatal(err)
	}
}

// teardown closes the db and removes the dbfile.
func teardown() error {
	if err := bx.Close(); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	// log.Printf("Temp file removed: %v", path)
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
