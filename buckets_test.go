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
	path string // file path to temp db
)

// Ensure we can put an item in a bucket.
func TestPut(t *testing.T) {
	things, err := bx.New([]byte("things"))
	if err != nil {
		t.Error(err.Error())
	}

	// k, v pairs to put in `things` bucket
	pairs := []struct {
		key, value string
	}{
		{"A", "alpha"},
		{"B", "beta"},
		{"C", "gamma"},
	}

	for _, pair := range pairs {
		k, v := []byte(pair.key), []byte(pair.value)
		if err = things.Put(k, v); err != nil {
			t.Error(err.Error())
		}
	}
}

// Ensure that we can get stuff from a bucket.
func TestGet(t *testing.T) {
	things, err := bx.New([]byte("things"))
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

// Ensure that we can apply functions to each k/v pair.
func TestMap(t *testing.T) {
	things, err := bx.New([]byte("things"))
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

/* --- PREFIX SCANNER TESTS --- */

// Ensure we can scan prefixes.
func TestPrefixScanner(t *testing.T) {
	paths, err := bx.New([]byte("paths"))

	// k, v pairs to put in `paths` bucket
	putPairs := []struct {
		k, v string
	}{
		{"f/", ""},
		{"fo/", ""},
		{"foo/", "foo"},
		{"foo/bar/", "bar"},
		{"foo/bar/baz/", "baz"},
		{"food/", ""},
		{"good/", ""},
		{"goo/", ""},
	}

	// put pairs in `paths` bucket
	for _, pair := range putPairs {
		key, val := []byte(pair.k), []byte(pair.v)
		if err = paths.Put(key, val); err != nil {
			t.Error(err.Error())
		}
	}

	foo, err := paths.NewPrefixScanner([]byte("foo/"))
	if err != nil {
		t.Error(err.Error())
	}

	// expected count of items in range
	wantCount := 3

	count, err := foo.Count()
	if err != nil {
		t.Error(err.Error())
	}
	if count != wantCount {
		t.Errorf("got %v, want %v", count, wantCount)
	}

	// get keys for paths with `foo` prefix
	keys, err := foo.Keys()
	if err != nil {
		t.Error(err.Error())
	}

	// expected keys
	wantKeys := [][]byte{
		[]byte("foo/"),
		[]byte("foo/bar/"),
		[]byte("foo/bar/baz/"),
	}

	for i, want := range wantKeys {
		if got := keys[i]; !bytes.Equal(got, want) {
			t.Errorf("got %s, want %s", got, got, want, want)
		}
	}

	// get values for paths with `foo` prefix
	values, err := foo.Values()
	if err != nil {
		t.Error(err.Error())
	}

	// expected values
	wantValues := [][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
	}

	for i, want := range wantValues {
		if got := values[i]; !bytes.Equal(got, want) {
			t.Errorf("got %s, want %s", got, want)
		}
	}

	// get k/v pairs for keys with `foo` prefix
	items, err := foo.Items()

	// expected items
	wantItems := []Item{
		Item{
			Key:   []byte("foo/"),
			Value: []byte("foo"),
		},
		Item{
			Key:   []byte("foo/bar/"),
			Value: []byte("bar"),
		},
		Item{
			Key:   []byte("foo/bar/baz/"),
			Value: []byte("baz"),
		},
	}

	for i, want := range wantItems {
		got := items[i]
		if !bytes.Equal(got.Key, want.Key) {
			t.Errorf("got %s, want %s", got.Key, want.Key)
		}
		if !bytes.Equal(got.Value, want.Value) {
			t.Errorf("got %s, want %s", got.Value, want.Value)
		}
	}

	// expected mapping
	wantMapping := map[string][]byte{
		"foo/":         []byte("foo"),
		"foo/bar/":     []byte("bar"),
		"foo/bar/baz/": []byte("baz"),
	}

	// get mapping of k/v pairs for keys with `foo` prefix
	gotMapping, err := foo.ItemMapping()
	if err != nil {
		t.Error(err.Error())
	}

	for key, want := range wantMapping {
		got, ok := gotMapping[key]
		if ok == false {
			t.Errorf("missing wanted key: %s", key)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got %s, want %s", got, want)
		}
	}
}

/* --- RANGE SCANNER TESTS --- */

// Ensures we can scan ranges.
func TestRangeScanner(t *testing.T) {
	years, err := bx.New([]byte("years"))
	if err != nil {
		t.Error(err.Error())
	}

	// k, v pairs to put in `years` bucket
	putPairs := []struct {
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
	for _, pair := range putPairs {
		key, val := []byte(pair.k), []byte(pair.v)
		if err = years.Put(key, val); err != nil {
			t.Error(err.Error())
		}
	}

	// time range to scan over
	min := []byte("1990")
	max := []byte("2000")

	// expected count of items in range
	wantCount := 3

	// expected keys
	wantKeys := [][]byte{
		[]byte("1990"),
		[]byte("1995"),
		[]byte("2000"),
	}

	// expected values
	wantValues := [][]byte{
		[]byte("90"),
		[]byte("95"),
		[]byte("00"),
	}

	nineties, err := years.NewRangeScanner(min, max)
	if err != nil {
		t.Error(err.Error())
	}

	count, err := nineties.Count()
	if err != nil {
		t.Error(err.Error())
	}
	if count != wantCount {
		t.Errorf("got %v, want %v", count, wantCount)
	}

	keys, err := nineties.Keys()
	if err != nil {
		t.Error(err.Error())
	}

	for i, want := range wantKeys {
		if got := keys[i]; !bytes.Equal(got, want) {
			t.Errorf("got %s, want %s", got, want)
		}
	}

	values, err := nineties.Values()
	if err != nil {
		t.Error(err.Error())
	}
	for i, want := range wantValues {
		if got := values[i]; !bytes.Equal(got, want) {
			t.Errorf("got %s, want %s", got, want)
		}
	}

	// get k/v pairs for keys within range (1995 <= year <= 2000)
	items, err := nineties.Items()
	if err != nil {
		t.Error(err.Error())
	}

	// expected items
	wantItems := []Item{
		Item{
			Key:   []byte("1990"),
			Value: []byte("90"),
		},
		Item{
			Key:   []byte("1995"),
			Value: []byte("95"),
		},
		Item{
			Key:   []byte("2000"),
			Value: []byte("00"),
		},
	}

	for i, want := range wantItems {
		got := items[i]
		if !bytes.Equal(got.Key, want.Key) {
			t.Errorf("got %s, want %s", got.Key, want.Key)
		}
		if !bytes.Equal(got.Value, want.Value) {
			t.Errorf("got %s, want %s", got.Value, want.Value)
		}
	}

	// expected mapping
	wantMapping := map[string][]byte{
		"1990": []byte("90"),
		"1995": []byte("95"),
		"2000": []byte("00"),
	}

	// get mapping of k/v pairs for keys within range (1995 <= year <= 2000)
	gotMapping, err := nineties.ItemMapping()
	if err != nil {
		t.Error(err.Error())
	}

	for key, want := range wantMapping {
		got, ok := gotMapping[key]
		if ok == false {
			t.Errorf("missing wanted key: %s", key)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got %s, want %s", got, want)
		}
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
