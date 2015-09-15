package buckets_test

import (
	"bytes"
	"testing"
)

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
	wantItems := []struct{ 
		Key, Value []byte 
	}{
		{
			Key:   []byte("foo/"),
			Value: []byte("foo"),
		},
		{
			Key:   []byte("foo/bar/"),
			Value: []byte("bar"),
		},
		{
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

	if err = bx.Delete([]byte("paths")); err != nil {
		t.Error(err.Error())
	}
}
