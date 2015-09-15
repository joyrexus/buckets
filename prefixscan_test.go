package buckets_test

import (
	"bytes"
	"testing"
)

// Ensure we can scan prefixes.
func TestPrefixScanner(t *testing.T) {
	paths, err := bx.New([]byte("paths"))

	// items to put in `paths` bucket
	pathItems := []struct {
		Key, Value []byte
	}{
		{[]byte("f/"), []byte("")},
		{[]byte("fo/"), []byte("")},
		{[]byte("foo/"), []byte("foo")},
		{[]byte("foo/bar/"), []byte("bar")},
		{[]byte("foo/bar/baz/"), []byte("baz")},
		{[]byte("food/"), []byte("")},
		{[]byte("good/"), []byte("")},
		{[]byte("goo/"), []byte("")},
	}

	if err = paths.Insert(pathItems); err != nil {
		t.Error(err.Error())
	}

	foo, err := paths.NewPrefixScanner([]byte("foo/"))
	if err != nil {
		t.Error(err.Error())
	}

	// expected items in `foo`
	wantItems := []struct {
		Key, Value []byte
	}{
		{[]byte("foo/"), []byte("foo")},
		{[]byte("foo/bar/"), []byte("bar")},
		{[]byte("foo/bar/baz/"), []byte("baz")},
	}

	// expected count of items in range
	want := len(wantItems)

	got, err := foo.Count()
	if err != nil {
		t.Error(err.Error())
	}

	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	// get keys for paths with `foo` prefix
	keys, err := foo.Keys()
	if err != nil {
		t.Error(err.Error())
	}

	for i, want := range wantItems {
		if got := keys[i]; !bytes.Equal(got, want.Key) {
			t.Errorf("got %s, want %s", got, want.Key)
		}
	}

	// get values for paths with `foo` prefix
	values, err := foo.Values()
	if err != nil {
		t.Error(err.Error())
	}

	for i, want := range wantItems {
		if got := values[i]; !bytes.Equal(got, want.Value) {
			t.Errorf("got %s, want %s", got, want.Value)
		}
	}

	// get k/v pairs for keys with `foo` prefix
	items, err := foo.Items()

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
