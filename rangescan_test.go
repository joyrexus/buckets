package buckets_test

import (
	"bytes"
	"testing"
)

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
	wantItems := []struct{
		Key []byte
		Value []byte
	}{
		{
			Key:   []byte("1990"),
			Value: []byte("90"),
		},
		{
			Key:   []byte("1995"),
			Value: []byte("95"),
		},
		{
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

	if err = bx.Delete([]byte("years")); err != nil {
		t.Error(err.Error())
	}
}
