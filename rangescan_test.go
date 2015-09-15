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

	// items to put in `years` bucket
	yearItems := []struct {
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

	// insert items into `years` bucket
	if err = years.Insert(yearItems); err != nil {
		t.Error(err.Error())
	}

	// time range to scan over
	min := []byte("1990")
	max := []byte("2000")

	nineties, err := years.NewRangeScanner(min, max)
	if err != nil {
		t.Error(err.Error())
	}

	// expected count of items in range
	wantCount := 3

	// expected items
	wantItems := []struct {
		Key   []byte
		Value []byte
	}{
		{[]byte("1990"), []byte("90")},
		{[]byte("1995"), []byte("95")},
		{[]byte("2000"), []byte("00")},
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

	for i, want := range wantItems {
		if got := keys[i]; !bytes.Equal(got, want.Key) {
			t.Errorf("got %s, want %s", got, want.Key)
		}
	}

	values, err := nineties.Values()
	if err != nil {
		t.Error(err.Error())
	}

	for i, want := range wantItems {
		if got := values[i]; !bytes.Equal(got, want.Value) {
			t.Errorf("got %s, want %s", got, want.Value)
		}
	}

	// get k/v pairs for keys within range (1995 <= year <= 2000)
	items, err := nineties.Items()
	if err != nil {
		t.Error(err.Error())
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
