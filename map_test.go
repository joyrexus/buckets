package buckets_test

import (
	"bytes"
	"fmt"
	"testing"
)

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

	// Setup slice of items to collect results.
	type item struct {
		Key, Value []byte
	}
	results := []item{}

	// Anon func to apply to each item in bucket.
	// Here, we're just going to collect the items just inserted.
	do := func(k, v []byte) error {
		results = append(results, item{k, v})
		return nil
	}

	// Now map the `do` function over each item.
	if err := letters.Map(do); err != nil {
		t.Error(err.Error())
	}

	// Finally, check to see if our results match the originally
	// inserted items.
	for i, want := range items {
		got := results[i]
		if !bytes.Equal(got.Key, want.Key) {
			t.Errorf("got %v, want %v", got.Key, want.Key)
		}
		if !bytes.Equal(got.Value, want.Value) {
			t.Errorf("got %v, want %v", got.Value, want.Value)
		}
	}
}

// Ensure that we can apply a function to the k/v pairs
// of keys with a given prefix.
func TestMapPrefix(t *testing.T) {
	// Delete any existing bucket named "things".
	bx.Delete([]byte("things"))

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

	// Now collect each item whose key starts with "A".
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

	// Setup slice of items to collect results.
	type item struct {
		Key, Value []byte
	}
	results := []item{}

	// Anon func to map over matched keys.
	do := func(k, v []byte) error {
		results = append(results, item{k, v})
		return nil
	}

	if err := things.MapPrefix(do, prefix); err != nil {
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

// Show that we can apply a function to the k/v pairs
// of keys with a given prefix.
func ExampleBucket_MapPrefix() {
	// Delete any existing bucket named "things".
	bx.Delete([]byte("things"))

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

	// Now collect each item whose key starts with "A".
	prefix := []byte("A")

	// Setup slice of items.
	type item struct {
		Key, Value []byte
	}
	results := []item{}

	// Anon func to map over matched keys.
	do := func(k, v []byte) error {
		results = append(results, item{k, v})
		return nil
	}

	if err := things.MapPrefix(do, prefix); err != nil {
		fmt.Printf("could not map items with prefix %s: %v\n", prefix, err)
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

// Ensure we can apply functions to the k/v pairs
// of keys within a given range.
func TestMapRange(t *testing.T) {
	// Delete any existing bucket named "years".
	bx.Delete([]byte("years"))

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

	// Time range to map over.
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

	// Setup slice of items to collect results.
	type item struct {
		Key, Value []byte
	}
	results := []item{}

	// Anon func to map over matched keys.
	do := func(k, v []byte) error {
		results = append(results, item{k, v})
		return nil
	}

	if err := years.MapRange(do, min, max); err != nil {
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

// Show that we can apply a function to the k/v pairs
// of keys within a given range.
func ExampleBucket_MapRange() {
	// Delete any existing bucket named "years".
	bx.Delete([]byte("years"))

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

	// Time range to map over: 1990 <= key <= 2000.
	min := []byte("1990")
	max := []byte("2000")

	// Setup slice of items to collect results.
	type item struct {
		Key, Value []byte
	}
	results := []item{}

	// Anon func to map over matched keys.
	do := func(k, v []byte) error {
		results = append(results, item{k, v})
		return nil
	}

	if err := years.MapRange(do, min, max); err != nil {
		fmt.Printf("could not map items within range: %v\n", err)
	}

	for _, item := range results {
		fmt.Printf("%s -> %s\n", item.Key, item.Value)
	}
	// Output:
	// 1990 -> 90
	// 1995 -> 95
	// 2000 -> 00
}
