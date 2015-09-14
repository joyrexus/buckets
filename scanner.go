package buckets

// A Scanner implements methods for scanning a subset of keys
// in a bucket and retrieving data from or about those keys.
type Scanner interface {
	// Count returns a count of the scanned keys.
	Count() (int, error)
	// Keys returns a slice of the scanned keys.
	Keys() ([][]byte, error)
	// Values returns a slice of values from scanned keys.
	Values() ([][]byte, error)
	// Pairs returns a slice of k/v pairs from scanned keys.
	Pairs() ([]Pair, error)
	// Items returns a map of k/v pairs from scanned keys.
	Items() (map[string][]byte, error)
}

// A Pair holds a key/value pair.  Slices of Pairs are returned by the
// Pairs() method by types satisfying the Scanner interface.
// See PrefixScanner.Pairs() and RangeScanner.Pairs().
type Pair struct {
	Key   []byte
	Value []byte
}

