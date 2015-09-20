package buckets

// A Scanner implements methods for scanning a subset of keys
// in a bucket and retrieving data from or about those keys.
type Scanner interface {
	// Map applies a func on each key/value pair scanned.
	Map(func(k, v []byte) error) error
	// Count returns a count of the scanned keys.
	Count() (int, error)
	// Keys returns a slice of the scanned keys.
	Keys() ([][]byte, error)
	// Values returns a slice of values from scanned keys.
	Values() ([][]byte, error)
	// Items returns a slice of k/v pairs from scanned keys.
	Items() ([]Item, error)
	// ItemMapping returns a mapping of k/v pairs from scanned keys.
	ItemMapping() (map[string][]byte, error)
}
