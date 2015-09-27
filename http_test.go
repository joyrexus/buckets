package buckets_test

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"

	"github.com/joyrexus/buckets"
)

// Set this to see how the counts are actually updated.
const verbose = false

// Counter updates a the hits bucket for every URL path requested.
type counter struct {
	hits *buckets.Bucket
}

// Our handler communicates the new count from a successful database
// transaction.
func (c counter) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	key := []byte(req.URL.String())

	// Decode handles key not found for us.
	value, _ := c.hits.Get(key)
	count := decode(value) + 1

	if err := c.hits.Put(key, encode(count)); err != nil {
		http.Error(rw, err.Error(), 500)
		return
	}

	if verbose {
		log.Printf("server: %s: %d", req.URL.String(), count)
	}

	// Reply with the new count .
	rw.Header().Set("Content-Type", "application/octet-stream")
	fmt.Fprintf(rw, "%d\n", count)
}

func client(id int, base string, paths []string) error {
	// Process paths in random order.
	rng := rand.New(rand.NewSource(int64(id)))
	permutation := rng.Perm(len(paths))

	for i := range paths {
		path := paths[permutation[i]]
		resp, err := http.Get(base + path)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		buf, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if verbose {
			log.Printf("client: %s: %s", path, buf)
		}
	}
	return nil
}

func ExampleBucket() {
	// Open the database.
	bx, _ := buckets.Open(tempfile())
	defer os.Remove(bx.Path())
	defer bx.Close()

	// Create a hits bucket
	hits, _ := bx.New([]byte("hits"))

	// Start our web server
	count := counter{hits}
	srv := httptest.NewServer(count)
	defer srv.Close()

	// Get every path multiple times.
	paths := []string{
		"/foo",
		"/bar",
		"/baz",
		"/quux",
		"/thud",
		"/xyzzy",
	}
	for id := 0; id < 10; id++ {
		if err := client(id, srv.URL, paths); err != nil {
			fmt.Printf("client error: %v", err)
		}
	}

	// Check the final result
	do := func(k, v []byte) error {
		fmt.Printf("hits to %s: %d\n", k, decode(v))
		return nil
	}
	hits.Map(do)
	// outputs ...
	// hits to /bar: 10
	// hits to /baz: 10
	// hits to /foo: 10
	// hits to /quux: 10
	// hits to /thud: 10
	// hits to /xyzzy: 10

	// Output:
	// hits to /bar: 10
	// hits to /baz: 10
	// hits to /foo: 10
	// hits to /quux: 10
	// hits to /thud: 10
	// hits to /xyzzy: 10
}

// encode marshals a counter.
func encode(n uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, n)
	return buf
}

// decode unmarshals a counter. Nil buffers are decoded as 0.
func decode(buf []byte) uint64 {
	if buf == nil {
		return 0
	}
	return binary.BigEndian.Uint64(buf)
}
