package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"

	"github.com/joyrexus/buckets"
)

const verbose = true

func main() {
	// Open the database.
	bx, _ := buckets.Open(tempFilePath())
	defer os.Remove(bx.Path())
	defer bx.Close()

	// Create a todos bucket.
	todos, _ := bx.New([]byte("todos"))

	// Start our web server.
	handler := service{todos}
	srv := httptest.NewServer(handler)
	defer srv.Close()

	// Daily todos to post.
	posts := map[string]Todo {
		"/mon": Todo{Day: "mon", Task: "milk cows"},
		"/tue": Todo{Day: "tue", Task: "fold laundry"},
		"/wed": Todo{Day: "wed", Task: "flip burgers"},
		"/thu": Todo{Day: "thu", Task: "join army"},
		"/fri": Todo{Day: "fri", Task: "kill time"},
		"/sat": Todo{Day: "sat", Task: "make merry"},
		"/sun": Todo{Day: "sun", Task: "pray quietly"},
	}

	for path, todo := range posts {
		url := srv.URL + path
		bodyType := "application/json"
		body, err := encode(todo)
		if err != nil {
			log.Print(err)
		}
		resp, err := http.Post(url, bodyType, body)
		if err != nil {
			log.Print(err)
		}
		if verbose {
			log.Printf("client: %s\n", resp.Status)
		}
	}

	/*
		if err := client.Post(srv.URL, posts); err != nil {
			fmt.Printf("client post error: %v", err)
		}
	*/

	// Output:
	// /fri: kill time
	// /mon: milk cows
	// /sat: make merry
	// /sun: pray quietly
	// /thu: join army
	// /tue: fold laundry
	// /wed: flip burgers
}

type Todo struct {
	Task string
	Day  string
}

// This service handles post requests, storing them in a todos bucket.
// The URLs are used as bucket keys and the json payload as values.
type Service struct {
	todos *buckets.Bucket
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := []byte(r.URL.String())

	// Read request body's json payload into buffer.
	b, err := ioutil.ReadAll(r.Body)
	todo, err := decode(b)
	if err != nil {
		http.Error(w, err.Error(), 500)
	}

	// Put key/buffer into todos bucket
	if err := s.todos.Put(key, b); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	if verbose {
		log.Printf("server: %s: %v", key, todo)
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "put todo for %s: %s\n", key, todo)
}

type Client struct{}

/*
func (c *Client) Post(url, todo) error {
	bodyType := "application/json"
	body, err := encode(todo)
	if err != nil {
		return err
	}
	resp, err := http.Post(url, bodyType, body)
	if err != nil {
		log.Print(err)
	}
	if verbose {
		log.Printf("client: %s\n", resp.Status)
	}
}

func (c *Client) Get(url) error {
	for path, _ := range posts {
		resp, err := http.Get(base + path)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		todo, err := decode(b)
		if err != nil {
			return err
		}
		if verbose {
			log.Printf("client: %s: %s", path, buf)
		}
		fmt.Printf("%s: %s\n", path, todo.Task)
	}
	return nil
}
*/

// encode marshals a Todo into a buffer.
func encode(todo Todo) (*bytes.Buffer, error) {
	b, err := json.Marshal(todo)
	if err != nil {
		return &bytes.Buffer{}, err
	}
	return bytes.NewBuffer(b), nil
}

// decode unmarshals a json-encoded byteslice into a Todo.
func decode(b []byte) (*Todo, error) {
	todo := new(Todo)
	if err := json.Unmarshal(b, todo); err != nil {
		return &Todo{}, err
	}
	return todo, nil
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
