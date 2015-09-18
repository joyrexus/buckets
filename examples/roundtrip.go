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
	mux "github.com/julienschmidt/httprouter"
)

const verbose = false // if `true` you'll see log output

func main() {
	// Open the database.
	bx, _ := buckets.Open(tempFilePath())
	defer os.Remove(bx.Path())
	defer bx.Close()

	// Create a bucket for storing todos.
	bucket, _ := bx.New([]byte("todos"))

	// Create our service for handling routes.
	service := NewService(bucket)

	// Create and setup our router.
	router := mux.New()
	router.GET("/:day", service.get)
	router.POST("/:day", service.post)

	// Start our web server.
	srv := httptest.NewServer(router)
	defer srv.Close()

	// Daily todos for client to post.
	posts := map[string]*Todo{
		"/mon": &Todo{Day: "mon", Task: "milk cows"},
		"/tue": &Todo{Day: "tue", Task: "fold laundry"},
		"/wed": &Todo{Day: "wed", Task: "flip burgers"},
		"/thu": &Todo{Day: "thu", Task: "join army"},
		"/fri": &Todo{Day: "fri", Task: "kill time"},
		"/sat": &Todo{Day: "sat", Task: "make merry"},
		"/sun": &Todo{Day: "sun", Task: "pray quietly"},
	}

	// Create our client.
	client := new(Client)

	for path, todo := range posts {
		url := srv.URL + path
		if err := client.post(url, todo); err != nil {
			fmt.Printf("client post error: %v", err)
		}
	}

	for path, _ := range posts {
		url := srv.URL + path
		task, err := client.get(url)
		if err != nil {
			fmt.Printf("client get error: %v", err)
		}
		fmt.Printf("%s: %s\n", path, task)
	}

	// Output:
	// /mon: milk cows
	// /tue: fold laundry
	// /wed: flip burgers
	// /thu: join army
	// /fri: kill time
	// /sat: make merry
	// /sun: pray quietly
}

/* -- MODELS --*/

type Todo struct {
	Task string
	Day  string
}

/* -- SERVICE -- */

// NewService initializes a new instance of our service.
func NewService(bk *buckets.Bucket) *Service {
	return &Service{bk}
}

// This service handles requests for todo items.  The items are stored
// in a todos bucket.  The request URLs are used as bucket keys and the
// raw json payload as values.
//
// In MVC parlance, our service would be called a "controller".  We use
// it to define "handle" methods for our router. Note that since we're using
// `httprouter` (abbreviated as `mux` when imported) as our router, each
// service method is a `httprouter.Handle` rather than a `http.HandlerFunc`.
type Service struct {
	todos *buckets.Bucket
}

// get handles get requests for a daily todo item.
func (s *Service) get(w http.ResponseWriter, r *http.Request, _ mux.Params) {
	key := []byte(r.URL.String())
	value, err := s.todos.Get(key)
	if err != nil {
		http.Error(w, err.Error(), 500)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(value)
}

// post handles post requests to create a daily todo item.
func (s *Service) post(w http.ResponseWriter, r *http.Request, _ mux.Params) {
	// Read request body's json payload into buffer.
	b, err := ioutil.ReadAll(r.Body)
	todo, err := decode(b)
	if err != nil {
		http.Error(w, err.Error(), 500)
	}

	// Use the url path as key.
	key := []byte(r.URL.String())

	// Put key/buffer into todos bucket.
	if err := s.todos.Put(key, b); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	if verbose {
		log.Printf("server: %s: %v", key, todo.Task)
	}

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "put todo for %s: %s\n", key, todo)
}

/* -- CLIENT -- */

// Our http client for sending requests.
type Client struct{}

// post sends a post request with a json payload.
func (c *Client) post(url string, todo *Todo) error {
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
	return nil
}

// get sends get requests and expects responses to be a json-encoded todo item.
func (c *Client) get(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	todo := new(Todo)
	if err = json.NewDecoder(resp.Body).Decode(todo); err != nil {
		return "", err
	}
	return todo.Task, nil
}

/* -- CODEC -- */

// encode marshals a Todo into a buffer.
func encode(todo *Todo) (*bytes.Buffer, error) {
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

/* -- UTILITY FUNCTIONS -- */

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
