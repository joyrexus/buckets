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
	"strings"
	"time"

	"github.com/joyrexus/buckets"
	"github.com/julienschmidt/httprouter"
)

const verbose = false // if `true` you'll see log output

func main() {
	// Open a buckets database.
	bx, err := buckets.Open(tempFilePath())
	if err != nil {
		log.Fatalf("couldn't open db: %v", err)
	}

	// Delete and close the db when done.
	defer os.Remove(bx.Path())
	defer bx.Close()

	// Create a bucket for storing todos.
	bucket, err := bx.New([]byte("todos"))
	if err != nil {
		log.Fatalf("couldn't create todos bucket: %v", err)
	}

	// Initialize our controller for handling specific routes.
	control := NewController(bucket)

	// Create and setup our router.
	router := httprouter.New()
	router.GET("/:day", control.get)
	router.POST("/:day", control.post)

	// Start our web server.
	srv := httptest.NewServer(router)
	defer srv.Close()

	// Daily todos for client to post.
	posts := []*Todo{
		{Day: "mon", Task: "milk cows"},
		{Day: "mon", Task: "feed cows"},
		{Day: "mon", Task: "wash cows"},
		{Day: "tue", Task: "wash laundry"},
		{Day: "tue", Task: "fold laundry"},
		{Day: "tue", Task: "iron laundry"},
		{Day: "wed", Task: "flip burgers"},
		{Day: "thu", Task: "join army"},
		{Day: "fri", Task: "kill time"},
		{Day: "sat", Task: "have beer"},
		{Day: "sat", Task: "make merry"},
		{Day: "sun", Task: "take aspirin"},
		{Day: "sun", Task: "pray quietly"},
	}

	// Create our client.
	client := new(Client)

	// Have our client post each daily todo.
	for _, todo := range posts {
		url := srv.URL + "/" + todo.Day
		if err := client.post(url, todo); err != nil {
			fmt.Printf("client post error: %v", err)
		}
	}

	// Have our client get a list of tasks for each day.
	week := []string{"mon", "tue", "wed", "thu", "fri", "sat", "sun"}
	for _, day := range week {
		url := srv.URL + "/" + day
		tasks, err := client.get(url)
		if err != nil {
			fmt.Printf("client get error: %v", err)
		}
		fmt.Printf("%s: %s\n", day, tasks)
	}

	// Output:
	// mon: milk cows, feed cows, wash cows
	// tue: wash laundry, fold laundry, iron laundry
	// wed: flip burgers
	// thu: join army
	// fri: kill time
	// sat: have beer, make merry
	// sun: take aspirin, pray quietly
}

/* -- MODELS --*/

// A Todo models a daily task.
type Todo struct {
	Task    string    // task to be done
	Day     string    // day to do task
	Created time.Time // when created
}

// Encode marshals a Todo into a buffer.
func (todo *Todo) Encode() (*bytes.Buffer, error) {
	b, err := json.Marshal(todo)
	if err != nil {
		return &bytes.Buffer{}, err
	}
	return bytes.NewBuffer(b), nil
}

// A TaskList is a list of tasks for a particular day.
type TaskList struct {
	Day   string
	Tasks []string
}

/* -- CONTROLLER -- */

// NewController initializes a new instance of our controller.
// It provides handler methods for our router.
func NewController(bk *buckets.Bucket) *Controller {
	prefix := map[string]*buckets.PrefixScanner{
		"/mon": bk.NewPrefixScanner([]byte("/mon")),
		"/tue": bk.NewPrefixScanner([]byte("/tue")),
		"/wed": bk.NewPrefixScanner([]byte("/wed")),
		"/thu": bk.NewPrefixScanner([]byte("/thu")),
		"/fri": bk.NewPrefixScanner([]byte("/fri")),
		"/sat": bk.NewPrefixScanner([]byte("/sat")),
		"/sun": bk.NewPrefixScanner([]byte("/sun")),
	}
	return &Controller{bk, prefix}
}

// Controller handles requests for todo items.  The items are stored
// in a todos bucket.  The request URLs are used as bucket keys and the
// raw json payload as values.
//
// Note that since we're using `httprouter` (abbreviated as `mux` when
// imported) as our router, each method is a `httprouter.Handle` rather
// than a `http.HandlerFunc`.
type Controller struct {
	todos  *buckets.Bucket
	prefix map[string]*buckets.PrefixScanner
}

// get handles get requests for a particular day, returning the day's
// task list.
func (c *Controller) get(w http.ResponseWriter, r *http.Request,
	_ httprouter.Params) {

	day := r.URL.String()
	items, err := c.prefix[day].Items()
	if err != nil {
		http.Error(w, err.Error(), 500)
	}

	taskList := &TaskList{day, []string{}}

	for _, item := range items {
		todo, err := decode(item.Value)
		if err != nil {
			http.Error(w, err.Error(), 500)
		}
		taskList.Tasks = append(taskList.Tasks, todo.Task)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(taskList)
}

// post handles post requests to create a daily todo item.
func (c *Controller) post(w http.ResponseWriter, r *http.Request,
	_ httprouter.Params) {

	// Read request body's json payload into buffer.
	b, err := ioutil.ReadAll(r.Body)
	todo, err := decode(b)
	if err != nil {
		http.Error(w, err.Error(), 500)
	}

	// Use the day (url path) + creation time as key.
	key := fmt.Sprintf("%s/%s", r.URL, todo.Created.Format(time.RFC3339Nano))

	// Put key/buffer into todos bucket.
	if err := c.todos.Put([]byte(key), b); err != nil {
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

// Client is our http client for sending requests.
type Client struct{}

// post sends a post request with a json payload.
func (c *Client) post(url string, todo *Todo) error {
	todo.Created = time.Now()
	bodyType := "application/json"
	body, err := todo.Encode()
	if err != nil {
		return err
	}
	resp, err := http.Post(url, bodyType, body)
	if err != nil {
		return err
	}
	if verbose {
		log.Printf("client: %s\n", resp.Status)
	}
	return nil
}

// get sends get requests and expects responses to be a json-encoded
// task list.
func (c *Client) get(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	taskList := new(TaskList)
	if err = json.NewDecoder(resp.Body).Decode(taskList); err != nil {
		return "", err
	}
	return strings.Join(taskList.Tasks, ", "), nil
}

/* -- UTILITY FUNCTIONS -- */

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
