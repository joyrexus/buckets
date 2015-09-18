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
	router.POST("/day/:day", control.post)
	router.GET("/day/:day", control.getDayTasks)
	router.GET("/weekend", control.getWeekendTasks)
	router.GET("/weekdays", control.getWeekdayTasks)

	// Start our web server.
	srv := httptest.NewServer(router)
	defer srv.Close()

	// Setup daily todos for client to post.
	posts := []*Todo{
		&Todo{Day: "mon", Task: "milk cows"},
		&Todo{Day: "mon", Task: "feed cows"},
		&Todo{Day: "mon", Task: "wash cows"},
		&Todo{Day: "tue", Task: "wash laundry"},
		&Todo{Day: "tue", Task: "fold laundry"},
		&Todo{Day: "tue", Task: "iron laundry"},
		&Todo{Day: "wed", Task: "flip burgers"},
		&Todo{Day: "thu", Task: "join army"},
		&Todo{Day: "fri", Task: "kill time"},
		&Todo{Day: "sat", Task: "have beer"},
		&Todo{Day: "sat", Task: "make merry"},
		&Todo{Day: "sun", Task: "take aspirin"},
		&Todo{Day: "sun", Task: "pray quietly"},
	}

	// Create our client.
	client := new(Client)

	// Use our client to post each daily todo.
	for _, todo := range posts {
		url := srv.URL + "/day/" + todo.Day
		if err := client.post(url, todo); err != nil {
			fmt.Printf("client post error: %v", err)
		}
	}

	// Now, let's try retrieving the persisted todos.

	// Get a list of tasks for each day.
	week := []string{"mon", "tue", "wed", "thu", "fri", "sat", "sun"}
	fmt.Println("daily tasks ...")
	for _, day := range week {
		url := srv.URL + "/day/" + day
		tasks, err := client.get(url)
		if err != nil {
			fmt.Printf("client get error: %v", err)
		}
		fmt.Printf("  %s: %s\n", day, tasks)
	}
	// Output:
	// daily tasks ...
	//   mon: milk cows, feed cows, wash cows
	//   tue: wash laundry, fold laundry, iron laundry
	//   wed: flip burgers
	//   thu: join army
	//   fri: kill time
	//   sat: have beer, make merry
	//   sun: take aspirin, pray quietly

	// Get a list of combined tasks for weekdays.
	tasks, err := client.get(srv.URL + "/weekdays")
	if err != nil {
		fmt.Printf("client get error: %v", err)
	}
	fmt.Printf("\nweekday tasks: %s\n", tasks)
	// Output:
	// weekday tasks: milk cows, feed cows, wash cows, wash laundry,
	// fold laundry, iron laundry, flip burgers, join army, kill time

	// Get a list of combined tasks for the weekend.
	tasks, err = client.get(srv.URL + "/weekend")
	if err != nil {
		fmt.Printf("client get error: %v", err)
	}
	fmt.Printf("\nweekend tasks: %s\n", tasks)
	// Output:
	// weekend tasks: have beer, make merry, take aspirin, pray quietly
}

/* -- MODELS --*/

// A Todo models a daily task.
type Todo struct {
	Task    string    // task to be done
	Day     string    // day to do task
	Created time.Time // when created
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
	prefixscan := map[string]*buckets.PrefixScanner{
		"mon": bk.NewPrefixScanner([]byte("1")),
		"tue": bk.NewPrefixScanner([]byte("2")),
		"wed": bk.NewPrefixScanner([]byte("3")),
		"thu": bk.NewPrefixScanner([]byte("4")),
		"fri": bk.NewPrefixScanner([]byte("5")),
		"sat": bk.NewPrefixScanner([]byte("6")),
		"sun": bk.NewPrefixScanner([]byte("7")),
	}
	rangescan := map[string]*buckets.RangeScanner{
		"weekday": bk.NewRangeScanner([]byte("1"), []byte("6")),
		"weekend": bk.NewRangeScanner([]byte("6"), []byte("8")),
	}
	return &Controller{bk, prefixscan, rangescan}
}

// This Controller handles requests for todo items.  The items are stored
// in a todos bucket.  The request URLs are used as bucket keys and the
// raw json payload as values.
//
// Note that since we're using `httprouter` (abbreviated as `mux` when
// imported) as our router, each method is a `httprouter.Handle` rather
// than a `http.HandlerFunc`.
type Controller struct {
	todos      *buckets.Bucket
	prefixscan map[string]*buckets.PrefixScanner
	rangescan  map[string]*buckets.RangeScanner
}

// getWeekendTasks handles get requests for `/weekend`, returning the
// combined task list for saturday and sunday. We utilize the range scanner
// we set up for scanning keys in our todos bucket, viz. for keys representing
// tasks for saturday (day number 6) and sunday (7). In other words, our
// range scanner iterates over keys within range 6 <= key <= 8.
func (c *Controller) getWeekendTasks(w http.ResponseWriter, r *http.Request,
	_ httprouter.Params) {

	items, err := c.rangescan["weekend"].Items()
	if err != nil {
		http.Error(w, err.Error(), 500)
	}

	taskList := &TaskList{"weekend", []string{}}

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

// getWeekdayTasks handles get requests for `/weekdays`, returning the
// combined task list for monday through friday. We utilize the range scanner
// we set up for scanning keys in our todos bucket, viz. for keys within
// the monday (day number 1) to friday (5) range.  In other words, our
// range scanner iterates over keys within range 1 <= key <= 6.
func (c *Controller) getWeekdayTasks(w http.ResponseWriter, r *http.Request,
	_ httprouter.Params) {

	items, err := c.rangescan["weekday"].Items()
	if err != nil {
		http.Error(w, err.Error(), 500)
	}

	taskList := &TaskList{"weekdays", []string{}}

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

// getDayTasks handles get requests for `/:day`, returning a particular
// day's task list.
func (c *Controller) getDayTasks(w http.ResponseWriter, r *http.Request,
	p httprouter.Params) {

	day := p.ByName("day")
	items, err := c.prefixscan[day].Items()
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
	p httprouter.Params) {

	// Read request body's json payload into buffer.
	b, err := ioutil.ReadAll(r.Body)
	todo, err := decode(b)
	if err != nil {
		http.Error(w, err.Error(), 500)
	}

	// Use the day number + creation time as key.
	day := p.ByName("day")
	num := dayNumMap[day] // number of day of week
	created := todo.Created.Format(time.RFC3339Nano)
	key := fmt.Sprintf("%d/%s", num, created)

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

// Our http client for sending requests.
type Client struct{}

// post sends a post request with a json payload.
func (c *Client) post(url string, todo *Todo) error {
	todo.Created = time.Now()
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

/* -- UTILITY FUNCTIONS, &c. -- */

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

var dayNumMap = map[string]int{
	"mon": 1,
	"tue": 2,
	"wed": 3,
	"thu": 4,
	"fri": 5,
	"sat": 6,
	"sun": 7,
}
