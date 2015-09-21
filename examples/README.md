# Examples

This directory contains standalone examples demonstrating the use of buckets
for persistence in a web server context.

* [`post.go`](post.go) - sets up an http server that stores raw json
  payloads sent via http POST requests.

* [`roundtrip.go`](roundtrip.go) - extends the previous example by
  appropriately handling http requests sent to the same route with 
  different methods (GET or POST).

* [`prefix.go`](prefix.go) - extends the previous example to demonstrate [prefix scanning](https://godoc.org/github.com/joyrexus/buckets#PrefixScanner).

* [`range.go`](range.go) - extends the previous example to demonstrate [range scanning](https://godoc.org/github.com/joyrexus/buckets#RangeScanner).

* [`items.go`](range.go) - variant of the previous example demonstrating an alternative way to get items with a given key prefix or range: viz., using [`Bucket.PrefixItems`](https://godoc.org/github.com/joyrexus/buckets#PrefixItems) and [`Bucket.RangeItems`](https://godoc.org/github.com/joyrexus/buckets#RangeItems).  
