# Examples

This directory contains standalone examples demonstrating the use of buckets
for persistence in a web server context.

* [x] [`post.go`](post.go) - sets up an http server that persists json
  payloads sent via http POST requests.

* [x] [`roundtrip.go`](roundtrip.go) - sets up an http server that persists
  json payloads sent via http POST requests and returns the persisted
  payload for http GET requests. 
