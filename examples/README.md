# Examples

This directory contains standalone examples demonstrating the use of buckets
for persistence in a web server context.

* [`post.go`](post.go) - sets up an http server that [stores raw json payloads](https://github.com/joyrexus/buckets/blob/master/examples/post.go#L140-L141) sent via http POST requests.

* [`roundtrip.go`](roundtrip.go) - extends the previous example by appropriately [handling](https://github.com/joyrexus/buckets/blob/master/examples/roundtrip.go#L39-L42) http requests sent to the same route with different methods (GET or POST).

* [`prefix.go`](prefix.go) - extends the previous example to demonstrate [prefix scanning](https://github.com/joyrexus/buckets/blob/master/examples/prefix.go#L128-L135).

* [`range.go`](range.go) - extends the previous example to demonstrate [range scanning](https://github.com/joyrexus/buckets/blob/master/examples/range.go#L171-L174).

* [`items.go`](range.go) - variant of the previous example, demonstrating another way to get items with a given key prefix or range: viz., using [`Bucket.PrefixItems`](https://github.com/joyrexus/buckets/blob/master/examples/items.go#L238-L252) and [`Bucket.RangeItems`](https://github.com/joyrexus/buckets/blob/master/examples/items.go#L208-L218).  
