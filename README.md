# buckets

A simple key/value store based on [Bolt](https://github.com/boltdb/bolt).

![buckets](buckets.jpg)

Buckets is just an extension of Bolt, providing a `Bucket` type with some nifty convenience methods for operating on key/value pairs within it.

A bucket is a collection of unique keys that are associated with values. A buckets database is a set of buckets.  The underlying datastore is represented by a single file on disk.  

Use `go get github.com/joyrexus/buckets` to install then try `godoc github.com/joyrexus/buckets` for documentation.
