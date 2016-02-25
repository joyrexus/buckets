# buckets

[![GoDoc](https://godoc.org/github.com/joyrexus/buckets?status.svg)](https://godoc.org/github.com/joyrexus/buckets)

A simple key/value store based on [Bolt](https://github.com/boltdb/bolt). 

![buckets](buckets.jpg)

In the parlance of [key/value stores](https://en.wikipedia.org/wiki/Key-value_database), a "bucket" is a collection of unique keys that are associated with values. A buckets database is a set of buckets.  The underlying datastore is represented by a single file on disk.  

Note that buckets is just an extension of Bolt, providing a `Bucket` type with some nifty convenience methods for operating on the items (key/value pairs) within instances of it.  It streamlines simple transactions (a single put, get, or delete) and working with subsets of items within a bucket (via prefix and range scans). 

For example, here's how you put an item in a bucket and get it back out. (Note we're omitting proper error handling here.)

```go
// Open a buckets database.
bx, _ := buckets.Open("data.db")
defer bx.Close()

// Create a new `things` bucket.
things, _ := bx.New([]byte("things"))

// Put key/value into the `things` bucket.
key, value := []byte("A"), []byte("alpha")
things.Put(key, value)

// Read value back in a different read-only transaction.
got, _ := things.Get(key)

fmt.Printf("The value of %q in `things` is %q\n", key, got)
```

Output:

    The value of "A" in `things` is "alpha"


## Overview

As noted above, buckets is a wrapper for Bolt, streamlining [basic transactions](https://github.com/boltdb/bolt#transactions).  If you're unfamiliar with Bolt, check out the [README](https://github.com/boltdb/bolt#resources) and [intro articles](https://github.com/boltdb/bolt#resources).

A buckets/bolt database contains a set of buckets.  What's a bucket?  It's basically just an [associative array](https://en.wikipedia.org/wiki/Associative_array), mapping keys to values.  For simplicity, we say that a bucket *contains* key/values pairs and we refer to these k/v pairs as "items".  You use buckets for storing and retrieving such items.

Since Bolt stores keys in [byte-sorted order](https://github.com/boltdb/bolt#iterating-over-keys), we can take advantage of this sorted key namespace for fast prefix and range scanning of keys.  In particular, it gives us a way to easily retrieve a subset of items. (See the `PrefixItems` and `RangeItems` methods, described below.)


#### Read/write transactions

* [`Put(k, v)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.Put) - save/update item
* [`PutNX(k, v)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.Put) - save item if key does not exist
* [`Delete(k)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.Delete) - delete item
* [`Insert(items)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.Insert) - save/update items (k/v pairs)
* [`InsertNX(items)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.Insert) - for each item (k/v pair), save item if key does not exist


#### Read-only transactions

* [`Get(k)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.Get) - get value
* [`Items()`](https://godoc.org/github.com/joyrexus/buckets#Bucket.Items) - get list of items (k/v pairs)
* [`PrefixItems(pre)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.PrefixItems) - get list of items with key prefix
* [`RangeItems(min, max)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.RangeItems) - get list of items within key range
* [`Map(func)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.Map) - apply func to each item
* [`MapPrefix(func, pre)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.MapPrefix) - apply func to each item with key prefix
* [`MapRange(func, min, max)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.MapRange) - apply a func to each item within key range


## Getting Started

Use `go get github.com/joyrexus/buckets` to install and see the [docs](https://godoc.org/github.com/joyrexus/buckets) for details.

To open a database, use `buckets.Open()`:

```go
package main

import (
    "log"

    "github.com/joyrexus/buckets"
)

func main() {
    bx, err := buckets.Open("my.db")
    if err != nil {
        log.Fatal(err)
    }
    defer bx.Close()

    ...
}
```

Note that buckets obtains a file lock on the data file so multiple processes cannot open the same database at the same time.


## Examples

The docs contain numerous [examples](https://godoc.org/github.com/joyrexus/buckets#pkg-examples) demonstrating basic usage.

See also the [examples](examples) directory for standalone examples, demonstrating use of buckets for persistence in a web service context.
