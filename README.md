# buckets 

[![GoDoc](https://godoc.org/github.com/joyrexus/buckets?status.svg)](https://godoc.org/github.com/joyrexus/buckets)

A simple key/value store based on [Bolt](https://github.com/boltdb/bolt). 

![buckets](buckets.jpg)

In the parlance of [key/value stores](https://en.wikipedia.org/wiki/Key-value_database), a "bucket" is a collection of unique keys that are associated with values. A buckets database is a set of buckets.  The underlying datastore is represented by a single file on disk.  

Note that buckets is just an extension of Bolt, providing a `Bucket` type with some nifty convenience methods for operating on the items (key/value pairs) within instances of it.  It streamlines simple transactions (a single put, get, or delete) and working with subsets of items within a bucket (via prefix and range scans).

Use `go get github.com/joyrexus/buckets` to install and see the [docs](https://godoc.org/github.com/joyrexus/buckets) for details.


## Overview

As noted above, buckets is a wrapper for Bolt, streamlining basic transactions.

A buckets/bolt database contains a set of buckets.  What's a bucket?  It's basically just an [associate array](https://en.wikipedia.org/wiki/Associative_array), mapping keys to values.  For simplicity, we say that a bucket *contains* key/values pairs and we refer to these k/v pairs as "items".  You use buckets for storing and retrieving such items.

Since Bolt stores keys in byte-sorted order, we can take advantage of this
sorted key namespace with fast prefix and range scanning of keys.


#### Read/write transactions

* [`Put(k, v)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.Put) - save item
* [`Delete(k)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.Delete) - delete item
* [`Insert(items)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.Insert) - save items (k/v pairs)


#### Read-only transactions

* [`Get(k)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.Get) - get value
* [`Items()`](https://godoc.org/github.com/joyrexus/buckets#Bucket.Items) - get list of items (k/v pairs)
* [`PrefixItems(pre)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.PrefixItems) - get list of items with key prefix
* [`RangeItems(min, max)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.RangeItems) - get list of items within key range
* [`Map(func)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.Map) - apply func to each item
* [`MapPrefix(func, pre)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.MapPrefix) - apply func to each item with key prefix
* [`MapRange(func, min, max)`](https://godoc.org/github.com/joyrexus/buckets#Bucket.MapRange) - apply a func to each item within key range
