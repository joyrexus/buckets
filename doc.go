/*

Package buckets provides a simplified interface to a Bolt database.

A buckets DB is a Bolt database, but it allows you to easily create new bucket instances.  The database is represented by a single file on disk.  A bucket is a collection of unique keys that are associated with values.

The Bucket type has nifty convenience methods for operating on key/value pairs within it.  It streamlines simple transactions (a single put, get, or delete) and working with subsets of items within a bucket (via prefix and range scans).  It's not designed to handle more complex or batch transactions. For such cases, use the standard techniques offered by Bolt.

---

What is bolt?

"Bolt implements a low-level key/value store in pure Go. It supports
fully serializable transactions, ACID semantics, and lock-free MVCC with
multiple readers and a single writer. Bolt can be used for projects that
want a simple data store without the need to add large dependencies such as
Postgres or MySQL."

See https://github.com/boltdb/bolt for important details.

*/
package buckets
