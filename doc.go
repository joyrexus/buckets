/*

Package buckets provides a simplified interface to a bolt database.

A buckets DB is a bolt database, but it allows you to easily create new bucket instances.  The database is represented by a single file on disk.  A bucket is a collection of unique keys that are associated with values. The Bucket type has nifty convenience methods for operating on key/value pairs within it.

What is bolt?

"Bolt implements a low-level key/value store in pure Go. It supports
fully serializable transactions, ACID semantics, and lock-free MVCC with
multiple readers and a single writer. Bolt can be used for projects that
want a simple data store without the need to add large dependencies such as
Postgres or MySQL.

"Bolt is a single-level, zero-copy, B+tree data store. This means that Bolt is
optimized for fast read access and does not require recovery in the event of a
system crash. Transactions which have not finished committing will simply be
rolled back in the event of a crash."

See https://github.com/boltdb/bolt for important details.

*/
package buckets
