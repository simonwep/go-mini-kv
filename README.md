### Go Mini Key-Value Database

[![Go](https://github.com/simonwep/go-mini-kv/actions/workflows/main.yml/badge.svg)](https://github.com/simonwep/go-mini-kv/actions/workflows/main.yml)

A miniature key-value database written in go, functions implemented so far include: 

* `Open(loc tring)` - _Opens a new database, files are stored under the folder specified via `loc`._
* `Set(key []byte, value []byte) error` - _Sets a new value, the value is immediately written to the database._
* `Get(key []byte) ([]byte, error)` - _Retrieves a value from the database._
* `Stat() (*DataBaseStats, error)` - _Returns statistical information about the current database, such as the size and amount of entries._
* `RunGC() error` - _Runs the garbage collector, compressing both the dictionary and the data file._

Things that are not yet implemented:

* In-memory mode.
* Transactions.
* Backups.

...and many possible performance improvements.
