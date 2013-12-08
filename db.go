package ratgo

/*
#cgo LDFLAGS: -lrocksdb -lrt
#include <stdlib.h>
#include "rocksdb/c.h"
*/
import "C"

import (
	"unsafe"
)

type DatabaseError string

func (e DatabaseError) Error() string {
	return string(e)
}

// DB is a reusable handle to a LevelDB database on disk, created by Open.
//
// To avoid memory and file descriptor leaks, call Close when the process no
// longer needs the handle. Calls to any DB method made after Close will
// panic.
//
// The DB instance may be shared between goroutines. The usual data race
// conditions will occur if the same key is written to from more than one, of
// course.
type DB struct {
	RocksDb *C.leveldb_t
}

// Range is a range of keys in the database. GetApproximateSizes calls with it
// begin at the key Start and end right before the key Limit.
type Range struct {
	Start []byte
	Limit []byte
}

// Snapshot provides a consistent view of read operations in a DB. It is set
// on to a ReadOptions and passed in. It is only created by DB.NewSnapshot.
//
// To prevent memory leaks and resource strain in the database, the snapshot
// returned must be released with DB.ReleaseSnapshot method on the DB that
// created it.
type Snapshot struct {
	snap *C.leveldb_snapshot_t
}

// Open opens a database.
//
// Creating a new database is done by calling SetCreateIfMissing(true) on the
// Options passed to Open.
//
// It is usually wise to set a Cache object on the Options with SetCache to
// keep recently used data from that database in memory.
func Open(dbName string, o *Options) (*DB, error) {
	var errStr *C.char
	rocksDbName := C.CString(dbName)
	defer C.free(unsafe.Pointer(rocksDbName))

	rocksdb := C.leveldb_open(o.Opt, rocksDbName, &errStr)
	if errStr != nil {
		gs := C.GoString(errStr)
		C.free(unsafe.Pointer(errStr))
		return nil, DatabaseError(gs)
	}
	return &DB{rocksdb}, nil
}

// DestroyDatabase removes a database entirely, removing everything from the
// filesystem.
func DestroyDatabase(dbname string, o *Options) error {
	var errStr *C.char
	ldbname := C.CString(dbname)
	defer C.free(unsafe.Pointer(ldbname))

	C.leveldb_destroy_db(o.Opt, ldbname, &errStr)
	if errStr != nil {
		gs := C.GoString(errStr)
		C.free(unsafe.Pointer(errStr))
		return DatabaseError(gs)
	}
	return nil
}

// Put writes data associated with a key to the database.
//
// If a nil []byte is passed in as value, it will be returned by Get as an
// zero-length slice.
//
// The key and value byte slices may be reused safely. Put takes a copy of
// them before returning.
func (db *DB) Put(wo *WriteOptions, key, value []byte) error {
	var errStr *C.char
	var k, v *C.char
	if len(key) != 0 {
		k = (*C.char)(unsafe.Pointer(&key[0]))
	}
	if len(value) != 0 {
		v = (*C.char)(unsafe.Pointer(&value[0]))
	}

	lenk := len(key)
	lenv := len(value)
	C.leveldb_put(
		db.RocksDb, wo.Opt, k, C.size_t(lenk), v, C.size_t(lenv), &errStr)

	if errStr != nil {
		gs := C.GoString(errStr)
		C.free(unsafe.Pointer(errStr))
		return DatabaseError(gs)
	}
	return nil
}

// Get returns the data associated with the key from the database.
//
// If the key does not exist in the database, a nil []byte is returned. If the
// key does exist, but the data is zero-length in the database, a zero-length
// []byte will be returned.
//
// The key byte slice may be reused safely. Get takes a copy of
// them before returning.
func (db *DB) Get(ro *ReadOptions, key []byte) ([]byte, error) {
	var errStr *C.char
	var vallen C.size_t
	var k *C.char
	if len(key) != 0 {
		k = (*C.char)(unsafe.Pointer(&key[0]))
	}

	value := C.leveldb_get(
		db.RocksDb, ro.Opt, k, C.size_t(len(key)), &vallen, &errStr)

	if errStr != nil {
		gs := C.GoString(errStr)
		C.free(unsafe.Pointer(errStr))
		return nil, DatabaseError(gs)
	}

	if value == nil {
		return nil, nil
	}

	return C.GoBytes(unsafe.Pointer(value), C.int(vallen)), nil
}

// Delete removes the data associated with the key from the database.
//
// The key byte slice may be reused safely. Delete takes a copy of
// them before returning.
func (db *DB) Delete(wo *WriteOptions, key []byte) error {
	var errStr *C.char
	var k *C.char
	if len(key) != 0 {
		k = (*C.char)(unsafe.Pointer(&key[0]))
	}

	C.leveldb_delete(
		db.RocksDb, wo.Opt, k, C.size_t(len(key)), &errStr)

	if errStr != nil {
		gs := C.GoString(errStr)
		C.free(unsafe.Pointer(errStr))
		return DatabaseError(gs)
	}
	return nil
}
