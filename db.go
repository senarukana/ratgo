package ratgo

/*
#cgo LDFLAGS: -lrocksdb -lrt
#include <stdlib.h>
#include "rocksdb/c.h"

// This function exists only to clean up lack-of-const warnings when
// leveldb_approximate_sizes is called from Go-lang.
void levigo_leveldb_approximate_sizes(
    leveldb_t* db,
    int num_ranges,
    char** range_start_key, const size_t* range_start_key_len,
    char** range_limit_key, const size_t* range_limit_key_len,
    uint64_t* sizes) {
  leveldb_approximate_sizes(db,
                            num_ranges,
                            (const char* const*)range_start_key,
                            range_start_key_len,
                            (const char* const*)range_limit_key,
                            range_limit_key_len,
                            sizes);
}

void ratgo_leveldb_multi_get(
    leveldb_t* db,
    leveldb_readoptions_t* options,
    int key_num,
    char** key_array,
    size_t* key_array_length,
    char*** value_array,
    size_t** value_array_length,
    char*** errsptr) {
  leveldb_multi_get(db,
  					options,
  					key_num,
  					(const char* const *)key_array,
  					key_array_length,
  					value_array,
  					value_array_length,
  					errsptr);
}


// According to the answer of :https://groups.google.com/forum/#!msg/golang-nuts/6toTzvJbyIs/sLQF6NLn-wIJ
// There is no pointer arithmetic in Go.
// this function gives an easy approach to find char* from char**
char* get_list_at(char **list, int idx){
        return list[idx];
}

size_t get_list_int_at(size_t *list, int idx){
        return list[idx];
}

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
		C.leveldb_free(unsafe.Pointer(errStr))
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
		C.leveldb_free(unsafe.Pointer(errStr))
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
		C.leveldb_free(unsafe.Pointer(errStr))
		return DatabaseError(gs)
	}
	return nil
}

// Get returns the data associated with the key from the database.
//
// Returned slice is a wrapper for bytes read from rocksdb.
// If the key does not exist in the database, a nil Slice is returned. If the
// key does exist, but the data is zero-length in the database, a zero-length
// []byte is the Data of slice.
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
		C.leveldb_free(unsafe.Pointer(errStr))
		return nil, DatabaseError(gs)
	}

	if value == nil {
		return nil, nil
	}

	defer C.leveldb_free(unsafe.Pointer(value))
	return C.GoBytes(unsafe.Pointer(value), C.int(vallen)), nil
}

// MultiGet returns the data associated with multiple keys from the database.
//
// Returned slice is a wrapper for bytes read from rocksdb.
// If the key does not exist in the database, a nil Slice is returned. If the
// key does exist, but the data is zero-length in the database, a zero-length
// []byte is the Data of slice.
//
// The key byte slice may be reused safely. Get takes a copy of
// them before returning.
func (db *DB) MultiGet(ro *ReadOptions, keys [][]byte) (returnValues [][]byte, returnErrors []error) {
	var errsStr **C.char
	var valueArray **C.char
	var valueLengthArray *C.size_t
	num := len(keys)
	keyArray := make([]*C.char, num)
	keyLengthArray := make([]C.size_t, num)

	returnValues = make([][]byte, len(keys))
	returnErrors = make([]error, len(keys))

	for i, key := range keys {
		keyArray[i] = (*C.char)(unsafe.Pointer(&key[0]))
		// keyArray[i] = C.CString(string(key))
		keyLengthArray[i] = C.size_t(len(key))
	}

	keyArrayPtr := &keyArray[0]
	keyLengthArrayPtr := &keyLengthArray[0]

	C.leveldb_multi_get(
		db.RocksDb, ro.Opt, C.int(len(keys)),
		keyArrayPtr, keyLengthArrayPtr,
		&valueArray, &valueLengthArray, &errsStr)
	for i := 0; i < num; i++ {
		errStr := C.get_list_at(errsStr, C.int(i))
		if errStr != nil {
			returnErrors[i] = DatabaseError(C.GoString(errStr))
			C.leveldb_free(unsafe.Pointer(errStr))
		} else {
			value := C.get_list_at(valueArray, C.int(i))
			valueLength := C.get_list_int_at(valueLengthArray, C.int(i))
			if value == nil {
				returnValues[i] = nil
			} else {
				returnValues[i] = C.GoBytes(unsafe.Pointer(value), C.int(valueLength))
			}
			C.leveldb_free(unsafe.Pointer(value))
		}
	}
	C.leveldb_free(unsafe.Pointer(valueLengthArray))
	return
}

// Flush flush all the data in memtable to disk
//
// If FlushOptions wait is true, the flush will wait until the flush is done.
// Default: true
func (db *DB) Flush(fo *FlushOptions) error {
	var errStr *C.char
	C.leveldb_flush(db.RocksDb, fo.Opt, &errStr)
	if errStr != nil {
		gs := C.GoString(errStr)
		C.leveldb_free(unsafe.Pointer(errStr))
		return DatabaseError(gs)
	}
	return nil
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
		C.leveldb_free(unsafe.Pointer(errStr))
		return DatabaseError(gs)
	}
	return nil
}

func (db *DB) Merge(wo *WriteOptions, key []byte, value []byte) error {
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
	C.leveldb_merge(
		db.RocksDb, wo.Opt, k, C.size_t(lenk), v, C.size_t(lenv), &errStr)

	if errStr != nil {
		gs := C.GoString(errStr)
		C.leveldb_free(unsafe.Pointer(errStr))
		return DatabaseError(gs)
	}
	return nil
}

// Write atomically writes a WriteBatch to disk.
func (db *DB) Write(wo *WriteOptions, w *WriteBatch) error {
	var errStr *C.char
	C.leveldb_write(db.RocksDb, wo.Opt, w.wbatch, &errStr)
	if errStr != nil {
		gs := C.GoString(errStr)
		C.leveldb_free(unsafe.Pointer(errStr))
		return DatabaseError(gs)
	}
	return nil
}

// NewIterator returns an Iterator over the the database that uses the
// ReadOptions given.
//
// Often, this is used for large, offline bulk reads while serving live
// traffic. In that case, it may be wise to disable caching so that the data
// processed by the returned Iterator does not displace the already cached
// data. This can be done by calling SetFillCache(false) on the ReadOptions
// before passing it here.
//
// Similiarly, ReadOptions.SetSnapshot is also useful.
func (db *DB) NewIterator(ro *ReadOptions) *Iterator {
	it := C.leveldb_create_iterator(db.RocksDb, ro.Opt)
	return &Iterator{Iter: it}
}

// GetApproximateSizes returns the approximate number of bytes of file system
// space used by one or more key ranges.
//
// The keys counted will begin at Range.Start and end on the key before
// Range.Limit.
func (db *DB) GetApproximateSizes(ranges []Range) []uint64 {
	starts := make([]*C.char, len(ranges))
	limits := make([]*C.char, len(ranges))
	startLens := make([]C.size_t, len(ranges))
	limitLens := make([]C.size_t, len(ranges))
	for i, r := range ranges {
		starts[i] = C.CString(string(r.Start))
		startLens[i] = C.size_t(len(r.Start))
		limits[i] = C.CString(string(r.Limit))
		limitLens[i] = C.size_t(len(r.Limit))
	}
	sizes := make([]uint64, len(ranges))
	numranges := C.int(len(ranges))
	startsPtr := &starts[0]
	limitsPtr := &limits[0]
	startLensPtr := &startLens[0]
	limitLensPtr := &limitLens[0]
	sizesPtr := (*C.uint64_t)(&sizes[0])
	C.levigo_leveldb_approximate_sizes(
		db.RocksDb, numranges, startsPtr, startLensPtr,
		limitsPtr, limitLensPtr, sizesPtr)
	for i := range ranges {
		C.free(unsafe.Pointer(starts[i]))
		C.free(unsafe.Pointer(limits[i]))
	}
	return sizes
}

// PropertyValue returns the value of a database property.
//
// Examples of properties include "leveldb.stats", "leveldb.sstables",
// and "leveldb.num-files-at-level0".
func (db *DB) PropertyValue(propName string) string {
	cname := C.CString(propName)
	value := C.GoString(C.leveldb_property_value(db.RocksDb, cname))
	C.free(unsafe.Pointer(cname))
	return value
}

// NewSnapshot creates a new snapshot of the database.
//
// The snapshot, when used in a ReadOptions, provides a consistent view of
// state of the database at the the snapshot was created.
//
// To prevent memory leaks and resource strain in the database, the snapshot
// returned must be released with DB.ReleaseSnapshot method on the DB that
// created it.
//
// See the LevelDB documentation for details.
func (db *DB) NewSnapshot() *Snapshot {
	return &Snapshot{C.leveldb_create_snapshot(db.RocksDb)}
}

// ReleaseSnapshot removes the snapshot from the database's list of snapshots,
// and deallocates it.
func (db *DB) ReleaseSnapshot(snap *Snapshot) {
	C.leveldb_release_snapshot(db.RocksDb, snap.snap)
}

// CompactRange runs a manual compaction on the Range of keys given. This is
// not likely to be needed for typical usage.
func (db *DB) CompactRange(r Range) {
	var start, limit *C.char
	if len(r.Start) != 0 {
		start = (*C.char)(unsafe.Pointer(&r.Start[0]))
	}
	if len(r.Limit) != 0 {
		limit = (*C.char)(unsafe.Pointer(&r.Limit[0]))
	}
	C.leveldb_compact_range(
		db.RocksDb, start, C.size_t(len(r.Start)), limit, C.size_t(len(r.Limit)))
}

// Close closes the database, rendering it unusable for I/O, by deallocating
// the underlying handle.
//
// Any attempts to use the DB after Close is called will panic.
func (db *DB) Close() {
	C.leveldb_close(db.RocksDb)
}

// DisableFiledeleteltions instructs RocksDB to not delete data files.
// Compactions will continue to occur, but files that are not needed by the database will not be deleted.
func (db *DB) DisableFileDeletions() {
	C.leveldb_disable_file_deletions(db.RocksDb)
}

// EnableFileDeletions instructs RocksDB to the delete data again.
func (db *DB) EnableFileDeletions() {
	C.leveldb_enable_file_deletions(db.RocksDb)
}

// Get live files return the current db data files (include manifest file) and manifestFileSize
//
// flushMemtable indicates whether or not flush the memtable to disk.
// When use this operation, you'd better first issue DisableFileDeletions.
func (db *DB) GetLiveFiles(flushMemtable bool) (files []string, manifestFileSize int, err error) {
	var errStr *C.char
	var fileArray **C.char
	var fileLengthArray *C.size_t
	var fileNum C.int
	var retManifestSize C.uint64_t

	C.leveldb_get_live_files(db.RocksDb, &fileArray, &fileLengthArray, &fileNum, &retManifestSize, boolToUchar(flushMemtable), &errStr)
	if errStr != nil {
		gs := C.GoString(errStr)
		C.leveldb_free(unsafe.Pointer(errStr))
		err = DatabaseError(gs)
		return
	}
	for i := 0; i < int(fileNum); i++ {
		fileLength := C.get_list_int_at(fileLengthArray, C.int(i))
		file := C.get_list_at(fileArray, C.int(i))
		files = append(files, C.GoStringN(file, C.int(fileLength)))
		C.leveldb_free(unsafe.Pointer(file))
	}
	manifestFileSize = int(retManifestSize)
	return
}
