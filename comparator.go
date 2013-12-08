package ratgo

// #cgo LDFLAGS: -lrocksdb -lrt
// #include "rocksdb/c.h"
import "C"

// Comparator must be used in C in your own library

// DestroyComparator deallocates a *C.leveldb_comparator_t.
//
// This is provided as a convienience to advanced users that have implemented
// their own comparators in C in their own code.
func DestroyComparator(cmp *C.leveldb_comparator_t) {
	C.leveldb_comparator_destroy(cmp)
}
