package ratgo

// slice hold the data from rocksdb, and finnalize it in the gc.
// it can help us to avoid data copy.

/*
#cgo LDFLAGS: -lrocksdb
#include "rocksdb/c.h"
*/
import "C"

import (
	"reflect"
	"runtime"
	"unsafe"
)

type Slice struct {
	Data []byte
	data *c_slice_t
}

type c_slice_t struct {
	p unsafe.Pointer
	n int
}

// in iter case we don't need to free the data by our own
func newSlice(p unsafe.Pointer, n int, recycle bool) *Slice {
	data := &c_slice_t{p, n}
	if recycle {
		runtime.SetFinalizer(data, func(data *c_slice_t) {
			C.leveldb_free(data.p)
		})
	}
	s := &Slice{data: data}
	h := (*reflect.SliceHeader)((unsafe.Pointer)(&s.Data))
	h.Cap = n
	h.Len = n
	h.Data = uintptr(p)
	return s
}
