package main

// a custom Filter that ignore the trailing space

/*
#cgo LDFLAGS: -lrocksdb -lstdc++ -lbz2 -lz -lsnappy -lrt
#include <string.h>
#include <stdlib.h>
#include <rocksdb/c.h>

static unsigned char fake_filter_result = 1;

static void customFilterDestroy(void* arg) { }

static char* customCreateFilter(void *arg,
		const char* const* key_array,
		const size_t* key_length_array,
        int num_keys,
        size_t* filter_length) {
	*filter_length = 4;
	char* result = malloc(4);
	memcpy(result, "test", 4);
	return result;
}

static unsigned char customKeyMatch (
        void* arg,
        const char* key, size_t length,
        const char* filter, size_t filter_length) {
    if (filter_length != 4) {
		return 0;
    }
    if (memcmp(filter, "test", 4) != 0 ) {
		return 0;
    }
    return fake_filter_result;
}

static const char* customFilterName(void* arg) {
	return "foo";
}

static leveldb_filterpolicy_t *CustomeFilterNew() {
	return leveldb_filterpolicy_create(NULL, customFilterDestroy,
		customCreateFilter, customKeyMatch, customFilterName);
}
*/
import "C"

type FilterPolicy struct {
	Policy *C.leveldb_filterpolicy_t
}

func NewFooPolicy() *FilterPolicy {
	return &FilterPolicy{C.CustomeFilterNew()}
}

func (policy *FilterPolicy) Close() {
	C.leveldb_filterpolicy_destroy(policy.Policy)
}

func main() {
	NewFooPolicy().Close()
}
