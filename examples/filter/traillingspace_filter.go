package main

// a custom Filter that ignore the trailing space

/*
#cgo LDFLAGS: -lrocksdb -lstdc++ -lbz2 -lz -lsnappy -lrt
#include <string.h>
#include <stdlib.h>
#include <rocksdb/c.h>
void customFilterDestroy(void* arg) { }

char* removeTrailingSpaces(const char* key, size_t length, size_t *trimmedLength) {
	int i;
	for (i = length -1 ; i >= 0; i--) {
		if (key[i] != ' ') break;
	}
	if (i == -1) {
		return NULL;
	}
	if (trimmedLength != NULL) {
		*trimmedLength = i +1;
	}
	return strndup(key, i+1);
}

char* customCreateFilter(void *arg,
		const char* const* key_array,
		const size_t* key_length_array,
        int num_keys,
        size_t* filter_length) {
	int i;
	char** trimmed_key_array = (char**)malloc(sizeof(char *) * num_keys);
	size_t* trimmed_key_length_array = malloc(sizeof(size_t) * num_keys);
	for (i = 0; i < num_keys; i++) {
		trimmed_key_array[i] = removeTrailingSpaces(key_array[i],
			key_length_array[i], &trimmed_key_length_array[i]);
	}

}

unsigned char customKeyMatch (
        void* arg,
        const char* key, size_t length,
        const char* filter, size_t filter_length) {
    size_t trimmedLength;
    char* trimmedKey = removeTrailingSpaces(key, length, &trimmedLength);
	return (*defaultBloomFilter->key_match_)(arg, trimmedKey, trimmedLength,
							filter, filter_length);
}

const char* customFilterName(void* arg) {
	return "foo";
}

leveldb_filterpolicy_t *CustomeFilterNew() {
	return leveldb_filterpolicy_create(NULL, customFilterDestroy,
		customCreateFilter, customKeyMatch, customFilterName);
}
*/
import "C"

type FilterPolicy struct {
	Policy *C.leveldb_filterpolicy_t
}

func NewFooPolicy() *FilterPolicy {
	C.initDefaultBloomFilter()
	return &FilterPolicy{C.CustomeFilterNew()}
}

func (policy *FilterPolicy) Close() {
	C.leveldb_filterpolicy_destroy(policy.Policy)
}

func main() {
	NewFooComparator().Close()
}
