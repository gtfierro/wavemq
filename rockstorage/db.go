package rocksdb

// #cgo CXXFLAGS: -I./include/ -std=gnu++11
// #cgo LDFLAGS: -L./lib -lrocksdb -ldl -lrt -lsnappy -lgflags -lz -lbz2 -llz4 -lzstd
// #include "iface.h"
import "C"
import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"unsafe"
)

type Column int

const (
	QUEUE   Column = 1
	PERSIST Column = 2
)

var initOnce sync.Once

var ErrObjNotFound = errors.New("Object Not Found")

type RocksdbErr struct {
	message string
}

func (e *RocksdbErr) Error() string {
	if e == nil {
		return ""
	}
	return e.message
}

func getError(errstr *C.char, errlen C.size_t) error {
	var msg = make([]byte, errlen)
	if errlen > 0 {
		C.memcpy(unsafe.Pointer(&msg[0]), unsafe.Pointer(errstr), errlen)
		C.free(unsafe.Pointer(errstr))
		return &RocksdbErr{
			message: fmt.Sprintf("%s", string(msg)),
		}
	}
	return nil
}

func Initialize(conf StorageConfig) {
	initOnce.Do(func() {
		dbname := conf.DataStore
		spinning_metal := conf.OptimizeForSpinningMetal
		opt_for_spin := 0
		if spinning_metal {
			opt_for_spin = 1
		}
		name := []byte(dbname)
		C.c_init((*C.char)(unsafe.Pointer(&name[0])), (C.size_t)(len(name)), (C.size_t)(opt_for_spin))
	})
}

func Close() {
	C.close_db()
}

func db_get(col Column, key []byte) ([]byte, error) {
	var ln C.size_t
	val := C.db_get(C.int(col), (*C.char)(unsafe.Pointer(&key[0])),
		(C.size_t)(len(key)),
		&ln)
	if val == nil {
		return nil, ErrObjNotFound
	}
	rv := make([]byte, int(ln))
	C.memcpy(unsafe.Pointer(&rv[0]), unsafe.Pointer(val), ln)
	C.free(unsafe.Pointer(val))
	return rv, nil
}

func db_set(col Column, key, value []byte) error {
	var errstr *C.char
	var errlen C.size_t
	C.db_set(C.int(col), (*C.char)(unsafe.Pointer(&key[0])), (C.size_t)(len(key)),
		(*C.char)(unsafe.Pointer(&value[0])), (C.size_t)(len(value)),
		&errstr, &errlen)
	return getError(errstr, errlen)
}

func db_delete(col Column, key []byte) error {
	var errstr *C.char
	var errlen C.size_t
	C.db_delete(C.int(col), (*C.char)(unsafe.Pointer(&key[0])), (C.size_t)(len(key)), &errstr, &errlen)
	return getError(errstr, errlen)
}

func db_delete_prefix(col Column, pfx []byte) int {
	val := C.db_delete_prefix(C.int(col), (*C.char)(unsafe.Pointer(&pfx[0])), (C.size_t)(len(pfx)))
	return int(val)
}

func QueueGet(key []byte) ([]byte, error) {
	return db_get(QUEUE, key)
}

func PersistGet(key []byte) ([]byte, error) {
	return db_get(PERSIST, key)
}

func QueueSet(key, value []byte) error {
	return db_set(QUEUE, key, value)
}

func PersistSet(key, value []byte) error {
	return db_set(PERSIST, key, value)
}

func QueueDelete(key []byte) error {
	return db_delete(QUEUE, key)
}

func PersistDelete(key []byte) error {
	return db_delete(PERSIST, key)
}

func QueueDeletePrefix(key []byte) int {
	return db_delete_prefix(QUEUE, key)
}

func PersistDeletePrefix(key []byte) int {
	return db_delete_prefix(QUEUE, key)
}

type Iterator struct {
	state         unsafe.Pointer
	prefix        []byte
	current_value []byte
	current_key   []byte
	valid         bool
}

func NewIterator(col Column, prefix []byte) *Iterator {
	var (
		key      *C.char
		keylen   C.size_t
		value    *C.char
		valuelen C.size_t
	)
	it := Iterator{prefix: prefix}
	C.db_it_start(C.int(col), &it.state, (*C.char)(unsafe.Pointer(&prefix[0])), (C.size_t)(len(prefix)), &key, &keylen, &value, &valuelen)
	runtime.SetFinalizer(&it, func(it *Iterator) {
		// from bw2 rocks
		//I have no idea how long rocks will take to do this. I suspect
		//it involves deleting a snapshot. Lets not block the finalizer
		//goroutine
		go func() {
			fmt.Println("finalize iterator")
			C.db_it_delete(it.state)
		}()
	})
	if keylen == 0 && valuelen == 0 {
		it.valid = false
		return &it
	}
	it.valid = true
	it.current_key = make([]byte, keylen)
	it.current_value = make([]byte, valuelen)
	C.memcpy(unsafe.Pointer(&it.current_key[0]), unsafe.Pointer(key), keylen)
	C.memcpy(unsafe.Pointer(&it.current_value[0]), unsafe.Pointer(value), valuelen)
	return &it
}

func (it *Iterator) Next() {
	var (
		key      *C.char
		keylen   C.size_t
		value    *C.char
		valuelen C.size_t
	)
	C.db_it_next(it.state, (*C.char)(unsafe.Pointer(&it.prefix[0])), (C.size_t)(len(it.prefix)), &key, &keylen, &value, &valuelen)
	if keylen == 0 && valuelen == 0 {
		it.valid = false
		return
	}
	it.current_key = make([]byte, keylen)
	it.current_value = make([]byte, valuelen)
	C.memcpy(unsafe.Pointer(&it.current_key[0]), unsafe.Pointer(key), keylen)
	C.memcpy(unsafe.Pointer(&it.current_value[0]), unsafe.Pointer(value), valuelen)
	//TODO: free key/value ?
	it.valid = true
}

func (it *Iterator) HasNext() bool {
	return it.valid
}

func (it *Iterator) Key() []byte {
	return it.current_key
}
func (it *Iterator) Value() []byte {
	return it.current_value
}

type StorageConfig struct {
	// if true, optimize rocksdb for spinning metal
	OptimizeForSpinningMetal bool
	// file path of rocksdb for queue and persist
	DataStore string
}

type WriteBatch struct {
	state unsafe.Pointer
	col   Column
}

func NewWriteBatch(col Column) *WriteBatch {
	wb := &WriteBatch{
		col: col,
	}
	C.db_wb(&wb.state)
	return wb
}

func (wb *WriteBatch) Set(key, value []byte) {
	C.db_wb_set(C.int(wb.col), wb.state, (*C.char)(unsafe.Pointer(&key[0])), (C.size_t)(len(key)), (*C.char)(unsafe.Pointer(&value[0])), (C.size_t)(len(value)))
}

func (wb *WriteBatch) Commit() error {
	var errstr *C.char
	var errlen C.size_t
	C.db_wb_commit(C.int(wb.col), wb.state, &errstr, &errlen)
	return getError(errstr, errlen)
}
