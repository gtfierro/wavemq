package rocksdb

// #cgo CXXFLAGS: -I./include/ -std=gnu++11
// #cgo LDFLAGS: -L./lib -lrocksdb -ldl -lpthread -lrt -lsnappy -lgflags -lz -lbz2 -llz4 -lzstd
// #include "iface.h"
import "C"
import (
	"errors"
	"fmt"
	"runtime"
	"unsafe"
)

var didInit bool = false

var ErrObjNotFound = errors.New("Object Not Found")

func Initialize(dbname string, spinning_metal bool) {
	if didInit {
		return
	}
	opt_for_spin := 0
	if spinning_metal {
		opt_for_spin = 1
	}
	name := []byte(dbname)
	C.c_init((*C.char)(unsafe.Pointer(&name[0])), (C.size_t)(len(name)), (C.size_t)(opt_for_spin))
	didInit = true
}

func Close() {
	C.close()
}

func QueueGet(key []byte) ([]byte, error) {
	var ln C.size_t
	val := C.queue_get((*C.char)(unsafe.Pointer(&key[0])),
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

func QueueSet(key, value []byte) {
	C.queue_set((*C.char)(unsafe.Pointer(&key[0])), (C.size_t)(len(key)),
		(*C.char)(unsafe.Pointer(&value[0])), (C.size_t)(len(value)))
}

func QueueDelete(key []byte) {
	C.queue_delete((*C.char)(unsafe.Pointer(&key[0])), (C.size_t)(len(key)))
}

func QueueDeletePrefix(key []byte) int {
	val := C.queue_delete_prefix((*C.char)(unsafe.Pointer(&key[0])), (C.size_t)(len(key)))
	return int(val)
}

type Iterator struct {
	state         unsafe.Pointer
	prefix        []byte
	current_value []byte
	current_key   []byte
	valid         bool
}

func NewIterator(prefix []byte) *Iterator {
	var (
		key      *C.char
		keylen   C.size_t
		value    *C.char
		valuelen C.size_t
	)
	it := Iterator{prefix: prefix}
	C.queue_it_start(&it.state, (*C.char)(unsafe.Pointer(&prefix[0])), (C.size_t)(len(prefix)), &key, &keylen, &value, &valuelen)
	runtime.SetFinalizer(&it, func(it *Iterator) {
		//I have no idea how long rocks will take to do this. I suspect
		//it involves deleting a snapshot. Lets not block the finalizer
		//goroutine
		go func() {
			fmt.Println("finalize iterator")
			C.queue_it_delete(it.state)
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
	C.queue_it_next(it.state, (*C.char)(unsafe.Pointer(&it.prefix[0])), (C.size_t)(len(it.prefix)), &key, &keylen, &value, &valuelen)
	if keylen == 0 && valuelen == 0 {
		it.valid = false
		return
	}
	it.current_key = make([]byte, keylen)
	it.current_value = make([]byte, valuelen)
	C.memcpy(unsafe.Pointer(&it.current_key[0]), unsafe.Pointer(key), keylen)
	C.memcpy(unsafe.Pointer(&it.current_value[0]), unsafe.Pointer(value), valuelen)
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

type WriteBatch struct {
	state unsafe.Pointer
}

func NewWriteBatch() *WriteBatch {
	wb := WriteBatch{}
	C.queue_wb_start(&wb.state)
	runtime.SetFinalizer(&wb, func(wb *WriteBatch) {
		//I have no idea how long rocks will take to do this. I suspect
		//it involves deleting a snapshot. Lets not block the finalizer
		//goroutine
		//go func() {
		//	fmt.Println("finalize write batch")
		//	C.queue_wb_delete(wb.state)
		//}()
	})
	return &wb
}

func (wb *WriteBatch) Set(key, value []byte) {
	C.queue_wb_set(wb.state, (*C.char)(unsafe.Pointer(&key[0])), (C.size_t)(len(key)),
		(*C.char)(unsafe.Pointer(&value[0])), (C.size_t)(len(value)))
}

func (wb *WriteBatch) Commit() {
	C.queue_wb_done(wb.state)
	//C.queue_wb_delete(wb.state)
}
