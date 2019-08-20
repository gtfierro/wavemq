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

var initOnce sync.Once
var noop = true

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

func GetError(errstr *C.char, errlen C.size_t) error {
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

func Initialize(dbname string, spinning_metal bool) {
	initOnce.Do(func() {
		opt_for_spin := 0
		if spinning_metal {
			opt_for_spin = 1
		}
		name := []byte(dbname)
		C.c_init((*C.char)(unsafe.Pointer(&name[0])), (C.size_t)(len(name)), (C.size_t)(opt_for_spin))
	})
}

func Close() {
	if noop {
		return
	}
	C.close_db()
}

func QueueGet(key []byte) ([]byte, error) {
	fmt.Println("queue get")
	//if noop {
	//	return []byte{}, nil
	//}
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

func QueueSet(key, value []byte) error {
	//fmt.Println("queue set")
	//if noop {
	//	return nil
	//}
	var errstr *C.char
	var errlen C.size_t
	C.queue_set((*C.char)(unsafe.Pointer(&key[0])), (C.size_t)(len(key)),
		(*C.char)(unsafe.Pointer(&value[0])), (C.size_t)(len(value)),
		&errstr, &errlen)
	//fmt.Printf("errlen: %d\n", int(errlen))
	err := GetError(errstr, errlen)
	//fmt.Printf("got error: %v\n", err)
	return err
}

func QueueDelete(key []byte) error {
	//if noop {
	//	return
	//}
	var errstr *C.char
	var errlen C.size_t
	C.queue_delete((*C.char)(unsafe.Pointer(&key[0])), (C.size_t)(len(key)), &errstr, &errlen)
	return GetError(errstr, errlen)
}

func QueueDeletePrefix(key []byte) int {
	//if noop {
	//	return 0
	//}
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
	//if noop {
	//	return nil
	//}
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
	//if noop {
	//	return
	//}
	C.queue_it_next(it.state, (*C.char)(unsafe.Pointer(&it.prefix[0])), (C.size_t)(len(it.prefix)), &key, &keylen, &value, &valuelen)
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
	//if noop {
	//	return false
	//}
	return it.valid
}

func (it *Iterator) Key() []byte {
	//if noop {
	//	return nil
	//}
	return it.current_key
}
func (it *Iterator) Value() []byte {
	//if noop {
	//	return nil
	//}
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

func PersistGet(key []byte) ([]byte, error) {
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

func PersistSet(key, value []byte) error {
	var errstr *C.char
	var errlen C.size_t
	C.queue_set((*C.char)(unsafe.Pointer(&key[0])), (C.size_t)(len(key)),
		(*C.char)(unsafe.Pointer(&value[0])), (C.size_t)(len(value)),
		&errstr, &errlen)
	return GetError(errstr, errlen)
}

func PersistDelete(key []byte) error {
	var errstr *C.char
	var errlen C.size_t
	C.queue_delete((*C.char)(unsafe.Pointer(&key[0])), (C.size_t)(len(key)), &errstr, &errlen)
	return GetError(errstr, errlen)
}
