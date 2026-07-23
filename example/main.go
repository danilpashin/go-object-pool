package main

import (
	"context"
	"fmt"
	"runtime"
	"time"

	pool "github.com/danilpashin/go-object-pool"
)

type HeavyObject struct {
	Bytes1 []byte
	Bytes2 []byte
}

func (h *HeavyObject) Reset() {
	h.Bytes1 = h.Bytes1[:0]
	h.Bytes2 = h.Bytes2[:0]
	fmt.Println("Object is resetted")
}

func main() {
	// Config contains parameters for background cleaner goroutine
	//
	// ScanInterval defines how often the janitor checks for idle objects to remove.
	// MaxLifetime defines how long a pool object can live before being removed.
	// DeleteSize specifies how many idle objects are removed per scan when
	// the pool size exceeds MinIdle.
	conf := pool.NewConfig(
		pool.WithMinIdle[*HeavyObject](2),
		pool.WithScanInterval[*HeavyObject](time.Second*5),
		pool.WithMaxLifetime[*HeavyObject](time.Millisecond*50),
		pool.WithDeleteSize[*HeavyObject](1),
	)

	// Heavy objects (structures with multiple fields and lots of data).
	// Creating pool with parameters: initialSize = 2, capacity = 3.
	p, err := pool.NewPool(2, 3, conf, func() *HeavyObject {
		return &HeavyObject{
			Bytes1: make([]byte, 0, 8192),
			Bytes2: make([]byte, 0, 8192),
		}
	})
	if err != nil {
		fmt.Printf("Error creating pool: %v\n", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	// 1. Getting first object. (memory is already allocated).
	obj1 := p.Get(ctx)
	if obj1 == nil {
		fmt.Println("Failed to get obj1")
		return
	}
	// Putting data in obj1
	obj1.Value.Bytes1 = append(obj1.Value.Bytes1, []byte("first object data")...)

	// 2. Getting second object (last free in pool).
	obj2 := p.Get(ctx)
	obj2.Value.Bytes1 = append(obj2.Value.Bytes1, []byte("second object data")...)

	// 3. Trying to get third object. Pool is empty (created = 2, capacity = 3).
	// Object dynamically created in the pool.
	obj3 := p.Get(ctx)
	obj3.Value.Bytes2 = append(obj3.Value.Bytes2, []byte("third object data")...)

	// 4. Trying to get fourth object. Pool is empty and fully used (created = 3, capacity = 3).
	// Should be blocked call.
	fmt.Println("Trying to get fourth object...")
	obj4 := p.Get(ctx)
	if obj4 == nil {
		fmt.Printf("Pool is blocked: limit exceeded. It's okay!\n")
	}

	// Imitating work...
	fmt.Printf("Working with obj1: %s\n", string(obj1.Value.Bytes1))
	fmt.Printf("Working with obj2: %s\n", string(obj2.Value.Bytes1))
	fmt.Printf("Working with obj3: %s\n", string(obj3.Value.Bytes2))

	// 5. Returning objects to pool. Reset() works automatically.
	p.Put(obj1)
	p.Put(obj2)
	p.Put(obj3)

	// Nil to pointers to not use them later.
	obj1 = nil
	obj2 = nil
	obj3 = nil

	fmt.Printf("Pool(HeavyObject) stats: %+v\n", p.Stats())

	// Allow pool to be garbage collected, which stops background cleaner goroutine
	p = nil

	// Force GC to demonstrate janitor shutdown (for example only)
	// Janitor automatically stops when pool is garbage collected
	runtime.GC()

	// Regular objects (slices, arrays, strings, int, float types etc.).
	conf2 := pool.NewConfig(
		pool.WithResetFunc(func(obj []int) {
			obj = obj[:0]
		}),
	)

	// Creating pool with parameters: initialSize = 2, capacity = 2.
	pSlice, err := pool.NewPool(2, 2, conf2, func() []int {
		return make([]int, 0, 1)
	})
	if err != nil {
		fmt.Printf("Error creating pool: %v\n", err)
		return
	}

	ctx, cancel = context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	// 1. Getting slice object. (memory is already allocated).
	slice := pSlice.Get(ctx)

	// Putting data in slice.
	slice.Value = append(slice.Value, 5)

	fmt.Printf("Used slice: %v\n", slice.Value)

	// 2. Returning slice to the pool.
	// No Reset(). Object gets default value of its type.
	pSlice.Put(slice)

	// Nil to pointer to not use slice later.
	slice = nil

	// Imitating context timeout...
	time.Sleep(time.Second * 3)

	// Trying to get slice after context timeout.
	// Pool returns nil because of timeout.
	slice = pSlice.Get(ctx)
	if slice == nil {
		fmt.Println("Pool is blocked: timeout expired. It's okay!")
	}

	// ResetFunc clears the object state to prevent memory leaks in the pool.
	pSlice.Put(slice)

	// nil to pointer to not use slice later.
	slice = nil

	fmt.Printf("Pool([]int) stats: %+v\n", pSlice.Stats())

	// Allow pool to be garbage collected, which stops background cleaner goroutine
	pSlice = nil

	// Force GC to demonstrate janitor shutdown (for example only)
	// Janitor automatically stops when pool is garbage collected
	runtime.GC()

	// Waiting for cleaner finished message
	time.Sleep(time.Second * 1)
}
