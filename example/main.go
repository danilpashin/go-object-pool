package main

import (
	"context"
	"fmt"
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
	conf := pool.NewConfig[*HeavyObject]()
	conf.MaxTotal = 2

	// Heavy objects (structures with multiple fields and lots of data).
	// Creating pool with parameters: initialSize = 2, maxTotal = 2.
	p, err := pool.NewPool[*HeavyObject](2, 2, conf, func() *HeavyObject {
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
	obj1.Bytes1 = append(obj1.Bytes1, []byte("first object data")...)

	// 2. Getting second object (last free in pool).
	obj2 := p.Get(ctx)
	obj2.Bytes1 = append(obj2.Bytes1, []byte("second object data")...)

	// 3. Trying to get third object. Pool is empty (maxSize = 2).
	// Should be blocked call.
	fmt.Println("Trying to get third object...")
	obj3 := p.Get(ctx)
	if obj3 == nil {
		fmt.Println("Pool is blocked: limit exceeded or timeout expired. It's okay!")
	}

	// Imitating work...
	fmt.Printf("Working with obj1: %s\n", string(obj1.Bytes1))

	// 4. Returning objects to pool. Reset() works automatically.
	p.Put(obj1)
	p.Put(obj2)

	// Nil to pointers to not use them later.
	obj1 = nil
	obj2 = nil

	conf2 := pool.NewConfig[[]int]()
	conf2.ResetFunc = func(slice []int) {
		slice = slice[:0]
	}
	// Regular objects (slices, arrays, strings, int, float types etc.).
	// Creating pool with parameters: initalSize = 2, capacity = 2.
	pSlice, err := pool.NewPool[[]int](2, 2, conf2, func() []int {
		return make([]int, 0, 1)
	})

	ctx, cancel = context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	// 1. Getting slice object. (memory is already allocated).
	slice := pSlice.Get(ctx)

	// Putting data in slice.
	slice = append(slice, 5)

	fmt.Printf("Used slice: %v\n", slice)

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
}
