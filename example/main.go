package main

import (
	"fmt"

	pool "github.com/danilpashin/go-object-pool"
)

func main() {
	pool := pool.NewPool[int](3)
	pool.Put(2)
	pool.Put(4)
	pool.Put(1)
	fmt.Println(pool.Get())
}
