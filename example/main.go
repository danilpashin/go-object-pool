package main

import (
	"context"
	"fmt"
	"time"

	pool "github.com/danilpashin/go-object-pool"
)

func main() {
	conf := pool.NewConfig()

	pool, err := pool.NewPool[int](3, conf)
	if err != nil {
		return
	}

	pool.Put(2)
	pool.Put(4)
	pool.Put(1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	fmt.Println(pool.Get(ctx))
}
