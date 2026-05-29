package pool

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

type Pool[T any] struct {
	objects  chan *PoolObject[T]
	capacity int64
	created  int64
	missed   int64
	factory  func() T
	conf     *PoolConfig[T]
}

type PoolObject[T any] struct {
	Value    T
	lastUsed time.Time
}

func NewPool[T any](initialSize int, capacity int64, conf *PoolConfig[T], factory func() T) (*Pool[T], error) {
	if capacity <= 0 {
		return nil, errors.New("invalid capacity")
	}

	pool := &Pool[T]{
		objects:  make(chan *PoolObject[T], capacity),
		capacity: capacity,
		factory:  factory,
		conf:     conf,
	}

	for range initialSize {
		obj := &PoolObject[T]{
			Value:    factory(),
			lastUsed: time.Now(),
		}
		pool.objects <- obj
		atomic.AddInt64(&pool.created, 1)
	}
	go pool.cleaner()

	return pool, nil
}

func (p *Pool[T]) Get(ctx context.Context) *PoolObject[T] {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), p.conf.MaxWait)
		defer cancel()
	}

	select {
	case obj := <-p.objects:
		obj.lastUsed = time.Now()
		return obj
	default:
	}

	newCreated := atomic.AddInt64(&p.created, 1)

	if newCreated > atomic.LoadInt64(&p.capacity) {
		atomic.AddInt64(&p.created, -1)
		select {
		case obj := <-p.objects:
			obj.lastUsed = time.Now()
			return obj
		case <-ctx.Done():
			atomic.AddInt64(&p.missed, 1)
			var zero *PoolObject[T]
			return zero
		}
	}

	obj := &PoolObject[T]{
		Value:    p.factory(),
		lastUsed: time.Now(),
	}

	return obj
}

func (p *Pool[T]) Put(object *PoolObject[T]) {
	if p.conf.ResetFunc != nil {
		object.lastUsed = time.Now()
		p.conf.ResetFunc(object)
	} else if resettable, ok := any(object.Value).(Resettable); ok {
		object.lastUsed = time.Now()
		resettable.Reset()
	} else {
		var zero *PoolObject[T]
		object = zero
		fmt.Println("nil object")
	}

	p.objects <- object
}
