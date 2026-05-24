package pool

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
)

type Pool[T any] struct {
	objects  chan T
	capacity int64
	created  int64
	missed   int64
	factory  func() T
	conf     *PoolConfig[T]
}

type Resettable interface {
	Reset()
}

func NewPool[T any](initialSize int, capacity int, conf *PoolConfig[T], factory func() T) (*Pool[T], error) {
	if capacity <= 0 {
		return nil, errors.New("invalid capacity")
	}

	pool := &Pool[T]{
		objects:  make(chan T, capacity),
		capacity: capacity,
		factory:  factory,
		conf:     conf,
	}

	for range initialSize {
		pool.objects <- factory()
		atomic.AddInt64(&pool.created, 1)
	}

	return pool, nil
}

func (p *Pool[T]) Get(ctx context.Context) T {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), p.conf.MaxWait)
		defer cancel()
	}

	select {
	case obj := <-p.objects:
		return obj
	default:
	}

	newCreated := atomic.AddInt64(&p.created, 1)

	if newCreated > atomic.LoadInt64(&p.capacity) {
		atomic.AddInt64(&p.created, -1)
	select {
	case obj := <-p.objects:
		return obj
	case <-ctx.Done():
			atomic.AddInt64(&p.missed, 1)
		var zero T
		return zero
	}
	}

	return p.factory()
}

func (p *Pool[T]) Put(object T) {
	if p.conf.ResetFunc != nil {
		p.conf.ResetFunc(object)
	} else if resettable, ok := any(object).(Resettable); ok {
		resettable.Reset()
	} else {
		var zero T
		object = zero
	}

	p.objects <- object
}
