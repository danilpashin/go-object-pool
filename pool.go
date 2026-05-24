package pool

import (
	"context"
	"errors"
)

type Pool[T any] struct {
	objects chan T
	conf    *PoolConfig
}

type Resettable interface {
	Reset()
}

func NewPool[T any](initialSize int, capacity int, conf *PoolConfig, factory func() T) (*Pool[T], error) {
	if capacity <= 0 {
		return nil, errors.New("invalid capacity")
	}

	pool := &Pool[T]{
		objects: make(chan T, capacity),
		conf:    conf,
	}

	for range initialSize {
		pool.objects <- factory()
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
	case <-ctx.Done():
		var zero T
		return zero
	}
}

func (p *Pool[T]) Put(object T) {
	if resettable, ok := any(object).(Resettable); ok {
		resettable.Reset()
	} else {
		var zero T
		object = zero
	}

	p.objects <- object
}
