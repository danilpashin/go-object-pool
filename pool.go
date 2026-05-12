package pool

import (
	"context"
	"errors"
)

type Pool[T any] struct {
	objects chan T
	conf    *PoolConfig
}

func NewPool[T any](capacity int, conf *PoolConfig) (*Pool[T], error) {
	if capacity <= 0 {
		return nil, errors.New("invalid capacity")
	}

	return &Pool[T]{
		objects: make(chan T, capacity),
		conf:    conf,
	}, nil
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
	p.objects <- object
}
