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

func (p *Pool[T]) Get() T {
	return <-p.objects
}

func (p *Pool[T]) Put(object T) {
	p.objects <- object
}
