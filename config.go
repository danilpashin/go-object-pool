package pool

import "time"

type PoolConfig[T any] struct {
	MaxTotal           int
	MaxIdle            int
	MinIdle            int
	BlockWhenExhausted bool
	MaxWait            time.Duration
	ResetFunc          func(T)
}

type Option[T any] func(*PoolConfig[T])

func WithMaxTotal[T any](n int) Option[T] {
	return func(c *PoolConfig[T]) { c.MaxTotal = n }
}

func WithMaxIdle[T any](n int) Option[T] {
	return func(c *PoolConfig[T]) { c.MaxIdle = n }
}

func WithMinIdle[T any](n int) Option[T] {
	return func(c *PoolConfig[T]) { c.MinIdle = n }
}

func WithBlockWhenExhausted[T any](b bool) Option[T] {
	return func(c *PoolConfig[T]) { c.BlockWhenExhausted = b }
}

func WithMaxWait[T any](d time.Duration) Option[T] {
	return func(c *PoolConfig[T]) { c.MaxWait = d }
}

func NewConfig[T any](opts ...Option[T]) *PoolConfig[T] {
	conf := &PoolConfig[T]{
		MaxTotal:           100,
		MaxIdle:            30,
		MinIdle:            10,
		BlockWhenExhausted: true,
		MaxWait:            time.Second * 3,
	}

	for _, opt := range opts {
		opt(conf)
	}

	return conf
}
