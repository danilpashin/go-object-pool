package pool

import "time"

type PoolConfig[T any] struct {
	Capacity     int
	MinIdle      int
	MaxWait      time.Duration
	ResetFunc    func(*PoolObject[T])
	ScanInterval time.Duration
	DeleteSize   int
	stop         chan struct{}
}

type Option[T any] func(*PoolConfig[T])

func WithCapacity[T any](n int) Option[T] {
	return func(c *PoolConfig[T]) { c.Capacity = n }
}

func WithMinIdle[T any](n int) Option[T] {
	return func(c *PoolConfig[T]) { c.MinIdle = n }
}

func WithMaxWait[T any](d time.Duration) Option[T] {
	return func(c *PoolConfig[T]) { c.MaxWait = d }
}

func NewConfig[T any](opts ...Option[T]) *PoolConfig[T] {
	conf := &PoolConfig[T]{
		Capacity:     100,
		MinIdle:      10,
		MaxWait:      time.Second * 3,
		ScanInterval: time.Second,
		DeleteSize:   5,
		stop:         make(chan struct{}),
	}

	for _, opt := range opts {
		opt(conf)
	}

	return conf
}
