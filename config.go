package pool

import "time"

type PoolConfig[T any] struct {
	MinIdle      int64
	MaxWait      time.Duration
	ResetFunc    func(T)
	ScanInterval time.Duration
	MaxLifetime  time.Duration
	DeleteSize   int
	stop         chan struct{}
}

type Option[T any] func(*PoolConfig[T])

func WithMinIdle[T any](n int64) Option[T] {
	return func(c *PoolConfig[T]) { c.MinIdle = n }
}

func WithMaxWait[T any](d time.Duration) Option[T] {
	return func(c *PoolConfig[T]) { c.MaxWait = d }
}

func WithResetFunc[T any](f func(T)) Option[T] {
	return func(c *PoolConfig[T]) { c.ResetFunc = f }
}

func WithScanInterval[T any](d time.Duration) Option[T] {
	return func(c *PoolConfig[T]) { c.ScanInterval = d }
}

func WithMaxLifetime[T any](d time.Duration) Option[T] {
	return func(c *PoolConfig[T]) { c.MaxLifetime = d }
}

func WithDeleteSize[T any](n int) Option[T] {
	return func(c *PoolConfig[T]) { c.DeleteSize = n }
}

func NewConfig[T any](opts ...Option[T]) *PoolConfig[T] {
	conf := &PoolConfig[T]{
		MinIdle:      10,
		MaxWait:      time.Second * 3,
		ScanInterval: time.Second,
		MaxLifetime:  time.Second,
		DeleteSize:   5,
		stop:         make(chan struct{}),
	}

	for _, opt := range opts {
		opt(conf)
	}

	return conf
}
