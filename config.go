package pool

import "time"

type PoolConfig struct {
	MaxTotal           int
	MaxIdle            int
	MinIdle            int
	BlockWhenExhausted bool
	MaxWait            time.Duration
}

type Option func(*PoolConfig)

func WithMaxTotal(n int) Option {
	return func(c *PoolConfig) { c.MaxTotal = n }
}

func WithMaxIdle(n int) Option {
	return func(c *PoolConfig) { c.MaxIdle = n }
}

func WithMinIdle(n int) Option {
	return func(c *PoolConfig) { c.MinIdle = n }
}

func WithBlockWhenExhausted(b bool) Option {
	return func(c *PoolConfig) { c.BlockWhenExhausted = b }
}

func WithMaxWait(d time.Duration) Option {
	return func(c *PoolConfig) { c.MaxWait = d }
}

func NewConfig(opts ...Option) *PoolConfig {
	conf := &PoolConfig{
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
