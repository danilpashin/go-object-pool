package pool

import "sync/atomic"

type Stats struct {
	Capacity int64
	Created  int64
	Missed   int64
	Active   int64
}

func (c *poolCore[T]) Stats() *Stats {
	idleCount := 0
	for _, shard := range c.shards {
		idleCount += len(shard)
	}
	return &Stats{
		Capacity: atomic.LoadInt64(&c.capacity),
		Created:  atomic.LoadInt64(&c.created),
		Missed:   atomic.LoadInt64(&c.missed),
		Active:   atomic.LoadInt64(&c.created) - int64(idleCount),
	}
}
