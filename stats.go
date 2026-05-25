package pool

import "sync/atomic"

type Stats struct {
	Capacity int64
	Created  int64
	Missed   int64
	Active   int64
}

func (p *Pool[T]) Stats() *Stats {
	return &Stats{
		Capacity: atomic.LoadInt64(&p.capacity),
		Created:  atomic.LoadInt64(&p.created),
		Missed:   atomic.LoadInt64(&p.missed),
		Active:   atomic.LoadInt64(&p.created) - int64(len(p.objects)),
	}
}
