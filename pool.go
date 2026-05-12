package pool

type Pool[T any] struct {
	objects chan T
}

func NewPool[T any](capacity int) *Pool[T] {
	return &Pool[T]{
		objects: make(chan T, capacity),
	}
}

func (p *Pool[T]) Get() T {
	return <-p.objects
}

func (p *Pool[T]) Put(object T) {
	p.objects <- object
}
