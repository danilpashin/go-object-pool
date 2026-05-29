package pool

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"
)

type Pool[T any] struct {
	core *poolCore[T]
}

func (p *Pool[T]) Get(ctx context.Context) *PoolObject[T] { return p.core.Get(ctx) }
func (p *Pool[T]) Put(object *PoolObject[T])              { p.core.Put(object) }
func (p *Pool[T]) Stats() *Stats                          { return p.core.Stats() }

type poolCore[T any] struct {
	objects  chan *PoolObject[T]
	capacity int64
	created  int64
	missed   int64
	factory  func() T
	conf     *PoolConfig[T]
}

type PoolObject[T any] struct {
	Value    T
	lastUsed time.Time
}

func NewPool[T any](initialSize int, capacity int64, conf *PoolConfig[T], factory func() T) (*Pool[T], error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("capacity must be positive, got %d", capacity)
	}
	if initialSize < 0 {
		return nil, fmt.Errorf("initial size cannot be negative, got %d", initialSize)
	}
	if initialSize > int(capacity) {
		return nil, fmt.Errorf("initial size (%d) cannot exceed capacity (%d)", initialSize, capacity)
	}
	if factory == nil {
		return nil, errors.New("factory function is required")
	}

	if conf != nil {
		if conf.MinIdle > capacity || conf.MinIdle < 0 {
			return nil, fmt.Errorf("min idle cannot be negative or exceed capacity, got %d", conf.MinIdle)
		}
		if conf.ScanInterval < 0 {
			return nil, fmt.Errorf("scan interval cannot be negative, got %v", conf.ScanInterval)
		}
		if conf.DeleteSize < 0 {
			return nil, fmt.Errorf("delete size cannot be negative, got %d", conf.DeleteSize)
		}
	} else {
		return nil, errors.New("configuration settings are required")
	}

	if conf.stop == nil {
		conf.stop = make(chan struct{})
	}

	core := &poolCore[T]{
		objects:  make(chan *PoolObject[T], capacity),
		capacity: capacity,
		factory:  factory,
		conf:     conf,
	}
	go core.cleaner()

	pool := &Pool[T]{core: core}

	for range initialSize {
		obj := &PoolObject[T]{
			Value:    factory(),
			lastUsed: time.Now(),
		}
		core.objects <- obj
		atomic.AddInt64(&core.created, 1)
	}

	runtime.AddCleanup(pool, func(stopChan chan struct{}) {
		close(stopChan)
	}, core.conf.stop)

	return pool, nil
}

func (c *poolCore[T]) Get(ctx context.Context) *PoolObject[T] {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), c.conf.MaxWait)
		defer cancel()
	}

	select {
	case obj := <-c.objects:
		obj.lastUsed = time.Now()
		return obj
	default:
	}

	newCreated := atomic.AddInt64(&c.created, 1)

	if newCreated > atomic.LoadInt64(&c.capacity) {
		atomic.AddInt64(&c.created, -1)
		select {
		case obj := <-c.objects:
			obj.lastUsed = time.Now()
			return obj
		case <-ctx.Done():
			atomic.AddInt64(&c.missed, 1)
			var zero *PoolObject[T]
			return zero
		}
	}

	obj := &PoolObject[T]{
		Value:    c.factory(),
		lastUsed: time.Now(),
	}

	return obj
}

func (c *poolCore[T]) Put(object *PoolObject[T]) {
	if c.conf.ResetFunc != nil {
		object.lastUsed = time.Now()
		c.conf.ResetFunc(object.Value)
	} else if resettable, ok := any(object.Value).(Resettable); ok {
		object.lastUsed = time.Now()
		resettable.Reset()
	} else {
		var zero *PoolObject[T]
		object = zero
	}

	c.objects <- object
}
