package pool

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

type Pool[T any] struct {
	core *poolCore[T]
}

func (p *Pool[T]) Get(ctx context.Context) *PoolObject[T] { return p.core.Get(ctx) }
func (p *Pool[T]) Put(object *PoolObject[T])              { p.core.Put(object) }
func (p *Pool[T]) Stats() *Stats                          { return p.core.Stats() }

type poolCore[T any] struct {
	shards    []chan *PoolObject[T]
	factory   func() T
	conf      *PoolConfig[T]
	capacity  int64
	_         [64]byte
	created   int64
	_         [64]byte
	missed    int64
	_         [64]byte
	numShards int
	shardMask uint32
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
		capacity: capacity,
		factory:  factory,
		conf:     conf,
	}

	numShards := 16
	if int(capacity) < numShards {
		for numShards > int(capacity) {
			numShards = numShards >> 1
		}
	}

	shards := make([]chan *PoolObject[T], numShards)

	shardCaps := make([]int, numShards)
	shardsCap := int(core.capacity) / numShards
	shardsCapExtra := int(core.capacity) % numShards

	for i := 0; i < numShards; i++ {
		shardCaps[i] = shardsCap
		if i < shardsCapExtra {
			shardCaps[i]++
		}
		shards[i] = make(chan *PoolObject[T], shardCaps[i])
	}

	for i := range initialSize {
		shardIdx := i % numShards
		obj := &PoolObject[T]{
			Value:    factory(),
			lastUsed: time.Now(),
		}
		shards[shardIdx] <- obj
	}

	core.shards = shards
	core.shardMask = uint32(numShards - 1)
	core.numShards = numShards
	core.created = int64(initialSize)

	pool := &Pool[T]{core: core}

	go core.cleaner()

	runtime.AddCleanup(pool, func(stopChan chan struct{}) {
		close(stopChan)
	}, core.conf.stop)

	return pool, nil
}

func (c *poolCore[T]) Get(ctx context.Context) *PoolObject[T] {
	shardIdx := int(rand.Uint32()) & (c.numShards - 1)
	if obj := c.tryTakeFromShard(shardIdx); obj != nil {
		return obj
	}

	for i := 1; i < c.numShards; i++ {
		nextShardIdx := (shardIdx + i) & (c.numShards - 1)
		if obj := c.tryTakeFromShard(nextShardIdx); obj != nil {
			return obj
		}
	}

	for {
		currentCreated := atomic.LoadInt64(&c.created)
		if currentCreated >= atomic.LoadInt64(&c.capacity) {
			break
		}
		if atomic.CompareAndSwapInt64(&c.created, currentCreated, currentCreated+1) {
			return &PoolObject[T]{
				Value:    c.factory(),
				lastUsed: time.Now(),
			}
		}
	}

	ticker := time.NewTicker(time.Microsecond * 50)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			atomic.AddInt64(&c.missed, 1)
			return nil
		case <-ticker.C:
			for i := 0; i < c.numShards; i++ {
				if obj := c.tryTakeFromShard(i); obj != nil {
					return obj
				}
			}
		}
	}
}

func (c *poolCore[T]) tryTakeFromShard(idx int) *PoolObject[T] {
	select {
	case obj := <-c.shards[idx]:
		if obj != nil {
			obj.lastUsed = time.Now()
			return obj
		}
	default:
	}
	return nil
}

func (c *poolCore[T]) Put(object *PoolObject[T]) {
	if object == nil {
		return
	}

	canReuse := false

	if c.conf.ResetFunc != nil {
		object.lastUsed = time.Now()
		c.conf.ResetFunc(object.Value)
		canReuse = true
	} else if resettable, ok := any(object.Value).(Resettable); ok {
		object.lastUsed = time.Now()
		resettable.Reset()
		canReuse = true
	}

	if canReuse {
		shardIdx := int(rand.Uint32()) & (c.numShards - 1)
		shard := c.shards[shardIdx]

		if len(shard) >= cap(shard) {
			for i := 0; i < c.numShards; i++ {
				currentIdx := (shardIdx + i) % c.numShards

				currentShard := c.shards[currentIdx]

				if len(currentShard) < cap(currentShard) {
					shard = currentShard
					break
				}
			}
		}

		select {
		case shard <- object:
		default:
			atomic.AddInt64(&c.created, -1)
		}
	} else {
		atomic.AddInt64(&c.created, -1)
	}
}
