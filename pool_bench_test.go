package pool

import (
	"context"
	"sync"
	"testing"
	"time"
)

func BenchmarkSyncPool(b *testing.B) {
	pool := &sync.Pool{
		New: func() any { return make([]byte, 20480) },
	}
	b.ResetTimer()
	for b.Loop() {
		obj := pool.Get().([]byte)
		if obj == nil {
			continue
		}

		obj[0] = 1
		pool.Put(obj)
	}
}

func BenchmarkMyPool(b *testing.B) {
	cfg := NewConfig(
		WithMinIdle[[]byte](1),
		WithDeleteSize[[]byte](1),
		WithScanInterval[[]byte](time.Millisecond*100),
		WithMaxLifetime[[]byte](time.Millisecond*100),
		WithResetFunc(func(p []byte) { p = p[:0] }),
	)
	pool, err := NewPool(1, 2, cfg, func() []byte { return make([]byte, 20480) })
	if err != nil {
		b.Fatalf("Failed to create pool: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	b.ResetTimer()
	for b.Loop() {
		obj := pool.Get(ctx)

		obj.Value[0] = 1
		pool.Put(obj)
	}
}

func BenchmarkSyncPoolParallel(b *testing.B) {
	pool := &sync.Pool{
		New: func() any { return make([]byte, 20480) },
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			obj := pool.Get().([]byte)
			obj[0] = 1
			pool.Put(obj)
		}
	})
}

func BenchmarkMyPoolParallel(b *testing.B) {
	cfg := NewConfig(
		WithMinIdle[[]byte](100),
		WithDeleteSize[[]byte](1),
		WithScanInterval[[]byte](time.Millisecond*100),
		WithMaxLifetime[[]byte](time.Millisecond*100),
		WithResetFunc(func(p []byte) { p = p[:0] }),
	)
	pool, _ := NewPool(100, 200, cfg, func() []byte { return make([]byte, 20480) })
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			obj := pool.Get(ctx)
			if obj == nil {
				continue
			}
			obj.Value[0] = 1
			pool.Put(obj)
		}
	})
}
