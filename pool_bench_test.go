package pool

import (
	"context"
	"sync"
	"testing"
	"time"
)

func BenchmarkPureSyncPool(b *testing.B) {
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

func BenchmarkPureMyPool(b *testing.B) {
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

func BenchmarkPureSyncPoolParallel(b *testing.B) {
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

func BenchmarkPureMyPoolParallel(b *testing.B) {
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

	// pool.core.conf.stop <- struct{}{}
}

// t.Run("creating and cleaning large pool with heavy data", func(t *testing.T) {
// 		const (
// 			requests = 100000
// 			poolSize = 0
// 			poolCap  = 100000
// 			sliceCap = 10000
// 		)

// 		heavyData := bytes.Repeat([]byte{'A'}, 10000)

// 		cfg := NewConfig(
// 			WithMinIdle[*TestHeavyObject](5000),
// 			WithDeleteSize[*TestHeavyObject](5000),
// 			WithScanInterval[*TestHeavyObject](time.Millisecond*100),
// 			WithMaxLifetime[*TestHeavyObject](time.Millisecond*10),
// 		)
// 		factory := func() *TestHeavyObject {
// 			return &TestHeavyObject{
// 				Bytes1: make([]byte, 0, sliceCap),
// 				Bytes2: make([]byte, 0, sliceCap),
// 			}
// 		}

// 		p, err := NewPool(poolSize, poolCap, cfg, factory)
// 		if err != nil {
// 			t.Fatal("failed to create pool:", err)
// 		}

// 		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
// 		defer cancel()

// 		var wg sync.WaitGroup

// 		for range requests {
// 			wg.Go(func() {
// 				obj1 := p.Get(ctx)
// 				if obj1 != nil {
// 					obj1.Value.Bytes1 = append(obj1.Value.Bytes1, heavyData...)
// 					obj1.Value.Bytes2 = append(obj1.Value.Bytes2, heavyData...)
// 					time.Sleep(time.Second * 2)
// 					p.Put(obj1)
// 				}
// 			})
// 		}

// 		wg.Wait()

// 		time.Sleep(time.Second * 3)

// 		expectedStats := &Stats{
// 			Capacity: int64(poolCap),
// 			Created:  5000,
// 			Missed:   0,
// 			Active:   0,
// 		}

// 		currentStats := p.Stats()
// 		if !reflect.DeepEqual(expectedStats, currentStats) {
// 			t.Fatalf("stats mismatch: expected %+v, got: %+v", expectedStats, currentStats)
// 		}
// 	})
