package pool

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"
)

type TestHeavyObject struct {
	Bytes1 []byte
	Bytes2 []byte
}

func (h *TestHeavyObject) Reset() {
	h.Bytes1 = h.Bytes1[:0]
	h.Bytes2 = h.Bytes2[:0]
}

func TestPool(t *testing.T) {
	t.Run("synchronous_requests", func(t *testing.T) {
		const (
			requests = 20
			poolSize = 10
			poolCap  = 10
			sliceCap = 1024
		)

		cfg := NewConfig[*TestHeavyObject](
			WithResetFunc(func(p *TestHeavyObject) {
				p.Bytes1 = p.Bytes1[:0]
				p.Bytes2 = p.Bytes2[:0]
			}),
		)
		factory := func() *TestHeavyObject {
			return &TestHeavyObject{
				Bytes1: make([]byte, 0, sliceCap),
				Bytes2: make([]byte, 0, sliceCap),
			}
		}

		p, err := NewPool[*TestHeavyObject](poolSize, int64(poolCap), cfg, factory)
		if err != nil {
			t.Fatal("failed to create pool: ", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
		defer cancel()

		expectedObj := &TestHeavyObject{
			Bytes1: make([]byte, 0, sliceCap),
			Bytes2: make([]byte, 0, sliceCap),
		}
		for i := range requests {
			obj := p.Get(ctx)

			obj.Value.Bytes1 = append(obj.Value.Bytes1, []byte("object data 1")...)
			obj.Value.Bytes2 = append(obj.Value.Bytes2, []byte("object data 2")...)

			p.Put(obj)

			if !reflect.DeepEqual(expectedObj, obj.Value) {
				t.Fatalf("iteration %d: expected %v, got: %v", i, expectedObj, obj.Value)
			}
		}

		expectedStats := &Stats{
			Capacity: int64(poolCap),
			Created:  10,
			Missed:   0,
			Active:   0,
		}

		currentStats := p.Stats()
		if !reflect.DeepEqual(expectedStats, currentStats) {
			t.Fatalf("stats mismatch: expected %+v, got: %+v", expectedStats, currentStats)
		}
	})
	t.Run("capacity limit", func(t *testing.T) {
		const (
			requests = 11
			poolSize = 10
			poolCap  = 10
			sliceCap = 1024
		)

		cfg := NewConfig[*TestHeavyObject]()
		factory := func() *TestHeavyObject {
			return &TestHeavyObject{
				Bytes1: make([]byte, 0, sliceCap),
				Bytes2: make([]byte, 0, sliceCap),
			}
		}

		p, err := NewPool[*TestHeavyObject](poolSize, int64(poolCap), cfg, factory)
		if err != nil {
			t.Fatal("failed to create pool: ", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
		defer cancel()

		objects := make([]*PoolObject[*TestHeavyObject], 0, poolCap)
		for i := range poolCap {
			obj := p.Get(ctx)
			if obj.Value == nil {
				t.Fatalf("iteration %d: expected %v, got: %v", i, &TestHeavyObject{}, obj)
			}

			objects = append(objects, obj)
		}

		if res := p.Get(ctx); res != nil {
			t.Fatalf("expected nil, got: %v", res)
		}

		expectedStats := &Stats{
			Capacity: int64(poolCap),
			Created:  10,
			Missed:   1,
			Active:   10,
		}

		currentStats := p.Stats()
		if !reflect.DeepEqual(expectedStats, currentStats) {
			t.Fatalf("stats mismatch: expected %+v, got: %+v", expectedStats, currentStats)
		}

		for _, obj := range objects {
			p.Put(obj)
		}

		expectedStats = &Stats{
			Capacity: int64(poolCap),
			Created:  10,
			Missed:   1,
			Active:   0,
		}

		currentStats = p.Stats()
		if !reflect.DeepEqual(expectedStats, currentStats) {
			t.Fatalf("stats mismatch: expected %+v, got: %+v", expectedStats, currentStats)
		}
	})
	t.Run("concurrency", func(t *testing.T) {
		const (
			requests = 100
			poolSize = 2
			poolCap  = 10
			sliceCap = 10
		)

		cfg := NewConfig[*TestHeavyObject]()
		factory := func() *TestHeavyObject {
			return &TestHeavyObject{
				Bytes1: make([]byte, 0, sliceCap),
				Bytes2: make([]byte, 0, sliceCap),
			}
		}

		p, err := NewPool[*TestHeavyObject](poolSize, poolCap, cfg, factory)
		if err != nil {
			t.Fatal("failed to create pool:", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
		defer cancel()

		var wg sync.WaitGroup

		for range requests {
			wg.Go(func() {
				obj1 := p.Get(ctx)
				if obj1 != nil {
					obj1.Value.Bytes1 = append(obj1.Value.Bytes1, []byte("some data")...)
					obj1.Value.Bytes2 = append(obj1.Value.Bytes2, []byte("some data")...)
					time.Sleep(time.Millisecond * 20)
					p.Put(obj1)
				}
			})
		}

		wg.Wait()

		expectedStats := &Stats{
			Capacity: int64(poolCap),
			Created:  10,
			Missed:   90,
			Active:   0,
		}

		currentStats := p.Stats()
		if !reflect.DeepEqual(expectedStats, currentStats) {
			t.Fatalf("stats mismatch: expected %+v, got: %+v", expectedStats, currentStats)
		}
	})
	t.Run("cleaning", func(t *testing.T) {
		const (
			requests = 10
			poolSize = 30
			poolCap  = 30
			sliceCap = 10
		)

		cfg := NewConfig[*TestHeavyObject](
			WithMinIdle[*TestHeavyObject](2),
			WithScanInterval[*TestHeavyObject](time.Millisecond*50),
			WithDeleteSize[*TestHeavyObject](2),
		)
		factory := func() *TestHeavyObject {
			return &TestHeavyObject{
				Bytes1: make([]byte, 0, sliceCap),
				Bytes2: make([]byte, 0, sliceCap),
			}
		}

		p, err := NewPool[*TestHeavyObject](poolSize, poolCap, cfg, factory)
		if err != nil {
			t.Fatal("failed to create pool:", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
		defer cancel()

		var wg sync.WaitGroup

		for range requests {
			wg.Go(func() {
				obj1 := p.Get(ctx)
				if obj1 != nil {
					obj1.Value.Bytes1 = append(obj1.Value.Bytes1, []byte("some data")...)
					obj1.Value.Bytes2 = append(obj1.Value.Bytes2, []byte("some data")...)
					p.Put(obj1)
				}
			})
		}

		wg.Wait()

		time.Sleep(time.Second * 1)

		expectedStats := &Stats{
			Capacity: int64(poolCap),
			Created:  2,
			Missed:   0,
			Active:   0,
		}

		currentStats := p.Stats()
		if !reflect.DeepEqual(expectedStats, currentStats) {
			t.Fatalf("stats mismatch: expected %+v, got: %+v", expectedStats, currentStats)
		}
	})
	t.Run("cleaning with dynamic delete size", func(t *testing.T) {
		const (
			requests = 10
			poolSize = 30
			poolCap  = 30
			sliceCap = 10
		)

		cfg := NewConfig[*TestHeavyObject](
			WithMinIdle[*TestHeavyObject](2),
			WithDeleteSize[*TestHeavyObject](4),
			WithScanInterval[*TestHeavyObject](time.Millisecond*50),
		)
		factory := func() *TestHeavyObject {
			return &TestHeavyObject{
				Bytes1: make([]byte, 0, sliceCap),
				Bytes2: make([]byte, 0, sliceCap),
			}
		}

		p, err := NewPool[*TestHeavyObject](poolSize, poolCap, cfg, factory)
		if err != nil {
			t.Fatal("failed to create pool:", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
		defer cancel()

		var wg sync.WaitGroup

		for range requests {
			wg.Go(func() {
				obj1 := p.Get(ctx)
				if obj1 != nil {
					obj1.Value.Bytes1 = append(obj1.Value.Bytes1, []byte("some data")...)
					obj1.Value.Bytes2 = append(obj1.Value.Bytes2, []byte("some data")...)
					p.Put(obj1)
				}
			})
		}

		wg.Wait()

		time.Sleep(time.Second * 1)

		expectedStats := &Stats{
			Capacity: int64(poolCap),
			Created:  2,
			Missed:   0,
			Active:   0,
		}

		currentStats := p.Stats()
		if !reflect.DeepEqual(expectedStats, currentStats) {
			t.Fatalf("stats mismatch: expected %+v, got: %+v", expectedStats, currentStats)
		}
	})
	t.Run("cleaning large pool", func(t *testing.T) {
		const (
			requests = 40000
			poolSize = 40000
			poolCap  = 40000
			sliceCap = 100
		)

		cfg := NewConfig[*TestHeavyObject](
			WithMinIdle[*TestHeavyObject](5000),
			WithDeleteSize[*TestHeavyObject](1000),
			WithScanInterval[*TestHeavyObject](time.Millisecond*50),
		)
		factory := func() *TestHeavyObject {
			return &TestHeavyObject{
				Bytes1: make([]byte, 0, sliceCap),
				Bytes2: make([]byte, 0, sliceCap),
			}
		}

		p, err := NewPool[*TestHeavyObject](poolSize, poolCap, cfg, factory)
		if err != nil {
			t.Fatal("failed to create pool:", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
		defer cancel()

		var wg sync.WaitGroup

		for range requests {
			wg.Go(func() {
				obj1 := p.Get(ctx)
				if obj1 != nil {
					obj1.Value.Bytes1 = append(obj1.Value.Bytes1, []byte("some data")...)
					obj1.Value.Bytes2 = append(obj1.Value.Bytes2, []byte("some data")...)
					time.Sleep(time.Second * 2)
					p.Put(obj1)
				}
			})
		}

		wg.Wait()

		time.Sleep(time.Second * 2)

		expectedStats := &Stats{
			Capacity: int64(poolCap),
			Created:  5000,
			Missed:   0,
			Active:   0,
		}

		currentStats := p.Stats()
		if !reflect.DeepEqual(expectedStats, currentStats) {
			t.Fatalf("stats mismatch: expected %+v, got: %+v", expectedStats, currentStats)
		}
	})
	t.Run("creating and cleaning large pool", func(t *testing.T) {
		const (
			requests = 100000
			poolSize = 0
			poolCap  = 100000
			sliceCap = 10
		)

		cfg := NewConfig[*TestHeavyObject](
			WithMinIdle[*TestHeavyObject](5000),
			WithDeleteSize[*TestHeavyObject](5000),
			WithScanInterval[*TestHeavyObject](time.Millisecond*100),
		)
		factory := func() *TestHeavyObject {
			return &TestHeavyObject{
				Bytes1: make([]byte, 0, sliceCap),
				Bytes2: make([]byte, 0, sliceCap),
			}
		}

		p, err := NewPool[*TestHeavyObject](poolSize, poolCap, cfg, factory)
		if err != nil {
			t.Fatal("failed to create pool:", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		var wg sync.WaitGroup

		for range requests {
			wg.Go(func() {
				obj1 := p.Get(ctx)
				if obj1 != nil {
					obj1.Value.Bytes1 = append(obj1.Value.Bytes1, []byte("some data")...)
					obj1.Value.Bytes2 = append(obj1.Value.Bytes2, []byte("some data")...)
					time.Sleep(time.Second * 2)
					p.Put(obj1)
				}
			})
		}

		wg.Wait()

		time.Sleep(time.Second * 3)

		expectedStats := &Stats{
			Capacity: int64(poolCap),
			Created:  5000,
			Missed:   0,
			Active:   0,
		}

		currentStats := p.Stats()
		if !reflect.DeepEqual(expectedStats, currentStats) {
			t.Fatalf("stats mismatch: expected %+v, got: %+v", expectedStats, currentStats)
		}
	})
}
