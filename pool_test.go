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

		expectedObj := &TestHeavyObject{
			Bytes1: make([]byte, 0, sliceCap),
			Bytes2: make([]byte, 0, sliceCap),
		}
		for i := range requests {
			obj := p.Get(ctx)

			obj.Bytes1 = append(obj.Bytes1, []byte("object data 1")...)
			obj.Bytes2 = append(obj.Bytes2, []byte("object data 2")...)

			p.Put(obj)

			if !reflect.DeepEqual(expectedObj, obj) {
				t.Fatalf("iteration %d: expected %v, got: %v", i, expectedObj, obj)
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

		objects := make([]*TestHeavyObject, 0, poolCap)
		for i := range poolCap {
			obj := p.Get(ctx)
			if obj == nil {
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
					obj1.Bytes1 = append(obj1.Bytes1, []byte("some data")...)
					obj1.Bytes2 = append(obj1.Bytes2, []byte("some data")...)
					time.Sleep(time.Millisecond * 10)
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
}
