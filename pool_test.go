package pool

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"
)

type TestResettableObject struct{}

func (obj *TestResettableObject) Reset() {}

func TestPool(t *testing.T) {
	t.Run("synchronous_requests", func(t *testing.T) {
		cfg := NewConfig(WithResetFunc(func(obj []byte) { obj = obj[:0] }))

		p, _ := NewPool(10, 10, cfg, func() []byte { return make([]byte, 0, 10) })
		ctx := context.Background()

		for range 20 {
			obj := p.Get(ctx)
			obj.Value = append(obj.Value, []byte("object data")...)
			p.Put(obj)
		}

		stats := p.Stats()
		if stats.Created != 10 {
			t.Errorf("expected 10 objects left, got %d", stats.Created)
		}
	})
	t.Run("negative initial size", func(t *testing.T) {
		_, err := NewPool[[]byte](-10, 10, nil, nil)
		if err != nil {
			if err.Error() != "initial size cannot be negative, got -10" {
				t.Fatalf(`expected error "initial size cannot be negative, got -10", got: %v`, err)
			}
		}
	})
	t.Run("negative capacity", func(t *testing.T) {
		_, err := NewPool[[]byte](10, -10, nil, nil)
		if err != nil {
			if err.Error() != "capacity must be positive, got -10" {
				t.Fatalf(`expected error "capacity must be positive, got -10", got: %v`, err)
			}
		}
	})
	t.Run("initial size larger than capacity", func(t *testing.T) {
		_, err := NewPool[[]byte](11, 10, nil, nil)
		if err != nil {
			if err.Error() != "initial size (11) cannot exceed capacity (10)" {
				t.Fatalf(`expected error "initial size (11) cannot exceed capacity (10)", got: %v`, err)
			}
		}
	})
	t.Run("no factory function", func(t *testing.T) {
		_, err := NewPool[[]byte](10, 10, nil, nil)
		if err != nil {
			if err.Error() != "factory function is required" {
				t.Fatalf(`expected error "factory function is required", got: %v`, err)
			}
		}
	})
	t.Run("no configuration", func(t *testing.T) {
		_, err := NewPool(10, 10, nil, func() []byte { return make([]byte, 0, 10) })
		if err != nil {
			if err.Error() != "configuration settings are required" {
				t.Fatalf(`expected error "configuration settings are required", got: %v`, err)
			}
		}
	})
	t.Run("negative min idle", func(t *testing.T) {
		cfg := NewConfig(WithMinIdle[[]byte](-5))

		_, err := NewPool(1, 1, cfg, func() []byte { return make([]byte, 1) })
		if err != nil {
			if err.Error() != "min idle cannot be negative or exceed capacity, got -5" {
				t.Fatalf(`expected error "min idle cannot be negative or exceed capacity, got -5", got: %v`, err)
			}
		}
	})
	t.Run("negative scan interval", func(t *testing.T) {
		cfg := NewConfig(WithScanInterval[[]byte](-5))

		_, err := NewPool(1, 1, cfg, func() []byte { return make([]byte, 1) })
		if err != nil {
			if err.Error() != "scan interval cannot be negative, got -5ns" {
				t.Fatalf(`expected error "scan interval cannot be negative, got -5ns", got: %v`, err)
			}
		}
	})
	t.Run("negative delete size", func(t *testing.T) {
		cfg := NewConfig(WithDeleteSize[[]byte](-5))

		_, err := NewPool(1, 1, cfg, func() []byte { return make([]byte, 1) })
		if err != nil {
			if err.Error() != "delete size cannot be negative, got -5" {
				t.Fatalf(`expected error "delete size cannot be negative, got -5", got: %v`, err)
			}
		}
	})
	t.Run("put nil object", func(t *testing.T) {
		cfg := NewConfig[[]byte]()

		p, _ := NewPool(1, 1, cfg, func() []byte { return make([]byte, 1) })
		ctx := context.Background()

		_ = p.Get(ctx)
		p.Put(nil)
	})
	t.Run("put resettable object", func(t *testing.T) {
		cfg := NewConfig[*TestResettableObject]()

		p, _ := NewPool(1, 1, cfg, func() *TestResettableObject { return &TestResettableObject{} })
		ctx := context.Background()

		obj := p.Get(ctx)
		p.Put(obj)
		p.core.conf.stop <- struct{}{}
	})
	t.Run("put not resettable object", func(t *testing.T) {
		cfg := NewConfig[[]byte]()

		p, _ := NewPool(1, 1, cfg, func() []byte { return make([]byte, 1) })
		ctx := context.Background()

		obj := p.Get(ctx)
		p.Put(obj)
	})
	t.Run("capacity limit", func(t *testing.T) {
		cfg := NewConfig[[]byte]()

		p, _ := NewPool(10, 10, cfg, func() []byte { return make([]byte, 1) })

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
		defer cancel()

		objects := make([]*PoolObject[[]byte], 0, 10)
		for range 10 {
			obj := p.Get(ctx)
			objects = append(objects, obj)
		}

		if res := p.Get(ctx); res != nil {
			t.Fatalf("expected nil, got: %v", res)
		}

		expectedStats := &Stats{
			Capacity: 10,
			Created:  10,
			Missed:   1,
			Active:   10,
		}

		currentStats := p.Stats()
		if !reflect.DeepEqual(expectedStats, currentStats) {
			t.Fatalf("stats mismatch: expected %+v, got: %+v", expectedStats, currentStats)
		}
	})
	t.Run("incremental eviction under load", func(t *testing.T) {
		cfg := NewConfig(
			WithMinIdle[[]byte](5),
			WithDeleteSize[[]byte](5),
			WithScanInterval[[]byte](time.Millisecond*10),
			WithMaxLifetime[[]byte](time.Millisecond*5),
			WithResetFunc(func(obj []byte) { obj = obj[:0] }),
		)

		p, _ := NewPool(0, 10, cfg, func() []byte { return make([]byte, 0, 10) })
		ctx := context.Background()

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Go(func() {
				obj := p.Get(ctx)
				if obj != nil {
					time.Sleep(time.Millisecond * 2)
					p.Put(obj)
				}
			})
		}
		wg.Wait()

		time.Sleep(time.Millisecond * 40)

		stats := p.Stats()
		if stats.Created != 5 {
			t.Errorf("expected 5 objects left, got %d", stats.Created)
		}
	})
}
