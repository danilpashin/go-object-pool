# 📦 Thread-safe Generic Object Pool in Golang
![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go)
[![Coverage](https://img.shields.io/badge/Coverage-100.0%25-brightgreen)](https://github.com/danilpashin/go-object-pool/.github/workflows/go.yml)

## 📖 Overview

A high-performance, memory-efficient thread-safe generic object pool for Go that provides deterministic control over object lifecycle and memory usage. Built with production workloads in mind, this pool implements advanced concurrency patterns including sharding, atomic operations, and background cleanup to deliver predictable performance at scale.

## ✨ Why channels, sharding and CAS?
* 🚀 **Channels** - structures in Go made specifically to prevent any race conditions. They are safe and really fast. Even though it has internal mutexes that add some locks but it does not affect the performance that much (see **Benchmarks** ↓).
* 🔓 **Sharding** - to reduce channels lock time I separated one channel to 2<sup>n</sup> shards. It massively increases pool speed (2x in parallel)(see **Benchmarks** ↓).
* ⚡ **Compare and Swap (CAS)** - atomic operation allowing to perform pool methods without any mutex.

## 🧩 Why wasn't used lock-free? 
* **Vyukov Queue (MSPC)** - does not provide needed concurrency because of it `Single Consumer` nature. Thread-safe pool must be available to multiple consumers at the same time without long waiting.
* **Treiber Stack (MSMC)** - it provides `Multiple Consumers` pool availability but it has another problem. Stack structure is not suitable for cleaning operations (ABA problem). Each object must have lifetime that will be checked during janitor's background work. Cleaning will be possible only if the last object in stack exceeded lifetime and even if cleaning will start janitor will be stopped after new objects creation or stack pop from another thread.

## 🧹 How cleaning helps?
It helps to **reduce memory usage** in extreme pool sizes by reducing amount of idle objects to minimal idle value.
User can choose how many minimal idle objects must be in the pool and how often janitor must work.

## 🤔 Why not sync.Pool?

1. **Lack of size control**: sync.Pool uses GC for cleaning
2. **Unpredictability**: Objects may be deleted at anytime
3. **No guarantees**: Objects may be created and deleted without any control

My pool **solves these problems** by:
- Explicit size control
- Predictable behavior
- Cleaning without using GC

## 💻 Basic usage
```
func main() {
    conf := pool.NewConfig(
        pool.WithMinIdle[*HeavyObject](2),
        pool.WithScanInterval[*HeavyObject](time.Second*5),
        pool.WithMaxLifetime[*HeavyObject](time.Millisecond*50),
        pool.WithDeleteSize[*HeavyObject](1),
    )

    // Creating pool with parameters: initialSize = 2, capacity = 2.
    pSlice, err := pool.NewPool(2, 2, conf2, func() []int {
        return make([]int, 0, 1)
    })
    if err != nil {
        fmt.Printf("Error creating pool: %v\n", err)
        return
    }

    ctx, cancel = context.WithTimeout(context.Background(), time.Second*2)
    defer cancel()

    // 1. Getting slice object. (memory is already allocated).
    slice := pSlice.Get(ctx)

    // Putting data in slice.
    slice.Value = append(slice.Value, 5)

    fmt.Printf("Used slice: %v\n", slice.Value)

    // 2. Returning slice to the pool.
    // No Reset(). Object gets default value of its type.
    pSlice.Put(slice)

    // Nil to pointer to not use slice later.
    slice = nil
}
```

## 🏗️ Architecture
```
┌───────────────────────────────────────┐
│ Object Pool                           │
│ ┌───────────────────────────────────┐ │
│ │ Core                              │ │
│ │ ┌───────┐ ┌───────┐ ┌───────┐     │ │
│ │ │Shard0 │ │Shard1 │ │Shard2 │ ... │ │
│ │ │Channel│ │Channel│ │Channel│     │ │
│ │ └───────┘ └───────┘ └───────┘     │ │
│ └───────────────────────────────────┘ │
│                                       │
│ ┌───────────────────────────────────┐ │
│ │ Janitor (background worker)       │ │
│ │ • Checks objects lifetime         │ │
│ │ • Deletes expired objects         │ │
│ │ • Maintains MinIdle               │ │
│ └───────────────────────────────────┘ │
└───────────────────────────────────────┘
```

Shard is selected using rand.Uint32() & (numShards-1). numShards is 2<sup>n</sup>.

*<b><small>it's important to use math/rand/v2 because v1 is deprecated, slow and predictable</small></b>*

## 🧪 Testing
```bash
go test -v # only unit-tests
go test -run=^$ -bench=Benchmark -benchmem # only benchmarks
```

## 🚀 Benchmarks
* goos: windows
* goarch: amd64
* pkg: github.com/danilpashin/go-object-pool
* cpu: AMD Ryzen 5 7640HS w/ Radeon 760M Graphics

| Benchmark | Iterations | Time per operation | Memory per operation | Allocations per operation |
| :--- | :---: | :---: | :---: | :---: |
| `BenchmarkSyncPool-12`             |       33129585      |         37.43 ns/op     |     24 B/op    |     1 allocs/op |
| `BenchmarkMyPool-12`               |       17643633      |         65.56 ns/op     |      0 B/op    |     0 allocs/op |
| `BenchmarkSyncPoolParallel-12`     |      85893434       |         16.23 ns/op     |     24 B/op    |     1 allocs/op |
| `BenchmarkMyPoolParallel-12`       |       7876956       |         161.0 ns/op     |      0 B/op    |     0 allocs/op |
| `BenchmarkMyPoolParallel-12 (sharding)`      |       17711888      |         63.13 ns/op     |      0 B/op    |     0 allocs/op |

### 🔥 CPU cores

**Sync**
| Benchmark | Iterations | Time per operation | Memory per operation | Allocations per operation |
| :--- | :---: | :---: | :---: | :---: |
| `BenchmarkSyncPool`               |       31800453       |        38.35 ns/op     |     24 B/op     |    1 allocs/op |
| `BenchmarkSyncPool-2`             |       33286617       |        36.46 ns/op     |     24 B/op     |    1 allocs/op |
| **`BenchmarkSyncPool-4`**         |     **28256768**     |      **38.16 ns/op**   |   **24 B/op**   |  **1 allocs/op** |
| `BenchmarkSyncPool-8`             |       31688689       |        38.68 ns/op     |     24 B/op     |    1 allocs/op |
| `BenchmarkSyncPool-12`            |       31143446       |        38.36 ns/op     |     24 B/op     |    1 allocs/op |
| `BenchmarkMyPool`                 |       17931403       |        63.46 ns/op     |      0 B/op     |    0 allocs/op |
| `BenchmarkMyPool-2`               |       18287803       |        63.29 ns/op     |      0 B/op     |    0 allocs/op |
| `BenchmarkMyPool-4`               |       17531402       |        63.02 ns/op     |      0 B/op     |    0 allocs/op |
| `BenchmarkMyPool-8`               |       17282780       |        66.77 ns/op     |      0 B/op     |    0 allocs/op |
| **`BenchmarkMyPool-12`**          |     **18127243**     |      **62.89 ns/op**   |    **0 B/op**   |  **0 allocs/op** |

**Parallel**
| Benchmark | Iterations | Time per operation | Memory per operation | Allocations per operation |
| :--- | :---: | :---: | :---: | :---: |
| `BenchmarkSyncPoolParallel`       |       27507099       |        40.30 ns/op     |     24 B/op     |    1 allocs/op |
| `BenchmarkSyncPoolParallel-2`     |       61871604       |        18.75 ns/op     |     24 B/op     |    1 allocs/op |
| `BenchmarkSyncPoolParallel-4`     |       92886445       |        14.40 ns/op     |     24 B/op     |    1 allocs/op |
| **`BenchmarkSyncPoolParallel-8`** |     **78343810**     |      **13.82 ns/op**   |   **24 B/op**   |  **1 allocs/op** |
| `BenchmarkSyncPoolParallel-12`    |       74234456       |        14.80 ns/op     |     24 B/op     |    1 allocs/op |
| `BenchmarkMyPoolParallel`         |       16003455       |        71.48 ns/op     |      0 B/op     |    0 allocs/op |
| `BenchmarkMyPoolParallel-2`       |       12733418       |        92.68 ns/op     |      0 B/op     |    0 allocs/op |
| `BenchmarkMyPoolParallel-4`       |       14521497       |        80.61 ns/op     |      0 B/op     |    0 allocs/op |
| `BenchmarkMyPoolParallel-8`       |       16806204       |        73.39 ns/op     |      0 B/op     |    0 allocs/op |
| **`BenchmarkMyPoolParallel-12`**  |     **17551686**     |      **69.39 ns/op**   |    **0 B/op**   |  **0 allocs/op** |

### 📐 Absolute numbers
- **65 ns/op** - is **0.000000065 seconds** 
- In one second may be performed **~15 millions** operations Get/Put

| Operation | Time | How much slower |
|----------|-------|-------------------------|
| Get from my pool | 65 ns | 1x |
| Get from sync.Pool | 15 ns | 0.23x |
| Mutex Lock | 50 ns | 0.77x |
| Network request (LAN) | 500 μs | 7,692x |
| DB request | 1 ms | 15,384x |
| HTTP request | 10 ms | 153,846x |

### 🔥 Shards

| Benchmark | Iterations | Time per operation | Memory per operation | Allocations per operation |
| :----- | :---: | :---: | :---: | :---: |
| `BenchmarkMyPool-12 (2 shards)`          |   16087342       |        75.65 ns/op     |      0 B/op   |      0 allocs/op |
| `BenchmarkMyPoolParallel-12 (2 shards)`  |    7139690       |       171.1 ns/op      |      0 B/op   |      0 allocs/op |
| `BenchmarkMyPool-12 (4 shards)`          |   11766112       |       103.0 ns/op      |      0 B/op   |      0 allocs/op |
| `BenchmarkMyPoolParallel-12 (4 shards)`  |   10259601       |       132.4 ns/op      |      0 B/op   |      0 allocs/op |
| `BenchmarkMyPool-12 (8 shards)`          |    9280827       |       129.8 ns/op      |      0 B/op   |      0 allocs/op |
| `BenchmarkMyPoolParallel-12 (8 shards)`  |   14836813       |        88.61 ns/op     |      0 B/op   |      0 allocs/op |
| `BenchmarkMyPool-12 (16 shards)`         |     6518593      |        178.7 ns/op     |      0 B/op   |      0 allocs/op |
| **`BenchmarkMyPoolParallel-12 (16 shards)`** |    **15554618**      |       **69.69 ns/op**  |      **0 B/op**   |      **0 allocs/op** |



## 💾 Memory usage

### 📦 Profiling results (memprofile)

**My pool (Parallel)**
| flat | flat% | sum% | cum | cum% | func |
| :--- | :---: | :---: | :---: | :---: | :--- |
| 15661.95kB | 70.11% |  70.11%  | 15661.95kB | 70.11% | `github.com/danilpashin/go-object-pool.BenchmarkMyPoolParallel.func2` |
| 3590.24kB  | 16.07% |  86.18%  | 3590.24kB  | 16.07% | `runtime.mallocgc` |
| 1024.05kB  | 4.58%  |  90.76%  | 1024.05kB  | 4.58%  | `testing.(*B).ResetTimer` |
| 1024.03kB  | 4.58%  |  95.34%  | 6766.75kB  | 30.29% | `testing.(*B).RunParallel.func1` |
| 528.17kB   | 2.36%  |  97.71%  | 528.17kB   | 2.36%  | `regexp.(*bitState).reset` |
| 512.05kB   | 2.29%  |  100%    | 512.05kB   | 2.29%  | `time.NewTicker` |

**sync.Pool (Parallel)**
| flat | flat% | sum% | cum | cum% | func |
| :--- | :---: | :---: | :---: | :---: | :--- |
| 3002.07MB | 80.03% | 80.03% | 3010.70MB | 80.26% | `github.com/danilpashin/go-object-pool.BenchmarkSyncPoolParallel.func2`
| 737.02MB  | 19.65% | 99.68% | 738.04MB  | 19.67% | `github.com/danilpashin/go-object-pool.BenchmarkSyncPool`

*<b><small>0% rows removed</small></b>*

### ⚡**~192x less used memory!** (3 GB vs 15.7 MB)
**sync.Pool (Parallel)**:

  ████████████████████████████████████████████████████████ 3.0 GB

**My Pool (Parallel)**:

  ████████ 15.7 MB


| Factor | sync.Pool | My Pool |
|--------|-----------|--------|
| **Size control** | ❌ No control | ✅ Capacity limits |
| **Cleaning** | ❌ Only GC | ✅ Background janitor |
| **Sharding** | ❌ One pool | ✅ 2<sup>n</sup> shards |
| **Allocations** | ⚠️ 24 B/op | ✅ 0 B/op |
| **Memory** | ⚠️ ~3.7 GB | ✅ ~20 MB |
| **GC load** | 🔴 High | 🟢 Min |


*<small>This project was made with intent of **learning low-level optimizations and understanding concurrency**.</small>*