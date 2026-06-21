# go-spmc-ring

[![Go Reference](https://pkg.go.dev/badge/github.com/pintomau/go-spmc-ring.svg)](https://pkg.go.dev/github.com/pintomau/go-spmc-ring)
[![Latest Release](https://img.shields.io/github/v/release/pintomau/go-spmc-ring)](https://github.com/pintomau/go-spmc-ring/releases/latest)
[![Go Version](https://img.shields.io/github/go-mod/go-version/pintomau/go-spmc-ring)](go.mod)
[![Go Report Card](https://goreportcard.com/badge/github.com/pintomau/go-spmc-ring)](https://goreportcard.com/report/github.com/pintomau/go-spmc-ring)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![codecov](https://codecov.io/gh/pintomau/go-spmc-ring/branch/main/graph/badge.svg)](https://codecov.io/gh/pintomau/go-spmc-ring)

A single-writer, multiple-reader ring buffer in Go, inspired by the LMAX Disruptor pattern. The codebase
focuses on cache-line awareness, false-sharing avoidance, and lock-free reader lifecycle management: unlike the
classic Disruptor, where the consumer graph is wired once before the first event, readers here
[join and leave a live stream at runtime](#adding-and-removing-readers-at-runtime) without locks,
allocations, or writer stalls.

On a Ryzen 5 9600X (Linux, Go 1.26): a single publish takes **~5.2 ns** (≈193 M ops/s), batched
publishes reach **~2.1 ns/item** (≈476 M items/s), and eight concurrent readers cost the writer less
than 7%. On an Apple M4 Pro (macOS) the same publish is **~7.3 ns** and eight readers cost under 2%.
Full measurements for both architectures are in [docs/PERFORMANCE.md](docs/PERFORMANCE.md).

## Why it's fast

- **Single-writer contract.** With exactly one publishing goroutine, sequence bookkeeping needs no
  atomics. The entire hot path synchronizes through one `writeCursor.Store` per publish (or per batch).
- **Cache-line padding everywhere it matters.** The writer's hot fields and every reader's cursor sit
  on their own cache lines, so cores never invalidate each other's lines by accident. The
  false-sharing benchmark family measures exactly this.
- **Lock-free reader lifecycle.** Readers claim slots by CAS on a 2×uint64 bitmap: add and remove are
  wait-free for the writer, and reader goroutines are pooled and reused across registrations.
- **Cached gating.** The writer caches the slowest reader's position and only rescans all cursors
  when that cache is exhausted or the reader bitmap changes, keeping the common case scan-free.
- **Batch primitives.** `Reserve`/`Commit` exposes the ring's backing array directly and makes an
  entire batch visible with a single atomic store, amortizing the fixed publish overhead to ~2.1 ns/item.
- **Devirtualized waiting.** Wait strategies are a `uint8` switch rather than an interface, so the
  backpressure loop pays no dynamic-dispatch cost.

## Install

```bash
go get github.com/pintomau/go-spmc-ring
```

Requires Go 1.24+. No third-party dependencies and no cgo.

## Quick start

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

rb, _ := spmc.NewRingBuffer[int](ctx, 1024) // capacity rounds up to a power of 2

// Readers live in a Stage. Gate the writer on the stage so it
// never laps the slowest reader.
s := rb.NewStage(nil) // nil upstream = gated by the writer cursor
rb.SetGatingStage(s)  // must happen before the first Publish

slotID, _ := s.AddReader(func(ctx context.Context, rv spmc.ReadView[int], cur *atomic.Int64) {
	expected := cur.Load() + 1
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if w := rv.LoadWriterBarrier(); expected <= w {
				for seq := expected; seq <= w; seq++ {
					process(*rv.Get(seq))
				}
				cur.Store(w) // the reader owns its cursor: advance it or the writer stalls
				expected = w + 1
			} else {
				time.Sleep(50 * time.Microsecond) // idle backoff (see "Writing a reader")
			}
		}
	}
})

for i := range 1_000_000 {
	rb.Publish(i)
}

s.RemoveReader(slotID)
s.Shutdown()
```

## Architecture

```
                 ┌─────────────────────────────────────────────────────┐
 Writer          │ RingBuffer[T]                                       │
 (exactly one    │   buffer []T          power-of-2, mask-indexed      │
  goroutine)     │   writeCursor         atomic, one Store per publish │
   │             │   nextSequence        writer-private, no atomics    │
   ├─ Publish ──►│   cachedSlowestReader writer-private gate cache     │
   ├─ PublishBatch                                                     │
   └─ Reserve/Commit                     (hot fields cache-line padded)│
                 └──────────────┬──────────────────────────────────────┘
                                │ writeCursor visible to readers
                                ▼
                 ┌─────────────────────────────────────────────────────┐
                 │ Stage 1 (BitmapReaderPool)                          │
                 │   2×uint64 bitmap → up to 128 reader slots          │
                 │   one padded cursor per reader (no false sharing)   │
                 │   reader goroutines pooled & reused                 │
                 └──────────────┬──────────────────────────────────────┘
                                │ Barrier() = min(stage-1 cursors)
                                ▼
                 ┌─────────────────────────────────────────────────────┐
                 │ Stage 2 …N (optional pipeline stages)               │
                 │   readers only see events stage N−1 has committed   │
                 └──────────────┬──────────────────────────────────────┘
                                │ min cursor of the leaf stage
                                ▼
                  writer backpressure (SetGatingStage):
                  Publish blocks when the buffer would lap the
                  slowest reader of the gating stage
```

The writer is single-threaded by contract, so its sequence bookkeeping needs no atomics. One
`writeCursor.Store` per publish (or per batch) is the only synchronization on the hot path. Readers
register into a 128-slot bitmap pool. Each reader's cursor sits on its own cache line, and the writer
only rescans all cursors when the bitmap topology changes or its cached gate is exhausted.

## Usage patterns

### Writing a reader

A `ReaderFunc` runs on its own pooled goroutine and **owns its cursor**: advance `cur` after consuming
or the writer will eventually stall waiting for you. The canonical loop is in the quick start above.
Two details that matter in practice:

- See [Throughput vs latency](#throughput-vs-latency-choosing-spin-yield-and-sleep) below.
- **Batch your cursor stores.** Consume everything up to `LoadWriterBarrier()` and store the cursor
  once per batch, not once per event.

### Adding and removing readers at runtime

Readers are not a startup-time decision. While the writer publishes at full rate, you can attach a
metrics tap or debug subscriber to a live stream, run a canary consumer, or drain and detach a
reader before reconfiguring:

```go
tapID, _ := s.AddReader(metricsTap) // writer keeps publishing throughout

// ... observe for a while ...

s.RemoveReader(tapID)
```

What makes this safe and cheap:

- **`AddReader` is lock-free.** The slot is claimed by a CAS on the reader bitmap. The writer is
  never blocked and simply picks up the new gating cursor on its next scan.
- **A new reader starts at the writer's current position.** It sees only events published after it
  joined. There is no replay of history.
- **`RemoveReader` takes effect immediately from the writer's perspective.** The slot leaves the
  gating set before the reader goroutine finishes winding down, so a slow reader that gets removed
  can never stall the writer again.
- **Goroutines are pooled.** A removed reader's goroutine idles with exponential backoff and
  self-terminates after about a minute, so add/remove churn reuses live goroutines instead of
  paying spawn costs.

The dynamic part is the readers, not the topology: stages and the gating barrier must still be
wired before the first publish (see [Pipeline stages](#pipeline-stages)).

### Consuming in batches

`ReadView` offers three access paths, and all of them take absolute sequence numbers while masking
is internal. `Get(seq)` returns a single event. `GetSegments(start, end)` returns up to two
zero-copy slices spanning start..end inclusive. The second is non-nil only when the range
wraps the ring end, mirroring the two-segment shape `Reserve` gives the writer.
`Iterate(start, end)` yields each event in order via an iterator.

```go
if w := rv.LoadWriterBarrier(); expected <= w {
	seg1, seg2 := rv.GetSegments(expected, w) // zero-copy, even when the range wraps
	for i := range seg1 {
		process(seg1[i])
	}
	for i := range seg2 {
		process(seg2[i])
	}
	cur.Store(w)
	expected = w + 1
}
```

Both segments point straight into the ring, so the batch must be fully consumed before the
cursor is advanced: after `cur.Store(w)` the writer may overwrite those slots. `Iterate` is
built on `GetSegments` and measures at parity with direct segment loops when the body
inlines. Reach for `GetSegments` when you need the slices themselves, such as bulk copies
or vector processing. All read paths are allocation-free, wrapping included (see
[Batch read paths](docs/PERFORMANCE.md#batch-read-paths)).

### Publishing in batches

Single-item `Publish` pays a fixed ~3.1 ns of gating and cursor-store overhead per call. Batching
amortizes it (~2.1 ns/item at batch ≥ 10, see [Batch scaling](docs/PERFORMANCE.md#batch-scaling)).

```go
// Copy a prepared slice:
rb.PublishBatch(events)

// Fill slots in place (no intermediate slice):
rb.PublishBatchFunc(n, func(i int64, slot *Event) { slot.ID = i })

// Zero-overhead variant that writes directly into the ring:
seg1, seg2, claim := rb.Reserve(n) // seg2 non-nil only when the batch wraps the ring end
fill(seg1)
fill(seg2)
rb.Commit(claim) // one atomic store makes the whole batch visible
```

`Reserve` requires `0 < n < bufferSize` (a full-buffer reservation can never be satisfied and panics),
and every `Reserve` must be paired with exactly one `Commit`, in order.

### Non-blocking publishing

`Publish` blocks when the ring is full. When dropping or deferring work beats waiting on a
slow reader, use the try variants. They return `false` instead of waiting:

```go
if !rb.TryPublish(event) {
    // ring full: drop the event, buffer upstream, or push back on the source
}

if rb.Remaining() < lowWater {
    // adapt before hitting the wall: shed load or switch to batching
}
```

`TryReserve` is the batch sibling: it returns `ok = false` when the ring lacks room for the
whole batch. The try variants share the writer's contract, so call them only from the
writer goroutine.

A slow reader can also opt out from its side: returning from a `ReaderFunc` removes the
reader from the pool and ungates the writer. The reader picks its own exit point, so it is
never mid-read when its slots are reclaimed. See the self-evicting reader example in the
package docs.

### Pipeline stages

Stages chain reader groups so stage N only consumes events all of stage N−1 has committed, which is the LMAX
barrier-group pattern. Example: journal every event before business logic is allowed to see it.

```go
s1 := rb.NewStage(nil)          // gated by the writer cursor
s2 := rb.NewStage(s1.Barrier()) // gated by s1's slowest reader
rb.SetGatingStage(s2)           // writer waits for the leaf stage

s1.AddReader(journalFn)
s2.AddReader(businessFn)

// Each stage shuts down independently:
defer s1.Shutdown()
defer s2.Shutdown()
```

Constraints:

- `SetGatingStage` must be called **before the first publish**, because the gating field is not synchronized.
- Wire downstream stages with `Barrier()` (concurrent-safe), never with `Load()` (writer-only cache path).
- Depth is effectively free: overhead tracks *total* reader count, not stage count
  (see [Stage / pipeline scaling](docs/PERFORMANCE.md#stage--pipeline-scaling)).

### Wait strategies

The writer's backpressure behavior when the buffer is full is a `WaitStrategy` (a `uint8` switch, not
an interface, to keep the wait loop devirtualized):

| Strategy             | Behavior                      |
|----------------------|-------------------------------|
| `WaitStrategySpin`   | busy-spin                     |
| `WaitStrategyYield`  | `runtime.Gosched()` (default) |
| `WaitStrategySleep`  | `time.Sleep`                  |
| `WaitStrategyHybrid` | spin N times, then sleep      |

```go
rb, _ := spmc.NewRingBuffer[Event](ctx, 1<<16,
	spmc.WithWaitStrategy[Event](spmc.WaitStrategySpin))
```

Which one to pick, and how the same choice plays out differently for writers and readers, is covered
in [Throughput vs latency](#throughput-vs-latency-choosing-spin-yield-and-sleep) below.

### Things to know

- **One writer, always.** All publish methods assume a single publishing goroutine. There is no
  multi-producer mode.
- Capacity rounds up to the next power of two.
- With no readers registered, the writer free-runs and never blocks.
- Readers are limited to 128 per stage (the 2×64-bit bitmap). `AddReader` returns an error when the
  stage is full.

## Performance

Headline numbers, arithmetic mean of 10 runs, on two machines: x86-64 (Ryzen 5 9600X, 6C/12T,
Linux) and arm64 (Apple M4 Pro, 14C/14T, macOS), both Go 1.26.2:

| Measurement                                  | x86-64 (Ryzen)                       | arm64 (M4 Pro)                                                                    |
|----------------------------------------------|--------------------------------------|-----------------------------------------------------------------------------------|
| Single `Publish`                             | 5.2 ns/op (193 M ops/s)              | 7.3 ns/op (138 M ops/s)                                                           |
| Batch publish, size ≥ 10                     | 2.1–2.55 ns/item (all APIs converge) | 1.76 ns/item floor (bulk `PublishBatch`, per-item cost is fill-pattern dependent) |
| 8 concurrent readers                         | +6.2% writer cost vs. 1 reader       | +1.5% vs. 1 reader                                                                |
| 128 readers (capacity limit)                 | 3.0× single-reader cost              | 2.4× single-reader cost                                                           |
| Pipeline depth (2–3 stages, ≤4 readers each) | within noise of single-stage         | within noise of single-stage                                                      |
| Best end-to-end p99 (burst workload)         | 13.2µs (Yield wait, batch poll)      | 9.0µs (Hybrid wait, batch poll)                                                   |

Absolute numbers capture the whole platform (CPU, scheduler, OS timer), not just the CPU. See the
[per-section commentary](docs/PERFORMANCE.md) for where the two architectures diverge in *shape*
(notably the `Reserve` vs `PublishBatch` ordering, the multi-reader cliff position, and the FixedRate
`Spin` vs `Batch` poll recommendation). The full data, including multi-reader scaling, the
`LockOSThread` study, batch-size sweeps, and the complete latency matrix with HDR percentiles, is in
[docs/PERFORMANCE.md](docs/PERFORMANCE.md).

### Throughput vs latency: choosing spin, yield, and sleep

Three independent decisions hide behind "wait strategy", and the right answer is different for each.
Two mechanisms drive all of it:

1. **`time.Sleep` has a wakeup floor.** Asking Linux for a 1µs sleep takes 15–50µs in practice (Go
   runtime timer plus OS scheduler wakeup). Any sleep on an event-delivery path donates that floor
   to your latency percentiles.
2. **`runtime.Gosched` does not park.** It yields the processor but re-enters the run queue
   immediately, so an idle loop built on it busy-spins *through the scheduler*, churning run queues
   and stealing cycles from the writer. Measured cost: −157% writer throughput with 8 idle-spinning
   readers.

| Decision point                           | Best choice                                               | Why                                                                                                                     |
|------------------------------------------|-----------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| Writer backpressure (`WithWaitStrategy`) | `Yield` (default), or `Spin` when latency-critical           | the buffer-full path is rare when the ring is sized right, so politeness is cheap. spin only buys the last microseconds |
| Reader polling while events flow         | drain to the barrier each pass, never sleep between polls | every sleep adds the 15–50µs wakeup floor to every event that arrives during it                                         |
| Reader idle after catching up            | `time.Sleep(50µs)`, never `Gosched`                       | sleep parks the goroutine and frees the scheduler. Gosched creates a scheduler storm                                    |

**sleeping is the right way to idle** (it parks the goroutine, protecting throughput) **and the wrong way to poll**
(it pays the wakeup floor per event, destroying latency). Workload-specific recommendations,
including how the picture changes at p99.9 and beyond, are in
[Tail-aware recommendations](docs/PERFORMANCE.md#tail-aware-recommendations).

### Other findings worth knowing

- **Batching pays for itself by size 10.** The ~3.1 ns fixed cost per publish (gating check plus
  cursor store) amortizes away. On x86 the per-item cost is then flat at the payload-write floor. On
  arm64 it dips into a mid-size hump before reaching its lowest at large batches (see
  [Batch scaling](docs/PERFORMANCE.md#batch-scaling)).
- **Reader scaling is sub-linear up to the hardware thread count**, then degrades as the writer's
  full cursor scan starts to dominate.
- **Publishing is O(1) in reader count. A channel-based broadcast is O(readers).** Fanning the same
  event out through one buffered channel per reader costs 6x the ring at a single reader and ~288x at
  128, because every channel needs its own send while the ring's readers all watch one cursor (see
  [Baseline: channel fan-out](docs/PERFORMANCE.md#baseline-channel-fan-out)).
- **Batching closes most of the per-op gap, but not the allocation gap.** A `chan []T` fan-out cuts
  per-item cost 17x (266 to 15 ns/item from batch size 1 to 1000), yet every batch still allocates
  ~64 B per item, while the ring publishes batches in place with zero allocations (see
  [Batched insert and read](docs/PERFORMANCE.md#batched-insert-and-read)).
- **`runtime.LockOSThread` does not pay.** Go's scheduler beats manual OS-thread pinning for this
  workload at almost every reader count. The idle strategy matters far more than pinning.
- **Batch-draining readers get *more* valuable at higher percentiles.** At p99.9+ they win in almost
  every measured scenario.

## SIMD segment fills (experimental)

A `GOEXPERIMENT=simd` experiment (amd64, Go 1.26+, not part of the shipped library) found one pattern that
pays: filling `Reserve` segments with vector-*generated* payloads runs about **3.5× a scalar
`PublishBatchFunc`** at batch ≥ 64 while the ring is cache-resident. Plain copies gain nothing
(`memmove` is already vectorized) and the win disappears at DRAM bandwidth. Full tables, alignment
findings, and the reader-side analysis:
[SIMD segment fills](docs/PERFORMANCE.md#simd-segment-fills-experimental). Recipe in
[`example_simd_test.go`](example_simd_test.go).

## How it's tested

- **Property-based simulation** ([`pgregory.net/rapid`](https://pkg.go.dev/pgregory.net/rapid)):
  randomized writers, batch sizes, and reader add/remove churn are checked against ordering and
  visibility invariants. The full suite runs in minutes locally. A nightly CI job runs each
  property at a check budget sized to its cost, from hundreds of cases for the heaviest
  property to tens of thousands for the cheapest.
- **Deterministic replay.** When a simulation fails, rapid saves a failfile under
  `testdata/rapid/` and the next test run replays it automatically. A specific run can be
  reproduced with `RAPID_SEED=<seed> go test -run Simulation .`.
- **Multi-platform CI.** Build and short tests across Linux, macOS, and Windows on Go 1.24–1.26,
  plus golangci-lint and CodeQL.
- **Coordinated-omission-aware latency harness.** `internal/cmd/latency` stamps events with their *intended*
  dispatch time and records end-to-end, writer-stall, and reader-lag percentiles in HDR histograms,
  so writer stalls can't hide queueing delay.

## License

MIT, see [LICENSE](LICENSE).
