# Benchmarks and latency measurements

Complete performance data for [go-spmc-ring](../README.md). For headline numbers and configuration guidance, see the README's [Performance](../README.md#performance) section.

## Test setup and methodology

| Machine    | OS    | CPU                                | Cores / threads         | Cache line | Go     |
|------------|-------|------------------------------------|-------------------------|-----------:|--------|
| **x86-64** | Linux | AMD Ryzen 5 9600X 6-Core Processor | 6C / 12T (SMT)          |       64 B | 1.26.2 |
| **arm64**  | macOS | Apple M4 Pro                       | 14 cores / 14T (no SMT) |      128 B | 1.26.2 |

Results are machine-specific. We run each command twice and report the lower-mean set, dropping a run's first element when it exceeds the mean of the rest by >5% (warm-up filter).

Both architectures are shown side-by-side. The shape of a curve matters more than absolute numbers.

Command used (`mise run bench:core`):

```bash
go test -run '^$' -bench 'BenchmarkRingBuffer_(Publish$|Publish_NoReaders$|Publish_Direct$|PublishBatch$|PublishBatchFunc$|Reserve$)' -count=10 .
```

## How fast is publishing?

The single-event publish path runs at ~5.2 ns on x86 and ~7.3 ns on arm64. Per-event construction adds roughly 77% on x86 (a store-to-load forwarding stall) but only 20% on arm64. Here are the three core shapes:

| Benchmark                                               | x86-64 ns/op | x86-64 throughput | arm64 ns/op | arm64 throughput | Notes                                                                                                    |
|---------------------------------------------------------|-------------:|------------------:|------------:|-----------------:|----------------------------------------------------------------------------------------------------------|
| [`Publish_NoReaders`](../ring_buffer_bench_test.go#L88) |        5.097 |     196.2 M ops/s |       6.967 |    143.5 M ops/s | Writer upper bound with no registered readers                                                            |
| [`Publish`](../ring_buffer_bench_test.go#L22)           |        5.188 |     192.8 M ops/s |       7.258 |    137.8 M ops/s | Keep-up reader adds ~1.8% (x86) / ~4.2% (arm64) overhead vs. no readers                                  |
| [`Publish_Direct`](../ring_buffer_bench_test.go#L131)   |        9.206 |     108.6 M ops/s |       8.725 |    114.6 M ops/s | Constructs and publishes a value per event; see the decomposed breakdown below                           |

arm64 is ~40% slower in absolute ns (7.26 vs 5.19 ns). The reader overhead share is also larger (4.2% vs 1.8%): with no Simultaneous Multithreading (SMT), the keep-up reader competes for a full physical core instead of a sibling hardware thread.

### The Publish_Direct gap, decomposed

`Publish_Direct` measures 9.21 ns against `Publish`'s 5.19 ns. That 1.77× isn't a publish-API penalty. It's the per-event construction cost.

| Variant                                                                        | x86-64 ns/op | arm64 ns/op | What it isolates                              |
|--------------------------------------------------------------------------------|-------------:|------------:|-----------------------------------------------|
| [`PublishFunc`, write 1 byte in the slot](../ring_buffer_bench_test.go#L244)   |         5.12 |        7.23 | The standing `Publish` benchmark workload     |
| [`PublishFunc`, write all 64 B in the slot](../ring_buffer_bench_test.go#L203) |         5.13 |       43.95 | Full payload traffic through the callback API |
| [`Publish` by value, payload prepared once](../ring_buffer_bench_test.go#L183) |         5.49 |        5.46 | The honest by-value API cost                  |
| [`Publish` by value, value built per event](../ring_buffer_bench_test.go#L224) |         9.18 |        8.46 | The original `Publish_Direct` shape           |

<details>
<summary>Deep dive: arm64 Publish_Direct behavior</summary>

The `PublishFunc` full-fill row jumps to ~44 ns on the M4 Pro. That's not from the callback or the payload size. It's a cold-line write stall.

The `*slot = payload` assignment compiles to partial vector stores. The first store into a cold 128 B line triggers a read-for-ownership. Using `copy(slot[:], payload[:])` instead lowers to `memmove`'s full-line write and drops the row to ~4 ns.

`PublishBatchFunc` with a batch of one costs ~6 ns for the same struct copy. The `Reserve`/`Commit` bookkeeping gives the core independent instructions to overlap the stall. The tight single-publish loop has nothing to hide it behind. In real batches (`PublishBatchFunc_Fill`) the cost drops to ~3 ns/item.

By-value `Publish` (5.46 ns) is actually the fastest of the four on arm64. The struct copy compiles to the same vector load/store pairs on both machines (16 B `MOVUPS` on x86, 32 B `FLDPQ`/`FSTPQ` on arm64). The release store is heavier on x86 (`XCHGQ`, a full barrier) than on arm64 (`STLR`, one-way release). x86 runs the whole family flat at ~6 ns regardless of fill. The M4 stalls only in the tight struct-copy loop, where the larger line and the un-hidden read-for-ownership combine.

For cache-line-sized payloads in a hot single-publish loop on arm64: avoid `*slot = payload`. Use `copy` into the slot, batch with `Reserve`/`Commit` or `PublishBatchFunc`, or `Publish` a prepared value. Any of those land at ~4 to 6 ns. All are neutral or faster on x86, so no architecture-specific code path is needed.

</details>

<details>
<summary>Deep dive: why per-event construction costs more</summary>

Three findings, x86-64 (see the arm64 section above for how it diverges):

- **Writing 64 bytes costs the same as writing 1 byte.** The cache-line ownership transfer between writer and reader dominates, and it's paid either way. Payload size isn't the gap.
- **By-value publish itself costs ~7%** (5.49 vs 5.12 ns): one 64 B argument copy plus identical call structure. That's the real `Publish` vs `PublishFunc` API difference.
- **The remaining ~3.7 ns is the per-event construction pattern, not the API.** Building a fresh composite literal right before the call re-reads those bytes while the construction stores are still in flight. That defeats store-to-load forwarding and stalls the pipeline every iteration.

</details>

For cache-line-sized events, prefer `PublishFunc` (or `Reserve`/`Commit` for batches). Build the event directly in the ring slot. It skips both copies and the stall entirely. If you must publish by value, reuse a prepared value instead of constructing a fresh composite literal right before the call.

## How does it scale with multiple readers?

Each row sweeps reader count for the [`Publish_MultiReader`](../ring_buffer_bench_test.go#L103) benchmark. Degradation stays sub-linear up to the hardware thread count on both machines.

Command used (`mise run bench:multireader`):

```bash
go test -run '^$' -bench 'BenchmarkRingBuffer_Publish_MultiReader$' -count=10 .
```

| Readers | x86-64 ns/op | x86-64 slowdown | arm64 ns/op | arm64 slowdown | Notes                                                        |
|--------:|-------------:|----------------:|------------:|---------------:|--------------------------------------------------------------|
|       1 |        5.442 |          1.000x |       7.255 |         1.000x | Single keep-up reader                                        |
|       2 |        5.495 |          1.010x |       7.256 |         1.000x | Essentially flat vs. 1 reader                                |
|       4 |        5.535 |          1.017x |       7.289 |         1.005x | Small additional gating cost                                 |
|       8 |        5.780 |          1.062x |       7.367 |         1.015x | Modest overhead; bitmap scan amortized across few readers    |
|      16 |        6.274 |          1.153x |      12.650 |         1.744x | arm64 steps here (>14 threads); x86 still smooth             |
|      32 |        7.652 |          1.406x |      14.419 |         1.988x | Both accelerate as readers exceed physical thread count      |
|      64 |       10.720 |          1.970x |      14.869 |         2.050x | x86 near 2× slower; arm64 overhead plateaus                  |
|     128 |       16.550 |          3.041x |      17.298 |         2.385x | Bitmap capacity limit; arm64 degrades *less* at the high end |

- **x86-64 (Ryzen 5 9600X, 6C/12T)**: overhead is 6.2% at 8 readers and stays moderate through 16. It accelerates past the 12-thread limit, reaching ~2× at 64 and 3× at 128.
- **arm64 (M4 Pro, 14C/14T, no SMT)**: nearly flat through 8 readers (1.5% overhead). Then a sharp step at 16 (7.37 → 12.65 ns, +72%) when readers plus the writer exceed the 14 hardware threads. Past that the curve is gentler than x86's, reaching 2.39× at 128 vs. x86's 3.04×.

Past ~32 readers the primary bottleneck is `scanAll`, which scans all 128 cursor slots to find the minimum on every slow-path call.

Thread pinning doesn't help. Go's scheduler outperforms it for this workload.

<details>
<summary>Deep dive: LockOSThread testing</summary>

**[`runtime.LockOSThread`](../ring_buffer_bench_test.go#L621)**: pinning each reader and the writer to their own OS thread was tested with both `time.Sleep` and `runtime.Gosched()` idle strategies. Key findings:

- **Locked + Sleep**: universally worse. Catastrophic at 128 readers (+63%). 129 OS threads sleep/wake on 12 hardware threads, thrashing the scheduler.
- **Locked + Gosched**: worse at 1–32 readers (peak −40% at 8 readers). Marginally faster at 64–128 readers (~3–6%). The improvement at high counts comes from cursor cache locality: each locked thread keeps its slot warm on the same physical core. The hurt at low counts comes from M:P scheduling overhead (the locked M sits idle each time `Gosched` yields its P).
- **Unlocked + Gosched on keepUpReader**: dramatically worse everywhere (peak −157% at 8 readers). `Gosched` in a tight idle loop doesn't park the goroutine. It keeps re-entering the run queue, starving the writer. `time.Sleep` is better because it genuinely removes the goroutine from the run queue.

**Conclusion**: Go's GOMAXPROCS scheduler outperforms OS thread pinning for this workload. `LockOSThread` is only worth revisiting at ≥64 readers with a `Gosched`-based idle and continuous spinning, and even then the gain is marginal. The idle backoff strategy matters more than thread pinning.

> *This sweep was measured on x86-64/Linux only. Thread pinning interacts heavily with the OS scheduler, so don't assume these deltas carry over to Apple silicon.*

</details>

## How does it compare to channels?

The ring's fan-out is ~288x faster than channels at 128 readers. The gap widens with every reader you add.

A plain Go channel needs one send per reader per event. Per-event cost grows linearly with reader count. Ring publishing is O(1) because every reader observes one writer cursor (the bitmap scan adds a small sub-linear cost at high counts).

Command used (`mise run bench:chanbaseline`), measured on x86-64 (Ryzen 5 9600X). The same command also produces the batched table below.

```bash
go test -run '^$' -bench '^BenchmarkChannel' -benchmem -count=10 .
```

| Readers | Ring `Publish` ns/op | [Channel fan-out](../channel_baseline_bench_test.go#L36) ns/op | Channel slowdown vs. 1 reader | Channel ÷ ring |
|--------:|---------------------:|---------------------------------------------------------------:|------------------------------:|---------------:|
|       1 |                5.442 |                                                          33.46 |                        1.000x |           6.1x |
|       2 |                5.495 |                                                          68.24 |                        2.040x |          12.4x |
|       4 |                5.535 |                                                         117.22 |                        3.504x |          21.2x |
|       8 |                5.780 |                                                         223.44 |                        6.678x |          38.7x |
|      16 |                6.274 |                                                         477.18 |                       14.262x |          76.1x |
|      32 |                7.652 |                                                         935.80 |                       27.968x |         122.3x |
|      64 |               10.720 |                                                        2201.00 |                       65.780x |         205.3x |
|     128 |               16.550 |                                                        4765.20 |                      142.421x |         287.9x |

<details>
<summary>Note on fairness</summary>

The channel buffer is capped at `1<<12` per reader (the ring runs at `1<<22`), because a ring-sized buffer per reader would cost gigabytes at 128 readers. With readers that keep up, the writer rarely blocks. This measures per-send channel-op cost rather than backpressure. A shared single channel wasn't used because that's work-distribution, not broadcast, and wouldn't deliver every event to every reader.

</details>

### Batching on channels

Channels can batch too: send a slice of events per reader instead of individual items. This is the stdlib equivalent of the ring's `PublishBatch` (one `writeCursor.Store` per batch). Reader count is fixed at 8 and the sweep is over batch size.

| Batch size | Ring [`PublishBatch`](../ring_buffer_bench_test.go#L376) ns/item | Ring allocs/op | [Channel batched fan-out](../channel_baseline_bench_test.go#L103) ns/item | Channel B/op | Channel allocs/op | Channel ÷ ring |
|-----------:|-----------------------------------------------------------------:|---------------:|--------------------------------------------------------------------------:|-------------:|------------------:|---------------:|
|          1 |                                                            7.387 |              0 |                                                                    266.40 |           64 |                 1 |          36.1x |
|         10 |                                                            2.218 |              0 |                                                                     56.93 |          640 |                 1 |          25.7x |
|        100 |                                                            2.478 |              0 |                                                                     20.46 |         6528 |                 1 |           8.3x |
|       1000 |                                                            2.395 |              0 |                                                                     15.45 |        65536 |                 1 |           6.5x |

Batching drops per-item cost 17x (266 to 15.5 ns/item). But every batch allocates a fresh backing array (~64 B per item) because the slice can't be reused until the slowest reader is done. The ring writes into its pre-allocated buffer with zero allocations and ~2.1 to 2.5 ns/item. Even at batch size 1000 the batched channel is ~6.5x slower and allocates memory the ring never touches.

## Can I pipeline stages?

Yes, and they're essentially free at low reader counts. Adding pipeline depth doesn't cost anything on its own. Overhead scales with total readers across all stages, not depth.

Command used (`mise run bench:pipeline`):

```bash
go test -run '^$' -bench '^BenchmarkPipeline' -benchtime=1s -count=10 .
```

All four variants use `keepUpReader` (`time.Sleep(50µs)` idle). `1Stage_NoPipeline` registers readers directly on `rb.barrier`. `1Stage` adds one explicit `Stage[T]`. `2Stage` / `3Stage` add depth. "Readers per stage" means N × depth total readers.

**x86-64 (Ryzen 5 9600X):**

| Readers per stage | [`1Stage_NoPipeline`](../ring_buffer_pipeline_bench_test.go#L12) ns/op | [`1Stage`](../ring_buffer_pipeline_bench_test.go#L37) ns/op | [`2Stage`](../ring_buffer_pipeline_bench_test.go#L66) ns/op | [`3Stage`](../ring_buffer_pipeline_bench_test.go#L98) ns/op | Notes                                                                                                        |
|------------------:|-----------------------------------------------------------------------:|------------------------------------------------------------:|------------------------------------------------------------:|------------------------------------------------------------:|--------------------------------------------------------------------------------------------------------------|
|                 1 |                                                                  5.391 |                                                       5.656 |                                                       5.446 |                                                       5.409 | All variants within 9% of core publish baseline; Stage API overhead is small at 1 reader                     |
|                 2 |                                                                  5.555 |                                                       5.777 |                                                       5.514 |                                                       5.552 | Adding a second reader per stage costs essentially nothing; total readers = 2 / 4 / 6                        |
|                 4 |                                                                  5.625 |                                                       5.805 |                                                       5.689 |                                                       5.834 | Still under 6 ns across the board; 3Stage has 12 total readers                                               |
|                 8 |                                                                  5.854 |                                                       6.005 |                                                       6.112 |                                                       6.613 | 3Stage shows first meaningful overhead: 24 total readers, scanAll runs across 3 stages                       |
|                16 |                                                                  6.262 |                                                       6.504 |                                                       7.281 |                                                       8.669 | Overhead now clearly tied to total reader count (16 / 32 / 48); depth multiplies the scanAll cost            |
|                32 |                                                                  7.753 |                                                       7.915 |                                                      10.180 |                                                      12.940 | 2Stage doubles cost vs 1Stage (96 vs 32 total); 3Stage at 96 total readers approaching MultiReader territory |
|                64 |                                                                 10.710 |                                                      10.900 |                                                      16.070 |                                                      17.480 | 1Stage matches MultiReader/r=64 baseline; 2Stage and 3Stage reflect 128 and 192 total readers                |
|               128 |                                                                 16.490 |                                                      16.690 |                                                      23.090 |                                                      33.380 | 1Stage matches MultiReader/r=128; 3Stage at 384 total readers is 2× worse than 1Stage                        |

**arm64 (Apple M4 Pro):**

| Readers per stage | `1Stage_NoPipeline` ns/op | `1Stage` ns/op | `2Stage` ns/op | `3Stage` ns/op | Notes                                                                                        |
|------------------:|--------------------------:|---------------:|---------------:|---------------:|----------------------------------------------------------------------------------------------|
|                 1 |                     7.221 |          7.267 |          7.208 |          7.161 | All four indistinguishable from the single-reader publish baseline (~7.2 ns)                 |
|                 2 |                     7.215 |          7.292 |          7.280 |          7.201 | Second reader per stage is free; total readers = 2 / 4 / 6                                   |
|                 4 |                     7.304 |          7.330 |          7.359 |          7.309 | Still ~7.3 ns across the board; 3Stage has 12 total readers                                  |
|                 8 |                     7.402 |          7.405 |          8.037 |          8.748 | 3Stage first to move (24 total readers), but still well under the 14-thread cliff            |
|                16 |                    12.262 |         12.675 |         13.532 |         13.661 | All variants step together as total readers cross 14 hardware threads (16 / 32 / 48)         |
|                32 |                    14.351 |         14.313 |         14.068 |         13.850 | Flat across depth; past the thread cliff, depth barely matters on arm64                      |
|                64 |                    15.104 |         15.190 |         14.824 |         16.226 | 1Stage tracks MultiReader/r=64; 2Stage/3Stage stay close                                     |
|               128 |                    17.151 |         17.337 |         19.031 |         23.853 | 1Stage matches MultiReader/r=128; 3Stage at 384 total readers is ~1.4× 1Stage (vs 2× on x86) |

Key observations:

1. At 1–4 readers per stage, all variants are effectively free. Costs cluster at the core publish baseline (x86 ~5.4–5.8 ns, arm64 ~7.2–7.4 ns) and are indistinguishable across depth.
2. Overhead scales with total readers, not pipeline depth. On x86, `2Stage/r=16` (7.28 ns, 32 total) ≈ `MultiReader/r=32` (7.65 ns). Arm64 shows the same pattern, but all depths step together at the 14-thread cliff.
3. Depth costs less on arm64 at the high end. The Ryzen's `3Stage/r=128` is 2× its `1Stage`. The M4's is only ~1.4×. Past the thread cliff, per-reader scan cost grows more slowly.
4. `1Stage_NoPipeline` and `1Stage` track closely. The `Stage[T]` wrapper adds ~1–5% on x86 and is within noise (<1%) on arm64.

## How much does batching help?

Batching cuts per-item cost by 3x to 4x. At batch size 10, you're already near the floor.

The batch benchmarks vary along two axes: fill size (full slot vs. one byte) and API shape (`Reserve` inline loop, `PublishBatchFunc` closure, or `PublishBatch` bulk copy). They all sit on top of `Reserve`/`Commit`, so every variant issues exactly one `writeCursor.Store` per batch.

<details>
<summary>Deep dive: what the batch benchmarks measure</summary>

`PublishBatch` and `PublishBatchFunc` are both implemented on top of `Reserve`/`Commit` (see `ring_buffer.go`). They differ only in the per-slot fill loop between `Reserve` and `Commit`, along two axes:

- **fill size**: full-slot write (64 B on x86, 128 B on arm64) vs. one-byte poke
- **API shape**: inline loop (`Reserve`), per-element closure (`PublishBatchFunc`), or bulk `copy()` (`PublishBatch`)

The slot type is `[CacheLineSize]byte`. The ring is `1<<22` slots (256 MB on x86, 512 MB on arm64), far larger than any cache. The writer marches through cold memory with the keep-up reader chasing it. Numbers below are ns/item, lower of two runs.

</details>

Every column below writes the whole slot. This is the equal-work comparison: per-item cost reflects API and access-pattern cost, not fill size.

*x86-64 (Ryzen 5 9600X):*

| Batch size | [`PublishBatch`](../ring_buffer_bench_test.go#L376) (bulk copy) | [`Reserve_Fill`](../ring_buffer_bench_test.go#L465) (inline full) | [`PublishBatchFunc_Fill`](../ring_buffer_bench_test.go#L500) (closure full) |
|-----------:|----------------------------------------------------------------:|------------------------------------------------------------------:|----------------------------------------------------------------------------:|
|          1 |                                                           7.387 |                                                             7.029 |                                                                       7.261 |
|         10 |                                                           2.218 |                                                             2.212 |                                                                       2.255 |
|        100 |                                                           2.478 |                                                             2.199 |                                                                       2.195 |
|       1000 |                                                           2.395 |                                                         **2.146** |                                                                       2.184 |

*arm64 (Apple M4 Pro):*

| Batch size | `PublishBatch` (bulk copy) | `Reserve_Fill` (inline full) | `PublishBatchFunc_Fill` (closure full) |
|-----------:|---------------------------:|-----------------------------:|---------------------------------------:|
|          1 |                      5.531 |                        4.839 |                                  5.986 |
|         10 |                      2.658 |                        2.584 |                                  3.252 |
|        100 |                      4.939 |                        6.333 |                                  5.782 |
|       1000 |                  **1.762** |                    **1.681** |                                  3.068 |

At large batches, full-fill paths sit near one floor on each machine (**~2.1–2.5 ns/item on x86, ~1.7–3.1 on arm64**). Pick the API for ergonomics. On arm64, fill the whole slot. On x86, fill shape doesn't matter.

<details>
<summary>Deep dive: one-byte vs full-slot fill</summary>

Swapping the full-slot fill for a one-byte poke (the original [`Reserve`](../ring_buffer_bench_test.go#L429) / [`PublishBatchFunc`](../ring_buffer_bench_test.go#L404) benchmarks) is where the platforms diverge.

*x86-64 (Ryzen 5 9600X):*

| Batch size | `Reserve` 1-byte | `Reserve_Fill` full | `PublishBatchFunc` 1-byte | `PublishBatchFunc_Fill` full |
|-----------:|-----------------:|--------------------:|--------------------------:|-----------------------------:|
|          1 |            6.890 |               7.029 |                     6.854 |                        7.261 |
|         10 |            2.301 |               2.212 |                     2.235 |                        2.255 |
|        100 |            2.381 |               2.199 |                     2.157 |                        2.195 |
|       1000 |            2.210 |               2.146 |                     2.244 |                        2.184 |

*arm64 (Apple M4 Pro):*

| Batch size | `Reserve` 1-byte | `Reserve_Fill` full | `PublishBatchFunc` 1-byte | `PublishBatchFunc_Fill` full |
|-----------:|-----------------:|--------------------:|--------------------------:|-----------------------------:|
|          1 |            7.909 |               4.839 |                     8.305 |                        5.986 |
|         10 |            5.462 |               2.584 |                     6.094 |                        3.252 |
|        100 |            4.899 |               6.333 |                     3.753 |                        5.782 |
|       1000 |        **4.238** |           **1.681** |                     3.123 |                        3.068 |

- **On arm64, one-byte writes are ~2.5x slower than full-slot fills** (`Reserve` 4.24 vs. `Reserve_Fill` 1.68 ns/item at size 1000). A sub-line store into a cold 128 B line triggers a read-for-ownership. A full-line store streams without it.
- **On x86 the two fills are indistinguishable.** A 1-byte and a full-slot write to a cold 64 B line cost the same (the line transfer dominates). Every variant collapses onto one ~2.1 ns floor.
- **The callback flattens the fill pattern to ~3.1 ns/item on arm64.** For 1-byte writes the per-element call paces store issue and relieves oversubscription. For full writes it blocks compiler coalescing of streaming stores. See [DirectGap](#the-publish_direct-gap-decomposed) for the same effect.

</details>

<details>
<summary>Deep dive: is batch growth linear?</summary>

For total batch latency, **yes, approximately** once batches are no longer tiny.

**x86-64 (Ryzen 5 9600X):**

| Benchmark                                               | 10→100 total-time ratio | 100→1000 total-time ratio | Interpretation                                     |
|---------------------------------------------------------|------------------------:|--------------------------:|----------------------------------------------------|
| [`PublishBatch`](../ring_buffer_bench_test.go#L376)     |                  12.12x |                     9.62x | Mildly superlinear 10→100, improving again at 1000 |
| [`PublishBatchFunc`](../ring_buffer_bench_test.go#L404) |                  10.16x |                    10.38x | Near-linear across both ranges                     |
| [`Reserve`](../ring_buffer_bench_test.go#L429)          |                  10.16x |                     9.30x | Near-linear with improving per-item cost           |

**arm64 (Apple M4 Pro):**

| Benchmark          | 10→100 total-time ratio | 100→1000 total-time ratio | Interpretation                                                       |
|--------------------|------------------------:|--------------------------:|----------------------------------------------------------------------|
| `PublishBatch`     |                   18.6x |                      3.6x | Straddles the mid-size fill hump (see below), not superlinear growth |
| `PublishBatchFunc` |                   6.16x |                     8.32x | Close to linear (1-byte fill, no hump)                               |
| `Reserve`          |                   8.97x |                     8.65x | Close to linear (1-byte fill, no hump)                               |

The arm64 `PublishBatch` ratios look erratic because its per-item cost crosses a reproducible mid-size hump. Cheap at size 10, a broad peak through medium batch sizes, then a fall to its size-1000 floor (~1.8 ns/item). Nothing actually grows superlinearly. The hump is a write-path effect (reader-independent, present with no reader attached), tracking full-slot writes specifically.

The more useful signal is per-item cost. The practical conclusion:

- from size 1 to size 10, fixed overhead is heavily amortized
- from size 10 onward, per-item cost is mostly flat
- there is no sign of runaway superlinear growth in the core batch path

</details>

The practical takeaway for per-item cost:

1. Single-item operations pay a meaningful fixed cost.
2. Batching improves throughput substantially.
3. For medium and large batches, total time grows close to linearly while cost per item stays nearly constant.

## How do I read batches?

All read paths are fast and allocation-free. These numbers come from `ReadView` over a pre-filled
8192-slot buffer (512 KB, cache-resident) with no ring or goroutines. The wrapped variants straddle
the ring end (worst case). Mean of 10 runs (`mise run bench:readview`):

```bash
go test -run '^$' -bench 'BenchmarkReadView' -count=10 .
```

**x86-64 (Ryzen 5 9600X):**

| Batch size | [`GetSegments`](../ring_buffer_bench_test.go#L569) (wrapped) ns/item | [`GetSegments`](../ring_buffer_bench_test.go#L582) (contiguous) ns/item | [`Get`](../ring_buffer_bench_test.go#L593) loop ns/item | [`Iterate`](../ring_buffer_bench_test.go#L602) (wrapped) ns/item |
|-----------:|---------------------------------------------------------------------:|------------------------------------------------------------------------:|--------------------------------------------------------:|-----------------------------------------------------------------:|
|         10 |                                                                 0.40 |                                                                    0.37 |                                                    0.39 |                                                             0.42 |
|        256 |                                                                 0.23 |                                                                    0.27 |                                                    0.34 |                                                             0.23 |
|       4096 |                                                                 0.31 |                                                                    0.31 |                                                    0.31 |                                                             0.31 |

**arm64 (Apple M4 Pro):**

| Batch size | `GetSegments` (wrapped) ns/item | `GetSegments` (contiguous) ns/item | `Get` loop ns/item | `Iterate` (wrapped) ns/item |
|-----------:|--------------------------------:|-----------------------------------:|-------------------:|----------------------------:|
|         10 |                            0.45 |                               0.45 |               0.41 |                        0.40 |
|        256 |                            0.30 |                               0.28 |               0.33 |                        0.29 |
|       4096 |                            0.47 |                               0.46 |               0.48 |                        0.47 |

Takeaways (qualitative conclusions hold on both machines, magnitudes and sweet spots differ):

- Every read path is allocation-free (0 B/op, 0 allocs/op, verified on arm64). The two returned
  segments are direct views into the ring.
- Wrapping costs nothing. The wrapped `GetSegments` drain matches or beats the contiguous one at
  every batch size.
- `Iterate` matches `GetSegments` at every batch size. Prefer `GetSegments` when you need the
  slices themselves.
- The per-element `Get` loop pays masking and bounds work per event, visible at mid batch sizes
  (x86 0.34 vs 0.23, arm64 0.33 vs 0.30 at batch 256).
- arm64 has a higher floor at large batches. The M4 is fastest at batch 256, with its 128 B cache
  lines pushing the 4096-element working set out of the fastest cache level.

## What about tail latency?

The latency matrix tool ([`internal/cmd/latency`](../internal/cmd/latency)) sweeps shape × stages × wait strategy × polling × reader count combinations under a fixed duration, measuring end-to-end, writer-stall, and reader-lag percentiles via HDR histograms.

Command used:

```bash
go run ./internal/cmd/latency -matrix -duration 10s -output csv
```

These numbers are platform results, not just CPU results. arm64 FixedRate configs have dramatically tighter p99 (7.0µs vs 39.8µs on x86), largely due to macOS scheduler and timer behaviour on the steady publisher. The overflow quadrant (stages ≥ 2 with 8 readers) is worse on arm64: writer-stall p99 hits ~62ms on the M4 vs ~38ms on the Ryzen. Recommendations shift between platforms and are tabulated separately below.

### Top 5 configurations by e2e p99

**FixedRate** (500 KHz steady publisher), **x86-64:**

| stages | wait  | poll  | readers | e2e p50 | e2e p99 | e2e p99.99 | writer stall p99 | reader lag p99 |
|-------:|-------|-------|--------:|--------:|--------:|-----------:|-----------------:|---------------:|
|      1 | Spin  | Batch |       4 |  21.2µs |  39.8µs |   1213.4µs |           37.1µs |          5.6µs |
|      1 | Yield | Spin  |       4 |  21.2µs |  40.5µs |   1213.4µs |           37.6µs |          5.6µs |
|      1 | Spin  | Spin  |       4 |  21.1µs |  41.6µs |   1212.4µs |           37.9µs |          6.4µs |
|      1 | Yield | Batch |       4 |  21.2µs |  41.7µs |   5922.8µs |           37.4µs |          6.6µs |
|      1 | Spin  | Batch |       1 |  20.0µs |  44.1µs |   1208.3µs |           41.4µs |          6.0µs |

**FixedRate** (500 KHz steady publisher), **arm64 (Apple M4 Pro):**

| stages | wait   | poll  | readers | e2e p50 | e2e p99 | e2e p99.99 | writer stall p99 | reader lag p99 |
|-------:|--------|-------|--------:|--------:|--------:|-----------:|-----------------:|---------------:|
|      1 | Spin   | Spin  |       1 |   1.8µs |   7.0µs |     16.9µs |            6.7µs |          1.0µs |
|      1 | Hybrid | Spin  |       1 |   1.8µs |   7.0µs |     16.3µs |            6.8µs |          1.0µs |
|      1 | Yield  | Spin  |       1 |   1.8µs |   7.0µs |     16.2µs |            6.8µs |          1.0µs |
|      1 | Sleep  | Spin  |       1 |   1.8µs |   7.0µs |     16.4µs |            6.8µs |          1.0µs |
|      1 | Spin   | Batch |       1 |   1.8µs |   7.1µs |     18.4µs |            6.8µs |          1.0µs |

**BurstReserve** (1000 events/burst, 5ms idle), **x86-64:**

| stages | wait   | poll  | readers | e2e p50 | e2e p99 | e2e p99.99 | writer stall p99 | reader lag p99 |
|-------:|--------|-------|--------:|--------:|--------:|-----------:|-----------------:|---------------:|
|      1 | Yield  | Batch |       1 |   2.1µs |  13.2µs |     55.7µs |            0.4µs |         11.8µs |
|      1 | Spin   | Batch |       1 |   2.1µs |  13.2µs |    117.0µs |            0.4µs |         12.1µs |
|      1 | Hybrid | Batch |       1 |   2.1µs |  13.8µs |     80.9µs |            0.6µs |         12.4µs |
|      1 | Sleep  | Batch |       1 |   2.1µs |  16.9µs |    103.3µs |            0.5µs |         11.5µs |
|      1 | Yield  | Batch |       4 |   2.1µs |  17.8µs |   9756.7µs |            0.4µs |         17.6µs |

**BurstReserve** (1000 events/burst, 5ms idle), **arm64 (Apple M4 Pro):**

| stages | wait   | poll  | readers | e2e p50 | e2e p99 | e2e p99.99 | writer stall p99 | reader lag p99 |
|-------:|--------|-------|--------:|--------:|--------:|-----------:|-----------------:|---------------:|
|      1 | Hybrid | Batch |       4 |   6.0µs |   9.0µs |     30.0µs |            1.0µs |          9.0µs |
|      1 | Spin   | Batch |       4 |   6.0µs |  10.0µs |     66.0µs |            0.6µs |         10.0µs |
|      1 | Yield  | Batch |       4 |   6.0µs |  10.0µs |     34.0µs |            0.8µs |         10.0µs |
|      1 | Hybrid | Batch |       1 |   5.0µs |  10.0µs |     19.0µs |            0.1µs |         10.0µs |
|      1 | Spin   | Batch |       1 |   5.0µs |  13.0µs |     25.0µs |            0.2µs |         13.0µs |

On arm64 the BurstReserve winners cluster on `Batch` polling, with `Hybrid` waits edging out `Yield`.

### Best p99 per (stages × readers) combo: FixedRate

**x86-64:**

| stages | readers | best wait | best poll |     p99 |   p99.99 | writer stall p99 |
|-------:|--------:|-----------|-----------|--------:|---------:|-----------------:|
|      1 |       1 | Spin      | Batch     |  44.1µs | 1208.3µs |           41.4µs |
|      1 |       4 | Spin      | Batch     |  39.8µs | 1213.4µs |           37.1µs |
|      1 |       8 | Yield     | Yield     |  53.1µs |  270.3µs |           29.1µs |
|      2 |       1 | Spin      | Batch     |  69.1µs | 1210.4µs |           39.2µs |
|      2 |       4 | Yield     | Batch     | 197.2µs | 1257.5µs |           66.4µs |
|      2 |       8 | Sleep     | Yield     | 852.5µs | 3995.6µs |           26.1µs |
|      3 |       1 | Hybrid    | Spin      |  44.4µs | 1212.4µs |           36.2µs |
|      3 |       4 | Hybrid    | Yield     | 535.6µs | 2918.4µs |           22.9µs |
|      3 |       8 | Hybrid    | Yield     | 836.1µs | 5435.4µs |           22.0µs |

**arm64 (Apple M4 Pro):**

| stages | readers | best wait | best poll |     p99 |  p99.99 | writer stall p99 |
|-------:|--------:|-----------|-----------|--------:|--------:|-----------------:|
|      1 |       1 | Spin      | Spin      |   7.0µs |  16.9µs |            6.7µs |
|      1 |       4 | Hybrid    | Spin      |  13.3µs |  25.4µs |           13.0µs |
|      1 |       8 | Spin      | Spin      |  17.6µs |  40.3µs |           17.3µs |
|      2 |       1 | Spin      | Spin      |   8.2µs |  22.5µs |            7.6µs |
|      2 |       4 | Hybrid    | Batch     |  16.9µs |  36.9µs |           16.1µs |
|      2 |       8 | Sleep     | Sleep     |  71.0µs | 137.9µs |           38.1µs |
|      3 |       1 | Sleep     | Spin      |  12.8µs |  24.5µs |           12.2µs |
|      3 |       4 | Yield     | Batch     |  15.3µs | 205.6µs |           10.8µs |
|      3 |       8 | Spin      | Sleep     | 110.0µs | 191.1µs |           49.7µs |

On arm64 the winning poll strategy shifts toward `Spin` for FixedRate, and p99 values are 3–6× lower in the well-behaved quadrants. The 8-reader overflow rows still blow up, and there the M4 is worse.

### Best p99 per (stages × readers) combo: BurstReserve

**x86-64:**

| stages | readers | best wait | best poll |     p99 |   p99.99 | writer stall p99 |
|-------:|--------:|-----------|-----------|--------:|---------:|-----------------:|
|      1 |       1 | Yield     | Batch     |  13.2µs |   55.7µs |            0.4µs |
|      1 |       4 | Yield     | Batch     |  17.8µs | 9756.7µs |            0.4µs |
|      1 |       8 | Yield     | Batch     |  30.8µs |  523.8µs |            0.4µs |
|      2 |       1 | Spin      | Batch     |  25.4µs |  110.8µs |            0.5µs |
|      2 |       4 | Spin      | Spin      | 112.0µs |  391.7µs |            0.5µs |
|      2 |       8 | Hybrid    | Yield     | 355.1µs | 5087.2µs |            0.3µs |
|      3 |       1 | Spin      | Spin      |  39.4µs |  506.1µs |            0.4µs |
|      3 |       4 | Sleep     | Yield     | 612.4µs | 2658.3µs |            0.3µs |
|      3 |       8 | Sleep     | Yield     | 660.5µs | 4403.2µs |            0.3µs |

**arm64 (Apple M4 Pro):**

| stages | readers | best wait | best poll |    p99 |   p99.99 | writer stall p99 |
|-------:|--------:|-----------|-----------|-------:|---------:|-----------------:|
|      1 |       1 | Hybrid    | Batch     | 10.0µs |   19.0µs |            0.1µs |
|      1 |       4 | Hybrid    | Batch     |  9.0µs |   30.0µs |            1.0µs |
|      1 |       8 | Hybrid    | Batch     | 13.0µs |  119.0µs |            0.9µs |
|      2 |       1 | Spin      | Batch     | 16.0µs |   37.0µs |            0.6µs |
|      2 |       4 | Yield     | Batch     | 20.0µs |  102.0µs |            0.6µs |
|      2 |       8 | Spin      | Sleep     | 42.0µs |   70.0µs |            0.3µs |
|      3 |       1 | Yield     | Batch     | 17.0µs |   79.0µs |            0.5µs |
|      3 |       4 | Yield     | Batch     | 41.0µs | 1457.2µs |            0.4µs |
|      3 |       8 | Sleep     | Sleep     | 71.0µs |  114.0µs |            1.5µs |

`Batch` polling stays the winner across almost every BurstReserve cell on both platforms.

### Stages impact and writer stall at depth

Adding pipeline stages degrades p99 as total reader count grows, especially on x86 where every best-config row hits a multi-second worst-case stall. The 8-reader overflow quadrant hits ~38ms p99 on x86 and ~62ms on arm64.

<details>
<summary>Deep dive: stages impact on p99</summary>

Each row shows the best-polling config for that (readers, stages) combination.

**x86-64:**

| readers | stages | best wait | best poll |    p50 |     p99 |   p99.99 |         max |
|--------:|-------:|-----------|-----------|-------:|--------:|---------:|------------:|
|       1 |      1 | Spin      | Batch     | 20.0µs |  44.1µs | 1208.3µs | 9823060.0µs |
|       1 |      2 | Spin      | Batch     | 20.5µs |  69.1µs | 1210.4µs | 9831448.6µs |
|       1 |      3 | Hybrid    | Spin      | 20.9µs |  44.4µs | 1212.4µs | 9839837.2µs |
|       4 |      1 | Spin      | Batch     | 21.2µs |  39.8µs | 1213.4µs | 9814671.4µs |
|       4 |      2 | Yield     | Batch     | 24.4µs | 197.2µs | 1257.5µs | 9831448.6µs |
|       4 |      3 | Hybrid    | Yield     | 15.9µs | 535.6µs | 2918.4µs | 9839837.2µs |
|       8 |      1 | Yield     | Yield     |  3.3µs |  53.1µs |  270.3µs | 9823060.0µs |
|       8 |      2 | Sleep     | Yield     | 13.7µs | 852.5µs | 3995.6µs | 9831448.6µs |
|       8 |      3 | Hybrid    | Yield     | 25.5µs | 836.1µs | 5435.4µs | 9848225.8µs |

**arm64 (Apple M4 Pro):**

| readers | stages | best wait | best poll |    p50 |     p99 |  p99.99 |      max |
|--------:|-------:|-----------|-----------|-------:|--------:|--------:|---------:|
|       1 |      1 | Spin      | Spin      |  1.8µs |   7.0µs |  16.9µs |   77.5µs |
|       1 |      2 | Spin      | Spin      |  2.0µs |   8.2µs |  22.5µs |   51.5µs |
|       1 |      3 | Sleep     | Spin      |  2.5µs |  12.8µs |  24.5µs |   69.7µs |
|       4 |      1 | Hybrid    | Spin      |  2.4µs |  13.3µs |  25.4µs |   70.7µs |
|       4 |      2 | Hybrid    | Batch     |  3.0µs |  16.9µs |  36.9µs |  148.5µs |
|       4 |      3 | Yield     | Batch     |  3.4µs |  15.3µs | 205.6µs | 1285.1µs |
|       8 |      1 | Spin      | Spin      |  2.6µs |  17.6µs |  40.3µs | 1298.4µs |
|       8 |      2 | Sleep     | Sleep     | 12.5µs |  71.0µs | 137.9µs |  223.6µs |
|       8 |      3 | Spin      | Sleep     | 23.5µs | 110.0µs | 191.1µs |  290.0µs |

On x86 every best-config row hits ~9.8s (the full test duration) at least once: a single worst-case stall per run. On arm64 the best configs keep their max in the tens-to-hundreds of µs range (worst 1.3ms). The M4's well-behaved configs avoid the one-off multi-second stall that the Linux/Ryzen runs always carry in their tail.

</details>

<details>
<summary>Deep dive: writer stall worst offenders</summary>

Configurations where the writer was throttled most by slow readers.

**x86-64:**

| stages | wait  | poll  | readers | writer stall p99 | writer stall max |
|-------:|-------|-------|--------:|-----------------:|-----------------:|
|      3 | Spin  | Spin  |       8 |        38174.7µs |        59441.2µs |
|      2 | Spin  | Spin  |       8 |        37683.2µs |        55017.5µs |
|      2 | Yield | Spin  |       8 |        37224.4µs |        42139.6µs |
|      3 | Sleep | Batch |       8 |        34144.3µs |        42598.4µs |
|      2 | Sleep | Spin  |       8 |        33652.7µs |        55017.5µs |

**arm64 (Apple M4 Pro):**

| stages | wait   | poll  | readers | writer stall p99 | writer stall max |
|-------:|--------|-------|--------:|-----------------:|-----------------:|
|      2 | Hybrid | Batch |       8 |        62357.5µs |        98631.7µs |
|      3 | Yield  | Spin  |       8 |        62160.9µs |       111804.4µs |
|      2 | Hybrid | Spin  |       8 |        61538.3µs |        91226.1µs |
|      3 | Spin   | Spin  |       8 |        61276.2µs |       104333.3µs |
|      3 | Spin   | Batch |       8 |        61046.8µs |        77922.3µs |

This is the one place arm64 is clearly worse. In the 8-reader overflow quadrant the M4's writer-stall p99 reaches ~62ms (vs ~38ms on the Ryzen) and its max exceeds 110ms. When the ring genuinely can't be drained, the M4 throttles the writer harder. Don't run stages ≥ 2 with 8 readers at 500 KHz on a 131K-slot ring. Size the ring up or reduce per-stage reader count.

</details>

### Tail-aware recommendations

The table below gives the recommended config for each workload when tail latency (p99.9+) is the primary concern:

**x86-64:**

| Workload                | Rec for p99   | Rec for p99.9+     | Why it changed                               |
|-------------------------|---------------|--------------------|----------------------------------------------|
| FixedRate, 4 readers    | `Spin/Batch`  | `Spin/Batch`       | No change; gets stronger at tails            |
| FixedRate, 8 readers    | `Yield/Yield` | `Yield/Yield`      | No change; 9× gap at p99.9                   |
| FixedRate, 1 reader     | `Spin/Batch`  | `Spin/Batch`       | No change                                    |
| FixedRate, 2 stages     | `Spin/Batch`  | **`Yield/Yield`**  | Yield/Yield = 5× better p99.9 (145 vs 781µs) |
| FixedRate, 3 stages     | `Hybrid/Spin` | **`Hybrid/Batch`** | Batch = 25% better p99.9 (517 vs 689µs)      |
| BurstReserve, 1 reader  | `Yield/Batch` | `Yield/Batch`      | No change; gets stronger                     |
| BurstReserve, 4 readers | `Yield/Batch` | **`Spin/Batch`**   | Yield/Batch fails at p99.99 (9.8ms!)         |
| BurstReserve, 2 stages  | `Spin/Batch`  | `Spin/Batch`       | No change                                    |
| BurstReserve, 3 stages  | `Spin/Spin`   | **`Spin/Batch`**   | Batch = 2.4× better p99.99 (208 vs 506µs)    |

**arm64 (Apple M4 Pro):**

| Workload                | Rec for p99    | Rec for p99.9+    | Why it changed                                    |
|-------------------------|----------------|-------------------|---------------------------------------------------|
| FixedRate, 1 reader     | `Spin/Spin`    | `Spin/Spin`       | No change (Yield/Spin ties within noise)          |
| FixedRate, 4 readers    | `Hybrid/Spin`  | `Hybrid/Spin`     | No change                                         |
| FixedRate, 8 readers    | `Spin/Spin`    | `Hybrid/Batch`    | Marginal: Batch ~2% better p99.9 (24.8 vs 25.3µs) |
| FixedRate, 2 stages     | `Spin/Spin`    | `Spin/Spin`       | No change (Sleep/Spin ties at 14.1µs)             |
| FixedRate, 3 stages     | `Sleep/Spin`   | `Sleep/Spin`      | No change (Hybrid/Batch ties at 19.0µs)           |
| BurstReserve, 1 reader  | `Hybrid/Batch` | `Hybrid/Batch`    | No change                                         |
| BurstReserve, 4 readers | `Hybrid/Batch` | `Hybrid/Batch`    | No change; **no p99.99 blow-up** (unlike x86)     |
| BurstReserve, 2 stages  | `Spin/Batch`   | **`Yield/Batch`** | Yield/Batch ~7% better p99.9 (25 vs 27µs)         |
| BurstReserve, 3 stages  | `Yield/Batch`  | **`Spin/Spin`**   | Spin/Spin = 2× better p99.9 (25 vs 52µs)          |

Key takeaways:

- FixedRate's winning poll strategy is platform-dependent: `Batch` on x86, `Spin` on arm64. `Batch` remains the durable winner for BurstReserve on both platforms.
- arm64 configs are far more stable across percentiles. Almost every arm64 row is "no change" from p99 to p99.9+.
- The x86 BurstReserve `Yield/Batch` p99.99 blow-up (9.8ms) doesn't reproduce on arm64 (stays at 30µs).
- `Sleep` polling is rarely competitive on either platform (coarse timer granularity adds latency).
- Pipeline stages are free with ≤4 total readers on both machines. Overhead grows with total reader count, not stage count.
- Stages ≥ 2 with 8 readers is a buffer-overflow scenario for 500 KHz FixedRate on both platforms. Size the ring up or cut per-stage readers.

## Can I squeeze more performance with PGO?

Yes. PGO gives roughly **10% on x86-64** and **~4% on arm64** for single-event publish throughput. A library can't ship PGO (the profile applies when the final binary is compiled), so this gain belongs to whoever builds the application.

All tables in this document use non-PGO builds. To run the full comparison yourself:

```bash
mise run bench:pgo
```

The command collects a CPU profile, rebuilds with `-pgo=<profile>`, and prints a `benchstat` summary. The two builds run in interleaved rounds so background load widens variance instead of silently biasing one side.

**x86-64** (Go 1.26.2):

| Benchmark                                                       | no-PGO mean | PGO mean |  Delta |
|-----------------------------------------------------------------|------------:|---------:|-------:|
| [`Publish`](../ring_buffer_bench_test.go#L22)                   |     5.47 ns |  4.89 ns | -10.5% |
| [`PublishBatch`](../ring_buffer_bench_test.go#L376)`/size=1`    |     7.20 ns |  7.38 ns |  +2.6% |
| [`PublishBatch`](../ring_buffer_bench_test.go#L376)`/size=10`   |    21.69 ns | 21.62 ns |  -0.3% |
| [`PublishBatch`](../ring_buffer_bench_test.go#L376)`/size=100`  |    241.6 ns | 240.0 ns |  -0.7% |
| [`PublishBatch`](../ring_buffer_bench_test.go#L376)`/size=1000` |     2326 ns |  2325 ns |  -0.0% |
| [`Publish_Direct`](../ring_buffer_bench_test.go#L131)           |     9.66 ns |  9.78 ns |  +1.3% |

**arm64 (Apple M4 Pro)** (mean of 10 interleaved rounds, `p<0.05` except where noted):

| Benchmark                | no-PGO mean | PGO mean |             Delta |
|--------------------------|------------:|---------:|------------------:|
| `Publish`                |     7.39 ns |  7.08 ns |             -4.2% |
| `Publish_NoReaders`      |     7.09 ns |  6.81 ns |             -3.9% |
| `Publish_Direct`         |     8.66 ns |  8.31 ns |             -4.0% |
| `PublishBatch/size=1`    |     5.58 ns |  4.92 ns | -11.8% (±10% var) |
| `PublishBatch/size=10`   |    27.46 ns | 27.48 ns |          ~0% (ns) |
| `PublishBatch/size=100`  |    491.5 ns | 486.9 ns |             -0.9% |
| `PublishBatch/size=1000` |     1752 ns |  1779 ns |          ~0% (ns) |

(geomean across the full core family: **-2.2%**. "ns" = not statistically significant.)

PGO helps where the bottleneck is call overhead (the single-publish hot path where the compiler raises the inlining budget for the profiled-hot `PublishFunc`), not memory bandwidth (batch paths at size ≥ 10 sit at the store-bandwidth floor on both architectures). On x86, the win concentrates on `Publish` (-10.5%) while `Publish_Direct` stays flat. On arm64, a smaller ~4% improvement spreads across the whole single-publish family including `Publish_Direct`.

If publish-path latency matters to you, collect a profile from your own workload (`runtime/pprof` or `net/http/pprof` in production, or `-cpuprofile` from a representative benchmark) and build with `go build -pgo=<profile>` (or check the profile in as `default.pgo` next to your `main` package). Measure on your own target since the gain is architecture-dependent.

## SIMD segment fills (experimental)

`Reserve` returns up to two contiguous slices into the ring's backing array. That's the surface a hand-written vector fill wants. **Experimental and amd64-only:** `simd/archsimd` only exists in Go 1.26+ behind `GOEXPERIMENT=simd`, outside the Go 1 compatibility promise. The library can't import it. The deliverable is a caller-side recipe in [`example_simd_test.go`](../example_simd_test.go) (broadcast a template, stamp a `uint64` sequence per slot, byte-identical scalar fallback). The package has no aligned load/store API, and vectors can't be type parameters or struct fields. This stays caller-side on concrete types and can't live in the generic `RingBuffer[T]`.

Rig: Ryzen 5 9600X (Zen 5, AVX-512), one cache-line payload, ns/item, lower of two runs. SIMD verified byte-identical to scalar. Two regimes: the standard `1<<22` ring (256 MB) is DRAM-bandwidth-bound at ~2.8 ns/item for every method (hides compute). A `1<<13` ring (512 KB, L2) exposes it. "Cache-resident" means the L2 ring.

### Writer side: copies lose, generated fills win

Copying prepared events is a null result. Runtime `memmove` (`copy`, `PublishBatch`) is already vectorized, so explicit AVX-512 only matches it. The win is generating events in place: broadcast a 64-byte template, stamp a `uint64` sequence into the first 8 bytes. The compiler doesn't vectorize `*slot = template`. Explicit SIMD does. Cache-resident:

| batch | `PublishBatchFunc` | Reserve + scalar fill | Reserve + simd fill |
|------:|-------------------:|----------------------:|--------------------:|
|     1 |               10.2 |                   9.0 |                 8.9 |
|    10 |               3.39 |                  2.52 |                2.28 |
|    64 |               2.99 |                  2.13 |                1.07 |
|   256 |               2.94 |                  2.13 |                0.97 |
|  1024 |               2.96 |                  2.00 |                0.83 |
|  4096 |               3.04 |                  2.05 |                0.87 |

At batch ≥ 64, ~1 ns/item is `PublishBatchFunc`'s per-slot callback (the scalar column isolates it). The remaining ~2.4× is the vector fill, **~3.5× over scalar `PublishBatchFunc`** total. Gains start near batch 10 and hold through 4096. Below ~10 the Reserve/Commit fixed cost (~9 ns) dominates. On the DRAM ring everything collapses to ~2.8 ns/item.

### Alignment: mostly free, with one sharp edge

A cache-line-multiple payload gets a 64-byte-aligned base for free. The allocator carves page-aligned spans into size-class slots, and ≥ 32 KB is page-aligned outright. The edge: Go (1.26) prepends an 8-byte malloc header to pointer-containing allocations in `[512 B, 32 KB)`, fixing their base at **8 mod 64**. Reslicing only shifts it by `gcd(sizeof(T), 64)`. A pointer-carrying payload of multiple-of-16 size in a sub-32 KB ring is provably unalignable. The misaligned penalty is modest and cache-resident only (~15% for ZMM stores, gone once DRAM-bound). Remedy: pad the payload to a cache-line multiple. That removes line-straddling stores and buys alignment as a side effect.

| Allocation                        | base % 64 |
|-----------------------------------|----------:|
| 48 B ptr-struct × 72 (3.4 KB)     |  always 8 |
| 48 B ptr-struct × 8 (384 B)       |  always 0 |
| 48 B noscan × 72                  |  always 0 |
| 48 B ptr-struct × 1000 (48 KB)    |  always 0 |
| 64 B ptr-struct × `1<<19` (32 MB) |  always 0 |

### Reader side: single-threaded wins, submerged by the pipeline

Single-threaded, vectors pay for compute over events (not copy-out, since memmove already matches). Work is a checksum (XOR the eight words of each 64 B event):

| method                          | ring in L2 | ring in DRAM |
|---------------------------------|-----------:|-------------:|
| scalar checksum                 |       0.67 |         1.36 |
| per-element callback checksum   |       1.23 |         1.48 |
| simd checksum                   |       0.42 |         1.34 |
| simd checksum (4× accumulators) |       0.30 |         1.34 |
| memmove copy-out                |       0.47 |         1.53 |
| simd copy-out (4×)              |       0.46 |         1.62 |

SIMD checksum is 2.2× over a scalar loop and 4.1× over the per-element callback (`Iterate`'s shape. The indirect call alone is ~0.56 ns/item). In a live pipeline the ranking inverts:

| reader (live pipeline) | ring in L2 | ring in DRAM |
|------------------------|-----------:|-------------:|
| drain-only (no data)   |       1.52 |         2.95 |
| touch (1 word)         |       2.36 |         4.19 |
| scalar checksum        |       2.00 |         4.01 |
| simd checksum (4× acc) |       2.49 |         4.29 |

All data-touching readers cluster within ~1 ns/item, above drain-only. Once a reader pulls each line the writer reclaims ownership next lap. That cross-core ping-pong dominates. The faster SIMD reader even loses slightly by polling the barrier harder. SIMD reads only pay when the reader is the bottleneck (heavy per-event compute, offline scans), never for keep-up pipeline readers.
