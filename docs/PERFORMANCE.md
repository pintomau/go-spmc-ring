# Performance data

Full benchmark and latency measurements for [ringring](../README.md). The headline numbers
and configuration guidance live in the README's [Performance](../README.md#performance)
section; this document holds the complete data and per-experiment analysis.

## Benchmarks

The tables below summarize the core ring benchmarks on this machine:

| Environment | Value                              |
|-------------|------------------------------------|
| OS          | Linux                              |
| CPU         | AMD Ryzen 5 9600X 6-Core Processor |
| Go          | 1.26.2                             |

Results are machine-specific. The numbers below are the arithmetic mean of a 10-count benchmark
run; each command is run twice and the lower-mean set is reported, with a run's first element
dropped when it exceeds the mean of the rest by more than 5% (transient warm-up filter).
Baseline last refreshed 2026-06-12.

Command used (`mise run bench:core`):

```bash
go test -run '^$' -bench 'BenchmarkRingBuffer_(Publish$|Publish_NoReaders$|Publish_Direct$|PublishBatch$|PublishBatchFunc$|Reserve$)' -count=10 .
```

### Core publish path

| Benchmark           | Mean ns/op |    Throughput | Notes                                                |
|---------------------|-----------:|--------------:|------------------------------------------------------|
| `Publish_NoReaders` |      5.097 | 196.2 M ops/s | Writer upper bound with no registered readers        |
| `Publish`           |      5.188 | 192.8 M ops/s | Keep-up reader adds ~1.8% overhead vs. no readers    |
| `Publish_Direct`    |      9.206 | 108.6 M ops/s | Constructs and publishes a value per event; see [the gap decomposed](#the-publish_direct-gap-decomposed) |

**How batch publish compares:** at batch size 10 all three batch APIs converge to ~2.1–2.25 ns/item, about 59% cheaper
per-item than a single `Publish` (5.19 ns). The gap (~3.1 ns) is fixed overhead that cannot be amortized per-event:
the gating check, the `writeCursor` store, and cache-line coordination. Batching spreads that cost across N items,
so once the batch is ~10 items the per-item cost is mostly the payload write itself. `Reserve/Commit` amortizes
most efficiently at large sizes (2.12 ns/item at size 1000) because it issues only one `writeCursor.Store`
for the whole batch rather than one per item. See the [Batch scaling](#batch-scaling) section below for
the full numbers.

### The Publish_Direct gap, decomposed

`Publish_Direct` measures 9.21 ns against `Publish`'s 5.19 ns, which reads like a 1.77x
penalty for publishing by value. Controlled variants (2026-06-12, the
`BenchmarkRingBuffer_DirectGap_*` family) show the API is responsible for only a small
slice of that:

| Variant                                  | Mean ns/op | What it isolates                                  |
|------------------------------------------|-----------:|---------------------------------------------------|
| `PublishFunc`, write 1 byte in the slot  |       5.12 | The standing `Publish` benchmark workload          |
| `PublishFunc`, write all 64 B in the slot|       5.13 | Full payload traffic through the callback API      |
| `Publish` by value, payload prepared once|       5.49 | The honest by-value API cost                       |
| `Publish` by value, value built per event|       9.18 | The original `Publish_Direct` shape                |

Three findings:

- **Writing 64 bytes costs the same as writing 1 byte.** The cache-line ownership
  transfer between writer and reader dominates, and it is paid either way. Payload
  size is not the gap.
- **By-value publish itself costs ~7%** (5.49 vs 5.12 ns): one 64 B argument copy plus
  identical call structure. That is the real `Publish` vs `PublishFunc` API difference.
- **The remaining ~3.7 ns is the per-event construction pattern, not the API.**
  Disassembly shows `obj := object{}` compiles to four 16 B zeroing stores plus a 1-byte
  field store, and the immediately following 64 B copy into the call argument re-reads
  those bytes while the stores are still in flight. The first 16 B load overlaps two
  stores of different sizes, which defeats store-to-load forwarding and stalls roughly
  15-20 cycles, every iteration.

Guidance: for cache-line-sized events, prefer `PublishFunc` (or `Reserve`/`Commit` for
batches) and build the event directly in the ring slot; it skips both copies and the
stall entirely. If you must publish by value, reuse a prepared value or fill an existing
variable instead of constructing a fresh composite literal right before the call.

### Multi-reader publish scaling

Command used (`mise run bench:multireader`):

```bash
go test -run '^$' -bench 'BenchmarkRingBuffer_Publish_MultiReader$' -count=10 .
```

| Readers | Mean ns/op |    Throughput | Slowdown vs 1 reader | Notes                                                        |
|--------:|-----------:|--------------:|---------------------:|--------------------------------------------------------------|
|       1 |      5.442 | 183.8 M ops/s |               1.000x | Single keep-up reader                                        |
|       2 |      5.495 | 182.0 M ops/s |               1.010x | Essentially flat vs. 1 reader                                |
|       4 |      5.535 | 180.7 M ops/s |               1.017x | Small additional gating cost                                 |
|       8 |      5.780 | 173.0 M ops/s |               1.062x | Modest overhead; bitmap scan amortized across few readers    |
|      16 |      6.274 | 159.4 M ops/s |               1.153x | First visible step; reader count approaching thread budget   |
|      32 |      7.652 | 130.7 M ops/s |               1.406x | Overhead accelerates as readers exceed physical thread count |
|      64 |     10.720 |  93.3 M ops/s |               1.970x | Near 2× slower; scan over 64 cursor slots starts to dominate |
|     128 |     16.550 |  60.4 M ops/s |               3.041x | Bitmap capacity limit; 3× single-reader overhead             |

Degradation is **sub-linear up to the hardware thread count**. On this machine (Ryzen 5 9600X, 6C/12T),
going from 1 to 8 readers costs only **6.2%** throughput. The overhead stays moderate through 16
readers but accelerates noticeably past 12 hardware threads: at 64 readers throughput is
roughly halved, and at 128 (the bitmap capacity limit) it reaches 3× single-reader
overhead. The primary driver past ~32 readers is the `scanAll` loop over all 128
cursor slots, which the writer must run on every slow-path call to find the
minimum cursor.

**`runtime.LockOSThread`** (`ring_buffer_bench_test.go`): pinning each reader and the writer to their own
OS thread was tested with both `time.Sleep` and `runtime.Gosched()` idle strategies. Key findings:

- **Locked + Sleep**: universally worse; catastrophic at 128 readers (+63%) because 129 OS threads sleep/wake on 12
    hardware threads, thrashing the OS scheduler.
- **Locked + Gosched**: worse at 1–32 readers (peak −40% at 8 readers); marginally faster at 64–128 readers (~3–6%).
    The improvement at high reader counts comes from cursor cache locality: each locked thread keeps its cursor
    slot warm on the same physical core. The hurt at low counts comes from M:P scheduling overhead (the locked M sits
    idle each time `Gosched` yields its P).
- **Unlocked + Gosched on keepUpReader**: dramatically worse everywhere (peak −157% at 8 readers).
    `Gosched` in a tight idle loop does not park the goroutine: it keeps re-entering the run queue, creating a
    scheduler storm that starves the writer. `time.Sleep` is qualitatively better because it genuinely removes
    the goroutine from the run queue.

**Conclusion**: Go's GOMAXPROCS scheduler outperforms OS thread pinning for this workload. `LockOSThread` is only worth
revisiting at reader counts ≥ 64 with a `Gosched`-based idle and continuous spinning (no sleep points), and even then
the gain is marginal. The idle backoff strategy matters more than thread pinning.

### Stage / pipeline scaling

The stage table below uses the dedicated pipeline benchmark family in `ring_buffer_pipeline_bench_test.go`. The figures
follow the same methodology as the rest of this document: lower of two 10-count sets, first element of a set dropped
if >5% above the rest mean (transient-spike filter).

Command used (`mise run bench:pipeline`):

```bash
go test -run '^$' -bench '^BenchmarkPipeline' -benchtime=1s -count=10 .
```

All four variants now use `keepUpReader` (`time.Sleep(50µs)` idle). The benchmark cases:

- `1Stage_NoPipeline`: direct reader registration on `rb.barrier`; the closest equivalent to the core publish benchmarks
- `1Stage`: one explicit `Stage[T]` (`rb.NewStage(nil)`, `rb.SetGatingStage(s1)`)
- `2Stage` / `3Stage`: explicit pipeline depth; *readers per stage* means N × depth total readers across all stages

| Readers per stage | `1Stage_NoPipeline` ns/op | `1Stage` ns/op | `2Stage` ns/op | `3Stage` ns/op | Notes                                                                                                        |
|------------------:|--------------------------:|---------------:|---------------:|---------------:|--------------------------------------------------------------------------------------------------------------|
|                 1 |                     5.391 |          5.656 |          5.446 |          5.409 | All variants within 9% of core publish baseline; Stage API overhead is small at 1 reader                     |
|                 2 |                     5.555 |          5.777 |          5.514 |          5.552 | Adding a second reader per stage costs essentially nothing; total readers = 2 / 4 / 6                        |
|                 4 |                     5.625 |          5.805 |          5.689 |          5.834 | Still under 6 ns across the board; 3Stage has 12 total readers                                               |
|                 8 |                     5.854 |          6.005 |          6.112 |          6.613 | 3Stage shows first meaningful overhead: 24 total readers, scanAll runs across 3 stages                       |
|                16 |                     6.262 |          6.504 |          7.281 |          8.669 | Overhead now clearly tied to total reader count (16 / 32 / 48); depth multiplies the scanAll cost            |
|                32 |                     7.753 |          7.915 |         10.180 |         12.940 | 2Stage doubles cost vs 1Stage (96 vs 32 total); 3Stage at 96 total readers approaching MultiReader territory |
|                64 |                    10.710 |         10.900 |         16.070 |         17.480 | 1Stage matches MultiReader/r=64 baseline; 2Stage and 3Stage reflect 128 and 192 total readers                |
|               128 |                    16.490 |         16.690 |         23.090 |         33.380 | 1Stage matches MultiReader/r=128; 3Stage at 384 total readers is 2× worse than 1Stage                        |

Key observations:

1. **At 1–4 readers per stage all variants are effectively free**: costs cluster in the 5.4–5.8 ns range and are indistinguishable from the core single-reader publish baseline.
2. **Overhead scales with total readers, not pipeline depth.** `2Stage/r=16` (7.28 ns, 32 total) ≈ `MultiReader/r=32` (7.65 ns); `3Stage/r=8` (6.61 ns, 24 total) ≈ `MultiReader/r=24`. Pipeline depth itself costs nothing: the scanAll load from the extra readers does.
3. **`1Stage_NoPipeline` and `1Stage` track each other closely** because both have the same total reader count; the `Stage[T]` wrapper adds ~1–5% overhead, shrinking as reader count grows.

### Batch scaling

| Batch size | `PublishBatch` ns/op | `PublishBatch` ns/item | `PublishBatchFunc` ns/op | `PublishBatchFunc` ns/item | `Reserve` ns/op | `Reserve` ns/item |
|-----------:|---------------------:|-----------------------:|-------------------------:|---------------------------:|----------------:|------------------:|
|          1 |                6.803 |                  6.803 |                    6.898 |                      6.898 |           6.770 |             6.770 |
|         10 |               21.030 |                  2.103 |                   21.020 |                      2.102 |          22.490 |             2.249 |
|        100 |              254.800 |                  2.548 |                  213.600 |                      2.136 |         228.500 |             2.285 |
|       1000 |             2450.000 |                  2.450 |                 2217.000 |                      2.217 |        2124.000 |             2.124 |

At larger batch sizes the per-item cost settles in the **392–476 M items/s** range (**2.10–2.55 ns/item**), with
`Reserve` reaching the floor at size 1000 because a single `writeCursor.Store` covers the entire batch.

### Is the growth linear?

For total batch latency, **yes, approximately** once batches are no longer tiny.

| Benchmark          | 10→100 total-time ratio | 100→1000 total-time ratio | Interpretation                                           |
|--------------------|------------------------:|--------------------------:|----------------------------------------------------------|
| `PublishBatch`     |                  12.12x |                     9.62x | Mildly superlinear 10→100, improving again at 1000       |
| `PublishBatchFunc` |                  10.16x |                    10.38x | Near-linear across both ranges                           |
| `Reserve`          |                  10.16x |                     9.30x | Near-linear with improving per-item cost                 |

The more useful signal is the **per-item** cost:

- from size `1` to size `10`, fixed overhead is heavily amortized
- from size `10` onward, per-item cost is mostly flat
- there is **no sign of runaway superlinear growth** in the core batch path

So the practical conclusion is:

1. single-item operations pay a meaningful fixed cost
2. batching improves throughput substantially
3. for medium and large batches, total time grows close to linearly while cost per item stays nearly constant

### Batch read paths

`ReadView` access paths, benchmarked directly over a pre-filled 8192-slot buffer (512 KB,
cache-resident) with no ring or goroutines involved, so the numbers isolate pure traversal
cost. Work per event is a one-byte XOR. The wrapped variants drain a range that straddles
the ring end, the worst case for batch reads. Mean of 10 runs (`mise run bench:readview`):

```bash
go test -run '^$' -bench 'BenchmarkReadView' -count=10 .
```

| Batch size | `GetSegments` (wrapped) ns/item | `GetSegments` (contiguous) ns/item | `Get` loop ns/item | `Iterate` (wrapped) ns/item |
|-----------:|--------------------------------:|-----------------------------------:|-------------------:|----------------------------:|
|         10 |                            0.40 |                                0.37 |               0.39 |                         0.42 |
|        256 |                            0.23 |                                0.27 |               0.34 |                         0.23 |
|       4096 |                            0.31 |                                0.31 |               0.31 |                         0.31 |

Takeaways:

- **Every read path is allocation-free**, including `GetSegments` on ranges that wrap the
  ring end (0 B/op, 0 allocs/op across the board). The two returned segments are direct
  views into the ring.
- **Wrapping costs nothing measurable.** The wrapped `GetSegments` drain is at or below the
  contiguous one at every batch size; the seam between the two segments does not show up.
- **`Iterate` is at parity with `GetSegments`** at every batch size here: it is built on
  `GetSegments`, and the compiler inlines the range-over-func machinery away when the loop
  body is inlinable. Prefer `GetSegments` when you need the slices themselves (bulk copies,
  vector processing), not because the iterator is slower.
- **The per-element `Get` loop pays its masking and bounds work per event**, visible at
  mid batch sizes (0.34 vs 0.23 ns/item at batch 256) and washed out at batch 4096 where
  cache-line fetch dominates.

### Profile-guided optimization (PGO)

All tables in this document are non-PGO builds. This section records what PGO adds on
top, measured 2026-06-12: collect a CPU profile from the core benchmark family, rebuild
the same benchmarks with `-pgo=<profile>`, and compare against `-pgo=off`. One command
runs the whole comparison with a freshly collected profile and prints a `benchstat`
summary (using `go run` to fetch benchstat if it is not installed). The two builds are
executed in interleaved rounds rather than back-to-back blocks, so background load on
the machine widens the reported variance instead of silently biasing one side:

```bash
mise run bench:pgo
```

The `Publish` improvement is the durable signal; deltas of a few percent on the other
rows are within run-to-run noise.

| Benchmark               | no-PGO mean | PGO mean | Delta  |
|-------------------------|------------:|---------:|-------:|
| `Publish`               |     5.47 ns |  4.89 ns | -10.5% |
| `PublishBatch/size=1`   |     7.20 ns |  7.38 ns |  +2.6% |
| `PublishBatch/size=10`  |    21.69 ns | 21.62 ns |  -0.3% |
| `PublishBatch/size=100` |    241.6 ns | 240.0 ns |  -0.7% |
| `PublishBatch/size=1000`|     2326 ns |  2325 ns |  -0.0% |
| `Publish_Direct`        |     9.66 ns |  9.78 ns |  +1.3% |

Reading: PGO wins exactly where the cost is call overhead (the single-publish hot path,
where the compiler raises the inlining budget for the profiled-hot `PublishFunc`) and does
nothing where the cost is memory bandwidth (the batch paths run at the ~30 GB/s store
floor, and no inlining moves a memory wall).

What this means for users: a library cannot ship PGO. The profile applies when the final
binary is compiled, so this gain belongs to whoever builds the application. If publish-path
latency matters to you, collect a profile from your own workload
(`runtime/pprof` or `net/http/pprof` in production, or `-cpuprofile` from a representative
benchmark) and build your binary with `go build -pgo=<profile>` (or check the profile in as
`default.pgo` next to your `main` package). The numbers above suggest roughly 10% on
single-event publish throughput for profiles that capture the publish path as hot.

## Other benchmark coverage

The repository also contains additional benchmark families that are not summarized in the tables above:

- `ring_buffer_false_sharing_bench_test.go` - element-size and reader-lag false-sharing scenarios, including direct-buffer (no API) controls
- `bitmap_reader_bench_test.go` - reader-pool minimum scan costs and reader cursor layout effects
- `ring_buffer_pipeline_bench_test.go` - stage/pipeline depth comparisons

To run the full suite (or `mise run bench` for a quick smoke):

```bash
go test -run '^$' -bench=. .
```

## Latency matrix

The latency matrix tool (`cmd/latency`) sweeps shape × stages × wait strategy × polling × reader count
combinations under a fixed duration, measuring end-to-end, writer-stall, and reader-lag percentiles via
HDR histograms.

Command used:

```bash
go run ./cmd/latency -matrix -duration 10s -output csv
```

### Top 5 configurations by e2e p99

**FixedRate** (500 KHz steady publisher):

| stages | wait   | poll  | readers | e2e p50 | e2e p99 | e2e p99.99 | writer stall p99 | reader lag p99 |
|-------:|--------|-------|--------:|--------:|--------:|-----------:|-----------------:|---------------:|
|      1 | Spin   | Batch |       4 |  21.2µs |  39.8µs |   1213.4µs |           37.1µs |          5.6µs |
|      1 | Yield  | Spin  |       4 |  21.2µs |  40.5µs |   1213.4µs |           37.6µs |          5.6µs |
|      1 | Spin   | Spin  |       4 |  21.1µs |  41.6µs |   1212.4µs |           37.9µs |          6.4µs |
|      1 | Yield  | Batch |       4 |  21.2µs |  41.7µs |   5922.8µs |           37.4µs |          6.6µs |
|      1 | Spin   | Batch |       1 |  20.0µs |  44.1µs |   1208.3µs |           41.4µs |          6.0µs |

**BurstReserve** (1000 events/burst, 5ms idle):

| stages | wait   | poll  | readers | e2e p50 | e2e p99 | e2e p99.99 | writer stall p99 | reader lag p99 |
|-------:|--------|-------|--------:|--------:|--------:|-----------:|-----------------:|---------------:|
|      1 | Yield  | Batch |       1 |   2.1µs |  13.2µs |     55.7µs |            0.4µs |         11.8µs |
|      1 | Spin   | Batch |       1 |   2.1µs |  13.2µs |    117.0µs |            0.4µs |         12.1µs |
|      1 | Hybrid | Batch |       1 |   2.1µs |  13.8µs |     80.9µs |            0.6µs |         12.4µs |
|      1 | Sleep  | Batch |       1 |   2.1µs |  16.9µs |    103.3µs |            0.5µs |         11.5µs |
|      1 | Yield  | Batch |       4 |   2.1µs |  17.8µs |   9756.7µs |            0.4µs |         17.6µs |

### Best p99 per (stages × readers) combo: FixedRate

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

### Best p99 per (stages × readers) combo: BurstReserve

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

### Stages impact: p99 degradation by pipeline depth (FixedRate)

Each row shows the best-polling config for that (readers, stages) combination:

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

### Writer stall p99: FixedRate worst offenders

Configurations where the writer was throttled most by slow readers:

| stages | wait   | poll  | readers | writer stall p99 | writer stall max |
|-------:|--------|-------|--------:|-----------------:|-----------------:|
|      3 | Spin   | Spin  |       8 |        38174.7µs |        59441.2µs |
|      2 | Spin   | Spin  |       8 |        37683.2µs |        55017.5µs |
|      2 | Yield  | Spin  |       8 |        37224.4µs |        42139.6µs |
|      3 | Sleep  | Batch |       8 |        34144.3µs |        42598.4µs |
|      2 | Sleep  | Spin  |       8 |        33652.7µs |        55017.5µs |

### Tail-aware recommendations

Configurations optimized for p99 can degrade sharply at p99.9 and p99.99.
The table below gives the recommended config for each workload when tail
latency (p99.9+) is the primary concern:

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

Key takeaways:

- **`BatchReader` becomes *more* valuable at higher percentiles** in almost every scenario.
  The two exceptions are 8-reader FixedRate (Yield/Yield wins) and 4-reader burst
  (Yield/Batch has a pathological tail blow-up at p99.99).
- **`Sleep` polling is never competitive**: `time.Sleep(1µs)` granularity on Linux
  (~15–50µs) adds 3–5× latency to writer stall and reader lag.
- **Pipeline stages are free with ≤4 total readers.** At 1–4 total readers, all
  stage depths stay within noise of the single-stage baseline (~5.5–5.9 ns/op).
  Overhead grows with total reader count, not stage count.
- **Stages≥2 + readers=8 is a buffer-overflow scenario** for 500 KHz FixedRate:
  the 131K-slot ring buffer can't absorb the throughput when every stage needs to
  finish before the writer can lap. Every config in this quadrant hits ~10s max.
