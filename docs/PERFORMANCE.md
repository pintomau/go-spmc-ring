# Performance data

Full benchmark and latency measurements for [ringring](../README.md). The headline numbers
and configuration guidance live in the README's [Performance](../README.md#performance)
section; this document holds the complete data and per-experiment analysis.

## Benchmarks

The tables below summarize the core ring benchmarks on two machines. Every result
table carries side-by-side columns for both so the architectures can be compared
directly:

| Machine        | OS    | CPU                                | Cores / threads        | Cache line | Go     |
|----------------|-------|------------------------------------|------------------------|-----------:|--------|
| **x86-64**     | Linux | AMD Ryzen 5 9600X 6-Core Processor | 6C / 12T (SMT)         |       64 B | 1.26.2 |
| **arm64**      | macOS | Apple M4 Pro                       | 14 cores / 14T (no SMT)|      128 B | 1.26.2 |

Results are machine-specific. The numbers below are the arithmetic mean of a 10-count benchmark
run; each command is run twice and the lower-mean set is reported, with a run's first element
dropped when it exceeds the mean of the rest by more than 5% (transient warm-up filter).

**Reading the two columns.** The architectures differ in more than clock speed: the M4 Pro
has a 128 B cache line (vs 64 B), no SMT (14 hardware threads from 14 physical cores), a weak
memory model, and runs under the macOS scheduler rather than Linux. Absolute numbers therefore
are *not* directly comparable as a "which CPU is faster" verdict; they capture the whole
platform. Where the **shape** of a curve diverges between the two (not just its magnitude), that
is called out in the per-section commentary, because shape differences usually point at a real
microarchitectural or scheduler effect rather than raw frequency.

Command used (`mise run bench:core`):

```bash
go test -run '^$' -bench 'BenchmarkRingBuffer_(Publish$|Publish_NoReaders$|Publish_Direct$|PublishBatch$|PublishBatchFunc$|Reserve$)' -count=10 .
```

### Core publish path

| Benchmark           | x86-64 ns/op | x86-64 throughput | arm64 ns/op | arm64 throughput | Notes                                                |
|---------------------|-------------:|------------------:|------------:|-----------------:|------------------------------------------------------|
| [`Publish_NoReaders`](../ring_buffer_bench_test.go#L88) |        5.097 |     196.2 M ops/s |       6.967 |    143.5 M ops/s | Writer upper bound with no registered readers        |
| [`Publish`](../ring_buffer_bench_test.go#L22)           |        5.188 |     192.8 M ops/s |       7.258 |    137.8 M ops/s | Keep-up reader adds ~1.8% (x86) / ~4.2% (arm64) overhead vs. no readers |
| [`Publish_Direct`](../ring_buffer_bench_test.go#L131)    |        9.206 |     108.6 M ops/s |       8.725 |    114.6 M ops/s | Constructs and publishes a value per event; see [the gap decomposed](#the-publish_direct-gap-decomposed) |

**arm64 note:** the single-publish hot path is ~40% slower in absolute ns on the M4 Pro
(7.26 vs 5.19 ns), but `Publish_Direct` is the one core row that is actually *faster* on
arm64 (8.73 vs 9.21 ns). The x86 penalty for per-event construction (the store-to-load
forwarding stall described below) does not reproduce on Apple silicon, so the Ryzen's
`Publish` → `Publish_Direct` 1.77× cliff shrinks to 1.20× on the M4. The reader-overhead
share is also larger on arm64 (4.2% vs 1.8%): with no SMT, the keep-up reader competes for a
full physical core instead of a sibling hardware thread.

**How batch publish compares:** at batch size 10 all three batch APIs converge to ~2.1–2.25 ns/item, about 59% cheaper
per-item than a single `Publish` (5.19 ns). The gap (~3.1 ns) is fixed overhead that cannot be amortized per-event:
the gating check, the `writeCursor` store, and cache-line coordination. Batching spreads that cost across N items,
so once the batch is ~10 items the per-item cost is mostly the payload write itself. `Reserve/Commit` amortizes
most efficiently at large sizes (2.12 ns/item at size 1000) because it issues only one `writeCursor.Store`
for the whole batch rather than one per item. See the [Batch scaling](#batch-scaling) section below for
the full numbers.

### The Publish_Direct gap, decomposed

`Publish_Direct` measures 9.21 ns against `Publish`'s 5.19 ns, which reads like a 1.77x
penalty for publishing by value. Controlled variants show the API is responsible for only a small
slice of that:

| Variant | x86-64 ns/op | arm64 ns/op | What it isolates |
|---------|-------------:|------------:|------------------|
| [`PublishFunc`, write 1 byte in the slot](../ring_buffer_bench_test.go#L244)   |  5.12 |  7.23 | The standing `Publish` benchmark workload     |
| [`PublishFunc`, write all 64 B in the slot](../ring_buffer_bench_test.go#L203) |  5.13 | 43.95 | Full payload traffic through the callback API  |
| [`Publish` by value, payload prepared once](../ring_buffer_bench_test.go#L183) |  5.49 |  5.46 | The honest by-value API cost                   |
| [`Publish` by value, value built per event](../ring_buffer_bench_test.go#L224) |  9.18 |  8.46 | The original `Publish_Direct` shape            |

> **arm64 caveat: the headline still holds, but for a different reason; the x86 mechanism does not.**
> On the M4 Pro the `PublishFunc` full-fill row explodes to ~44 ns, 6× the 1-byte fill and 8× the
> by-value `Publish` (5.46). Read naively that says "payload size is the gap on arm64." It isn't: as
> [Batch scaling](#batch-scaling) shows, writing a whole 128 B slot *inline* (`Reserve_Fill`, 1.68
> ns/item) is actually **faster** than writing 1 byte of it (4.24). The 44 ns is specifically the
> **`PublishFunc` closure path**: a per-element callback fails to lower to coalesced full-line stores,
> the same penalty `PublishBatchFunc_Fill` shows in the batch table (3.07 vs `Reserve_Fill`'s 1.68),
> just un-amortized in the single-publish case. By-value and inline full writes don't pay it.
>
> - **By-value `Publish` (5.46 ns) is the *fastest* of the four on arm64**, below even the 1-byte
>   `PublishFunc`. Per-event construction (`Constructed` 8.46 vs `Prepared` 5.46) still costs, but
>   less than x86's (1.55× vs 1.67×).
>
> What does *not* transfer is the **x86 store-to-load-forwarding mechanism** below: the M4's gap is a
> closure codegen effect, not a construction stall. The portable guidance is the same on both
> architectures (prefer by-value `Publish` of a *prepared* value, or an inline `Reserve` fill), with
> one arm64-specific addition: avoid the per-element **closure** fill for cache-line-sized payloads.

Three findings (**x86-64**; see the caveat above for how arm64 diverges):

- **Writing 64 bytes costs the same as writing 1 byte.** The cache-line ownership
  transfer between writer and reader dominates, and it is paid either way. Payload
  size is not the gap.
- **By-value publish itself costs ~7%** (5.49 vs 5.12 ns): one 64 B argument copy plus
  identical call structure. That is the real `Publish` vs `PublishFunc` API difference.
- **The remaining ~3.7 ns is the per-event construction pattern, not the API.** Building a
  fresh composite literal right before the call re-reads those bytes while the construction
  stores are still in flight, which defeats store-to-load forwarding and stalls the pipeline
  every iteration.

Guidance: for cache-line-sized events, prefer `PublishFunc` (or `Reserve`/`Commit` for
batches) and build the event directly in the ring slot; it skips both copies and the
stall entirely. If you must publish by value, reuse a prepared value or fill an existing
variable instead of constructing a fresh composite literal right before the call.

### Multi-reader publish scaling

Each row sweeps reader count for the [`Publish_MultiReader`](../ring_buffer_bench_test.go#L103) benchmark.

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

Degradation is **sub-linear up to the hardware thread count** on both machines, but the curves
have different shapes:

- **x86-64 (Ryzen 5 9600X, 6C/12T)**: going from 1 to 8 readers costs only **6.2%** throughput.
  Overhead stays moderate through 16 readers but accelerates past 12 hardware threads; at 64
  readers throughput is roughly halved, and at 128 (the bitmap capacity limit) it reaches 3×
  single-reader overhead.
- **arm64 (M4 Pro, 14C/14T, no SMT)**: nearly *flat* through 8 readers (only **1.5%**), then a
  sharp **single step at 16 readers** (7.37 → 12.65 ns, +72%) as 16 readers + 1 writer first
  exceed the 14 hardware threads. Past that the curve is gentler than x86's: 32→128 readers
  rises only 14.4 → 17.3 ns, so at 128 readers the M4 is **2.39×** single-reader vs the Ryzen's
  3.04×. With no SMT and a higher physical core count, the M4 keeps every reader on its own core
  for longer (sharper cliff, milder tail), whereas the Ryzen's 12 SMT threads degrade more
  smoothly but further.

On both, the primary driver past ~32 readers is the `scanAll` loop over all 128 cursor slots,
which the writer must run on every slow-path call to find the minimum cursor.

**[`runtime.LockOSThread`](../ring_buffer_bench_test.go#L621)**: pinning each reader and the writer to their own
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

> *The `LockOSThread` sweep above was measured on x86-64/Linux only; thread-pinning interacts
> heavily with the OS scheduler, so do not assume these specific deltas carry over to Apple
> silicon.*

### Stage / pipeline scaling

The stage table below follows the same methodology as the rest of this document.

Command used (`mise run bench:pipeline`):

```bash
go test -run '^$' -bench '^BenchmarkPipeline' -benchtime=1s -count=10 .
```

All four variants now use `keepUpReader` (`time.Sleep(50µs)` idle). The benchmark cases:

- `1Stage_NoPipeline`: direct reader registration on `rb.barrier`; the closest equivalent to the core publish benchmarks
- `1Stage`: one explicit `Stage[T]` (`rb.NewStage(nil)`, `rb.SetGatingStage(s1)`)
- `2Stage` / `3Stage`: explicit pipeline depth; *readers per stage* means N × depth total readers across all stages

**x86-64 (Ryzen 5 9600X):**

| Readers per stage | [`1Stage_NoPipeline`](../ring_buffer_pipeline_bench_test.go#L12) ns/op | [`1Stage`](../ring_buffer_pipeline_bench_test.go#L37) ns/op | [`2Stage`](../ring_buffer_pipeline_bench_test.go#L66) ns/op | [`3Stage`](../ring_buffer_pipeline_bench_test.go#L98) ns/op | Notes                                                                                                        |
|------------------:|--------------------------:|---------------:|---------------:|---------------:|--------------------------------------------------------------------------------------------------------------|
|                 1 |                     5.391 |          5.656 |          5.446 |          5.409 | All variants within 9% of core publish baseline; Stage API overhead is small at 1 reader                     |
|                 2 |                     5.555 |          5.777 |          5.514 |          5.552 | Adding a second reader per stage costs essentially nothing; total readers = 2 / 4 / 6                        |
|                 4 |                     5.625 |          5.805 |          5.689 |          5.834 | Still under 6 ns across the board; 3Stage has 12 total readers                                               |
|                 8 |                     5.854 |          6.005 |          6.112 |          6.613 | 3Stage shows first meaningful overhead: 24 total readers, scanAll runs across 3 stages                       |
|                16 |                     6.262 |          6.504 |          7.281 |          8.669 | Overhead now clearly tied to total reader count (16 / 32 / 48); depth multiplies the scanAll cost            |
|                32 |                     7.753 |          7.915 |         10.180 |         12.940 | 2Stage doubles cost vs 1Stage (96 vs 32 total); 3Stage at 96 total readers approaching MultiReader territory |
|                64 |                    10.710 |         10.900 |         16.070 |         17.480 | 1Stage matches MultiReader/r=64 baseline; 2Stage and 3Stage reflect 128 and 192 total readers                |
|               128 |                    16.490 |         16.690 |         23.090 |         33.380 | 1Stage matches MultiReader/r=128; 3Stage at 384 total readers is 2× worse than 1Stage                        |

**arm64 (Apple M4 Pro):**

| Readers per stage | `1Stage_NoPipeline` ns/op | `1Stage` ns/op | `2Stage` ns/op | `3Stage` ns/op | Notes                                                                                          |
|------------------:|--------------------------:|---------------:|---------------:|---------------:|------------------------------------------------------------------------------------------------|
|                 1 |                     7.221 |          7.267 |          7.208 |          7.161 | All four indistinguishable from the single-reader publish baseline (~7.2 ns)                    |
|                 2 |                     7.215 |          7.292 |          7.280 |          7.201 | Second reader per stage is free; total readers = 2 / 4 / 6                                      |
|                 4 |                     7.304 |          7.330 |          7.359 |          7.309 | Still ~7.3 ns across the board; 3Stage has 12 total readers                                     |
|                 8 |                     7.402 |          7.405 |          8.037 |          8.748 | 3Stage first to move (24 total readers), but still well under the 14-thread cliff               |
|                16 |                    12.262 |         12.675 |         13.532 |         13.661 | All variants step together as total readers cross 14 hardware threads (16 / 32 / 48)            |
|                32 |                    14.351 |         14.313 |         14.068 |         13.850 | Flat across depth; past the thread cliff, depth barely matters on arm64                         |
|                64 |                    15.104 |         15.190 |         14.824 |         16.226 | 1Stage tracks MultiReader/r=64; 2Stage/3Stage stay close                                        |
|               128 |                    17.151 |         17.337 |         19.031 |         23.853 | 1Stage matches MultiReader/r=128; 3Stage at 384 total readers is ~1.4× 1Stage (vs 2× on x86)    |

Key observations:

1. **At 1–4 readers per stage all variants are effectively free on both machines**: costs cluster at the core single-reader publish baseline (x86 ~5.4–5.8 ns, arm64 ~7.2–7.4 ns) and are indistinguishable across pipeline depth.
2. **Overhead scales with total readers, not pipeline depth, on both.** On x86, `2Stage/r=16` (7.28 ns, 32 total) ≈ `MultiReader/r=32` (7.65 ns). On arm64 the same correspondence holds, but with the M4's sharp 14-thread cliff it shows up as *all depths stepping together* at r=16 rather than depth fanning the cost out gradually: at r≥16 the four columns sit within ~1.4 ns of each other until r=128.
3. **Depth costs less on arm64 at the high end.** The Ryzen's `3Stage/r=128` is 2× its `1Stage`; the M4's is only ~1.4×. Same root cause as the multi-reader curve: past the thread cliff the M4's per-reader scan cost grows more slowly.
4. **`1Stage_NoPipeline` and `1Stage` track each other closely on both machines**; the `Stage[T]` wrapper adds ~1–5% on x86 and is within noise (<1%) on arm64.

### Batch scaling

> **What the five batch benchmarks measure (read before comparing columns).**
> `PublishBatch` and `PublishBatchFunc` are both implemented *on top of* `Reserve`/`Commit`
> (see `ring_buffer.go`), so every variant issues **exactly one** `writeCursor.Store` per batch; the
> commit overhead is identical. They differ only in the **per-slot fill loop run between `Reserve` and
> `Commit`**, along two axes:
>
> - **fill size**: a full-slot write (`seg[j] = payload` / `*slot = payload` / `copy()`; 64 B on x86,
>   128 B on arm64) vs. a one-byte poke (`slot.x[0] = '0'`);
> - **API shape**: an inlined loop (`Reserve`), a per-element closure (`PublishBatchFunc`), or a bulk
>   `copy()` memmove (`PublishBatch`).
>
> The slot type is `[CacheLineSize]byte`, so the ring is `1<<22` slots ≈ **256 MB on x86 / 512 MB on
> arm64**, far larger than any cache, so the writer marches through *cold* memory with the keep-up
> reader chasing it. Numbers below are ns/item, lower-of-two-runs.

**The numbers you should expect (equal-work: full-slot fill).** This is the apples-to-apples API
comparison: every column writes the whole slot, so the per-item cost reflects API/access-pattern cost
rather than fill size.

*x86-64 (Ryzen 5 9600X):*

| Batch size | [`PublishBatch`](../ring_buffer_bench_test.go#L376) (bulk copy) | [`Reserve_Fill`](../ring_buffer_bench_test.go#L465) (inline full) | [`PublishBatchFunc_Fill`](../ring_buffer_bench_test.go#L500) (closure full) |
|-----------:|---------------------------:|-----------------------------:|---------------------------------------:|
|          1 |                      7.387 |                        7.029 |                                  7.261 |
|         10 |                      2.218 |                        2.212 |                                  2.255 |
|        100 |                      2.478 |                        2.199 |                                  2.195 |
|       1000 |                      2.395 |                    **2.146** |                                  2.184 |

*arm64 (Apple M4 Pro):*

| Batch size | `PublishBatch` (bulk copy) | `Reserve_Fill` (inline full) | `PublishBatchFunc_Fill` (closure full) |
|-----------:|---------------------------:|-----------------------------:|---------------------------------------:|
|          1 |                      5.531 |                        4.839 |                                  5.986 |
|         10 |                      2.658 |                        2.584 |                                  3.252 |
|        100 |                      4.939 |                        6.333 |                                  5.782 |
|       1000 |                  **1.762** |                    **1.681** |                                  3.068 |

At large batches the full-fill paths sit near one floor on each machine (**~2.1–2.5 ns/item on x86,
~1.7–3.1 on arm64**), with `Reserve_Fill` (inline) and `PublishBatch` (bulk copy) fastest and
`PublishBatchFunc_Fill` (closure) trailing on arm64. **Pick the API for ergonomics; fill the whole
slot contiguously.**

**The contrast: writing one byte of a cold slot.** Swapping the full-slot fill for a one-byte poke
(the original [`Reserve`](../ring_buffer_bench_test.go#L429) / [`PublishBatchFunc`](../ring_buffer_bench_test.go#L404) benchmarks) is where the platforms diverge. These columns
are kept as a deliberate control: they isolate the *fill-size* axis by holding the API fixed, which is
what makes the RFO story below provable rather than asserted.

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

- **On arm64, the one-byte write is ~2.5× slower than the full-slot fill** (`Reserve` 4.24 vs.
  `Reserve_Fill` 1.68 ns/item at size 1000), for two compounding reasons. First, a sub-line store into
  a *cold* 128 B line must read-for-ownership the whole line first, while a full-line store streams
  without that read. Second, the tight 1-byte store stream **oversubscribes the core's
  outstanding-miss buffers**, which a full-line write does not. arm64's 128 B line makes both penalties
  bite harder than x86's 64 B line; this is the "inversion" that made the old 1-byte-only table read
  like "`Reserve` is slow on arm64." It isn't; the *fill* was.
- **On x86 the two fills are indistinguishable** (`Reserve` 2.21 ≈ `Reserve_Fill` 2.15;
  `PublishBatchFunc` 2.24 ≈ `_Fill` 2.18). Per the DirectGap decomposition, a 1-byte and a full-slot
  write to a cold 64 B line cost the same (the line transfer dominates either way), so every variant
  collapses onto one ~2.1 ns floor regardless of fill.
- **The callback cuts both ways, and it's the same indirect call doing it.** On arm64
  `PublishBatchFunc` is *faster* than inline `Reserve` for the 1-byte fill (3.12 vs. 4.24) but *slower*
  for the full fill (3.07 vs. 1.68). For 1-byte writes the per-element call *paces* store issue and
  relieves the oversubscription, so doing more work per slot runs faster; for full writes the call
  boundary blocks the compiler from coalescing the streaming full-line stores, costing it the no-RFO
  fast path (the same effect as the [DirectGap](#the-publish_direct-gap-decomposed) `FullFill` row).
  Net, the callback flattens the fill pattern to **~3.1 ns/item either way**.

**Practical guidance:** on arm64, `Reserve` + an in-place full-slot fill (or `PublishBatch` with a
prepared payload) hits the ~1.7 ns/item floor; dabbing a few bytes into each large cold slot, or
filling via a per-element callback, leaves 1.5–2.5× on the table. On x86 fill shape is free, so
optimize for ergonomics. The size-100 bump is present across *every* variant above, so it is a
working-set/stride artifact (~12.8 KB/batch), not an API property.

> **None of this is about GC.** The ring is a pre-allocated `buffer []T` and `Reserve`/`PublishFunc`
> return views (`*T` / `[]T`) into it, so every fill above (including `seg[j] = payload` in
> `Reserve_Fill`) writes into existing storage and **allocates nothing** (`object` is `[128]byte`, a
> value type; the `payload` is a reused stack var built once; the batch benchmarks confirm 0 allocs/op).
> The numbers compare memory *traffic*, not allocation. Editing slots in place to avoid GC, the usual
> ring-buffer goal, is exactly what this API set is for; it only matters for GC when `T` holds
> pointers/slices (then in-place reuse avoids allocating fresh backing). The finding here is orthogonal:
> whichever pattern you use, on arm64 prefer touching the *whole* cache line over a few bytes of a cold
> one.

### Is the growth linear?

For total batch latency, **yes, approximately** once batches are no longer tiny.

**x86-64 (Ryzen 5 9600X):**

| Benchmark          | 10→100 total-time ratio | 100→1000 total-time ratio | Interpretation                                           |
|--------------------|------------------------:|--------------------------:|----------------------------------------------------------|
| [`PublishBatch`](../ring_buffer_bench_test.go#L376)     |                  12.12x |                     9.62x | Mildly superlinear 10→100, improving again at 1000       |
| [`PublishBatchFunc`](../ring_buffer_bench_test.go#L404) |                  10.16x |                    10.38x | Near-linear across both ranges                           |
| [`Reserve`](../ring_buffer_bench_test.go#L429)          |                  10.16x |                     9.30x | Near-linear with improving per-item cost                 |

**arm64 (Apple M4 Pro):**

| Benchmark          | 10→100 total-time ratio | 100→1000 total-time ratio | Interpretation                                           |
|--------------------|------------------------:|--------------------------:|----------------------------------------------------------|
| `PublishBatch`     |                  18.6x  |                     3.6x  | Distorted by the size-100 bump; a cache/stride artifact, not superlinear growth |
| `PublishBatchFunc` |                  6.16x  |                     8.32x | Close to linear                                          |
| `Reserve`          |                  8.97x  |                     8.65x | Close to linear                                          |

The arm64 `PublishBatch` ratios look erratic only because of the one-off size-100 bump: its
size-1000 per-item cost (1.76 ns) is the lowest of any path on either machine, so treat the M4's
medium-batch (≈100-item) `PublishBatch` cost as locally noisy rather than as runaway growth.

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

**x86-64 (Ryzen 5 9600X):**

| Batch size | [`GetSegments`](../ring_buffer_bench_test.go#L569) (wrapped) ns/item | [`GetSegments`](../ring_buffer_bench_test.go#L582) (contiguous) ns/item | [`Get`](../ring_buffer_bench_test.go#L593) loop ns/item | [`Iterate`](../ring_buffer_bench_test.go#L602) (wrapped) ns/item |
|-----------:|--------------------------------:|-----------------------------------:|-------------------:|----------------------------:|
|         10 |                            0.40 |                                0.37 |               0.39 |                         0.42 |
|        256 |                            0.23 |                                0.27 |               0.34 |                         0.23 |
|       4096 |                            0.31 |                                0.31 |               0.31 |                         0.31 |

**arm64 (Apple M4 Pro):**

| Batch size | `GetSegments` (wrapped) ns/item | `GetSegments` (contiguous) ns/item | `Get` loop ns/item | `Iterate` (wrapped) ns/item |
|-----------:|--------------------------------:|-----------------------------------:|-------------------:|----------------------------:|
|         10 |                            0.45 |                                0.45 |               0.41 |                         0.40 |
|        256 |                            0.30 |                                0.28 |               0.33 |                         0.29 |
|       4096 |                            0.47 |                                0.46 |               0.48 |                         0.47 |

Takeaways (the qualitative conclusions hold on both machines; the magnitudes and the batch-size sweet spot differ):

- **Every read path is allocation-free on both architectures**, including `GetSegments` on ranges
  that wrap the ring end (0 B/op, 0 allocs/op across the board, verified on arm64). The two returned
  segments are direct views into the ring.
- **Wrapping costs nothing measurable on either machine.** The wrapped `GetSegments` drain is at or
  below the contiguous one at every batch size on both; the seam between the two segments does not
  show up.
- **`Iterate` is at parity with `GetSegments`** at every batch size on both, for the same reason: it
  is built on `GetSegments` and the range-over-func machinery inlines away. Prefer `GetSegments` when
  you need the slices themselves, not because the iterator is slower.
- **The per-element `Get` loop pays its masking and bounds work per event** on both, visible at mid
  batch sizes (x86 0.34 vs 0.23; arm64 0.33 vs 0.30 at batch 256).
- **arm64-specific: the large-batch floor is higher and the sweet spot is sharper.** On x86 every path
  settles to ~0.31 ns/item at batch 4096; on the M4 the 4096 traversal rises back to ~0.46–0.48
  ns/item, ~50% above x86. The M4 is fastest at batch **256** (~0.28–0.33) and slower at both ends, a
  more pronounced U-shape, consistent with its larger 128 B cache line making the 4096-element
  (512 KB) working set fall further out of the fastest cache level.

### Profile-guided optimization (PGO)

All tables in this document are non-PGO builds. This section records what PGO adds on
top: collect a CPU profile from the core benchmark family, rebuild
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

**x86-64** (Go 1.26.2):

| Benchmark               | no-PGO mean | PGO mean | Delta  |
|-------------------------|------------:|---------:|-------:|
| [`Publish`](../ring_buffer_bench_test.go#L22)               |     5.47 ns |  4.89 ns | -10.5% |
| [`PublishBatch`](../ring_buffer_bench_test.go#L376)`/size=1`   |     7.20 ns |  7.38 ns |  +2.6% |
| [`PublishBatch`](../ring_buffer_bench_test.go#L376)`/size=10`  |    21.69 ns | 21.62 ns |  -0.3% |
| [`PublishBatch`](../ring_buffer_bench_test.go#L376)`/size=100` |    241.6 ns | 240.0 ns |  -0.7% |
| [`PublishBatch`](../ring_buffer_bench_test.go#L376)`/size=1000`|     2326 ns |  2325 ns |  -0.0% |
| [`Publish_Direct`](../ring_buffer_bench_test.go#L131)        |     9.66 ns |  9.78 ns |  +1.3% |

**arm64 (Apple M4 Pro)** (mean of 10 interleaved rounds, `p<0.05` except where noted):

| Benchmark               | no-PGO mean | PGO mean | Delta            |
|-------------------------|------------:|---------:|-----------------:|
| `Publish`               |     7.39 ns |  7.08 ns | -4.2%            |
| `Publish_NoReaders`     |     7.09 ns |  6.81 ns | -3.9%            |
| `Publish_Direct`        |     8.66 ns |  8.31 ns | -4.0%            |
| `PublishBatch/size=1`   |     5.58 ns |  4.92 ns | -11.8% (±10% var)|
| `PublishBatch/size=10`  |    27.46 ns | 27.48 ns |  ~0% (ns)        |
| `PublishBatch/size=100` |    491.5 ns | 486.9 ns | -0.9%            |
| `PublishBatch/size=1000`|     1752 ns |  1779 ns |  ~0% (ns)        |

(geomean across the full core family: **-2.2%**. "ns" = not statistically significant.)

Reading: on both architectures PGO wins exactly where the cost is **call overhead** (the
single-publish hot path, where the compiler raises the inlining budget for the profiled-hot
`PublishFunc`) and does **nothing where the cost is memory bandwidth** (the batch paths at
size ≥ 10 run at the store-bandwidth floor, unmoved on both). What differs is *distribution
and magnitude*:

- **x86 concentrates the win on `Publish` (-10.5%)** and leaves `Publish_Direct` flat-to-worse
  (+1.3%).
- **arm64 spreads a smaller ~4% across the whole single-publish family**: `Publish`,
  `Publish_NoReaders`, *and* `Publish_Direct` all improve ~4%. The M4 gets less from the same
  inlining decisions on the bare `Publish` hot path but, unlike x86, also speeds up the
  by-value `Publish_Direct`. The `PublishBatch/size=1` -11.8% is real (p=0.000) but the
  PGO arm carries ±10% variance there, so treat the single-item batch figure as soft.

Net: **PGO helps on Apple silicon too, but expect ~4% on the publish path rather than ~10%**,
and a flatter profile across the single-event APIs.

What this means for users: a library cannot ship PGO. The profile applies when the final
binary is compiled, so this gain belongs to whoever builds the application. If publish-path
latency matters to you, collect a profile from your own workload
(`runtime/pprof` or `net/http/pprof` in production, or `-cpuprofile` from a representative
benchmark) and build your binary with `go build -pgo=<profile>` (or check the profile in as
`default.pgo` next to your `main` package). The numbers above suggest roughly **10% on x86-64
and ~4% on arm64** for single-event publish throughput, for profiles that capture the publish
path as hot; measure on your own target, since the gain is architecture-dependent.

## Other benchmark coverage

The repository also contains additional benchmark families that are not summarized in the tables
above: element-size and reader-lag false-sharing scenarios (including direct-buffer controls),
reader-pool scan costs and cursor-layout effects, and stage/pipeline depth comparisons.

To run the full suite (or `mise run bench` for a quick smoke):

```bash
go test -run '^$' -bench=. .
```

## Latency matrix

The latency matrix tool ([`cmd/latency`](../cmd/latency)) sweeps shape × stages × wait strategy × polling × reader count
combinations under a fixed duration, measuring end-to-end, writer-stall, and reader-lag percentiles via
HDR histograms.

Command used:

```bash
go run ./cmd/latency -matrix -duration 10s -output csv
```

> **arm64 vs x86 latency: read absolute numbers with care.** The matrix conflates the whole
> platform: CPU, the macOS vs Linux scheduler, and OS timer granularity (which sets the floor for
> `Sleep` strategies). Two effects dominate the comparison and are visible in every table below:
>
> - **Well-behaved FixedRate configs are dramatically tighter on macOS/M4.** The best FixedRate
>   p99 is **7.0µs on arm64 vs 39.8µs on x86/Linux** (roughly 6× lower), with p50 at 1.8µs vs
>   21µs. Much of this is the OS scheduler/timer behaviour on the steady 500 KHz publisher, not
>   raw CPU speed; treat it as a platform result.
> - **The overflow quadrant (stages ≥ 2 with 8 readers) is *worse* on arm64.** Writer-stall p99 in
>   that quadrant reaches **~62 ms on the M4 vs ~38 ms on the Ryzen**. When the ring can't be
>   drained fast enough, the M4's sharper scheduling cliff hurts more.
>
> The *recommendations* (which wait/poll wins per workload) also shift between platforms; they are
> tabulated separately for each below. Where the best config differs, that is called out.

### Top 5 configurations by e2e p99

**FixedRate** (500 KHz steady publisher), **x86-64:**

| stages | wait   | poll  | readers | e2e p50 | e2e p99 | e2e p99.99 | writer stall p99 | reader lag p99 |
|-------:|--------|-------|--------:|--------:|--------:|-----------:|-----------------:|---------------:|
|      1 | Spin   | Batch |       4 |  21.2µs |  39.8µs |   1213.4µs |           37.1µs |          5.6µs |
|      1 | Yield  | Spin  |       4 |  21.2µs |  40.5µs |   1213.4µs |           37.6µs |          5.6µs |
|      1 | Spin   | Spin  |       4 |  21.1µs |  41.6µs |   1212.4µs |           37.9µs |          6.4µs |
|      1 | Yield  | Batch |       4 |  21.2µs |  41.7µs |   5922.8µs |           37.4µs |          6.6µs |
|      1 | Spin   | Batch |       1 |  20.0µs |  44.1µs |   1208.3µs |           41.4µs |          6.0µs |

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

On arm64 the BurstReserve winners still cluster on `Batch` polling, but `Hybrid` waits edge out
`Yield`, and the per-event reader lag *is* the e2e latency (the two columns match) because in a
drained burst the only meaningful delay is how far behind the reader sits.

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

| stages | readers | best wait | best poll |     p99 |   p99.99 | writer stall p99 |
|-------:|--------:|-----------|-----------|--------:|---------:|-----------------:|
|      1 |       1 | Spin      | Spin      |   7.0µs |   16.9µs |            6.7µs |
|      1 |       4 | Hybrid    | Spin      |  13.3µs |   25.4µs |           13.0µs |
|      1 |       8 | Spin      | Spin      |  17.6µs |   40.3µs |           17.3µs |
|      2 |       1 | Spin      | Spin      |   8.2µs |   22.5µs |            7.6µs |
|      2 |       4 | Hybrid    | Batch     |  16.9µs |   36.9µs |           16.1µs |
|      2 |       8 | Sleep     | Sleep     |  71.0µs |  137.9µs |           38.1µs |
|      3 |       1 | Sleep     | Spin      |  12.8µs |   24.5µs |           12.2µs |
|      3 |       4 | Yield     | Batch     |  15.3µs |  205.6µs |           10.8µs |
|      3 |       8 | Spin      | Sleep     | 110.0µs |  191.1µs |           49.7µs |

On arm64 the winning *poll* strategy shifts toward `Spin` for FixedRate (vs `Batch` on x86), and the
p99 values are 3–6× lower in the well-behaved quadrants. The 8-reader rows still blow up (the ring
overflow scenario), and there the M4 is worse: `2 stages / 8 readers` is 71µs p99 here but its
writer-stall tail (next table) reaches tens of milliseconds.

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

| stages | readers | best wait | best poll |     p99 |   p99.99 | writer stall p99 |
|-------:|--------:|-----------|-----------|--------:|---------:|-----------------:|
|      1 |       1 | Hybrid    | Batch     |  10.0µs |   19.0µs |            0.1µs |
|      1 |       4 | Hybrid    | Batch     |   9.0µs |   30.0µs |            1.0µs |
|      1 |       8 | Hybrid    | Batch     |  13.0µs |  119.0µs |            0.9µs |
|      2 |       1 | Spin      | Batch     |  16.0µs |   37.0µs |            0.6µs |
|      2 |       4 | Yield     | Batch     |  20.0µs |  102.0µs |            0.6µs |
|      2 |       8 | Spin      | Sleep     |  42.0µs |   70.0µs |            0.3µs |
|      3 |       1 | Yield     | Batch     |  17.0µs |   79.0µs |            0.5µs |
|      3 |       4 | Yield     | Batch     |  41.0µs | 1457.2µs |            0.4µs |
|      3 |       8 | Sleep     | Sleep     |  71.0µs |  114.0µs |            1.5µs |

BurstReserve behaves much better than FixedRate at depth on arm64: the worst p99 here is 71µs (3
stages / 8 readers) vs the x86 660µs in the same cell. The idle gaps between bursts let even a deep
pipeline drain on the M4, and `Batch` polling stays the winner across almost every cell, the one
durable cross-platform recommendation in this whole section.

### Stages impact: p99 degradation by pipeline depth (FixedRate)

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

| readers | stages | best wait | best poll |    p50 |     p99 |   p99.99 |       max |
|--------:|-------:|-----------|-----------|-------:|--------:|---------:|----------:|
|       1 |      1 | Spin      | Spin      |  1.8µs |   7.0µs |   16.9µs |    77.5µs |
|       1 |      2 | Spin      | Spin      |  2.0µs |   8.2µs |   22.5µs |    51.5µs |
|       1 |      3 | Sleep     | Spin      |  2.5µs |  12.8µs |   24.5µs |    69.7µs |
|       4 |      1 | Hybrid    | Spin      |  2.4µs |  13.3µs |   25.4µs |    70.7µs |
|       4 |      2 | Hybrid    | Batch     |  3.0µs |  16.9µs |   36.9µs |   148.5µs |
|       4 |      3 | Yield     | Batch     |  3.4µs |  15.3µs |  205.6µs |  1285.1µs |
|       8 |      1 | Spin      | Spin      |  2.6µs |  17.6µs |   40.3µs |  1298.4µs |
|       8 |      2 | Sleep     | Sleep     | 12.5µs |  71.0µs |  137.9µs |   223.6µs |
|       8 |      3 | Spin      | Sleep     | 23.5µs | 110.0µs |  191.1µs |   290.0µs |

The most striking column is **`max`**: on x86 every best-config row still hits ~9.8s (the full test
duration) at least once: a single worst-case stall per run. On arm64 the best configs keep their max
in the **tens-to-hundreds of µs** range (worst 1.3ms), three to four orders of magnitude tighter.
The M4's well-behaved configs avoid the one-off multi-second stall that the Linux/Ryzen runs always
carry in their tail. (The overflow quadrant's *writer-stall* tail is the exception; see below.)

### Writer stall p99: FixedRate worst offenders

Configurations where the writer was throttled most by slow readers.

**x86-64:**

| stages | wait   | poll  | readers | writer stall p99 | writer stall max |
|-------:|--------|-------|--------:|-----------------:|-----------------:|
|      3 | Spin   | Spin  |       8 |        38174.7µs |        59441.2µs |
|      2 | Spin   | Spin  |       8 |        37683.2µs |        55017.5µs |
|      2 | Yield  | Spin  |       8 |        37224.4µs |        42139.6µs |
|      3 | Sleep  | Batch |       8 |        34144.3µs |        42598.4µs |
|      2 | Sleep  | Spin  |       8 |        33652.7µs |        55017.5µs |

**arm64 (Apple M4 Pro):**

| stages | wait   | poll  | readers | writer stall p99 | writer stall max |
|-------:|--------|-------|--------:|-----------------:|-----------------:|
|      2 | Hybrid | Batch |       8 |        62357.5µs |        98631.7µs |
|      3 | Yield  | Spin  |       8 |        62160.9µs |       111804.4µs |
|      2 | Hybrid | Spin  |       8 |        61538.3µs |        91226.1µs |
|      3 | Spin   | Spin  |       8 |        61276.2µs |       104333.3µs |
|      3 | Spin   | Batch |       8 |        61046.8µs |        77922.3µs |

This is the one place arm64 is clearly *worse*: in the 8-reader overflow quadrant the M4's writer-stall
p99 reaches ~62ms (vs ~38ms on the Ryzen) and its max exceeds 110ms. When the ring genuinely cannot be
drained, the M4 throttles the writer harder, the flip side of its sharp scheduling cliff. The takeaway
is unchanged on both platforms: **do not run stages ≥ 2 with 8 readers at 500 KHz on a 131K-slot ring**;
size the ring or reduce per-stage reader count.

### Tail-aware recommendations

Configurations optimized for p99 can degrade sharply at p99.9 and p99.99.
The table below gives the recommended config for each workload when tail
latency (p99.9+) is the primary concern:

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

| Workload                | Rec for p99   | Rec for p99.9+     | Why it changed                                     |
|-------------------------|---------------|--------------------|----------------------------------------------------|
| FixedRate, 1 reader     | `Spin/Spin`   | `Spin/Spin`        | No change (Yield/Spin ties within noise)           |
| FixedRate, 4 readers    | `Hybrid/Spin` | `Hybrid/Spin`      | No change                                           |
| FixedRate, 8 readers    | `Spin/Spin`   | `Hybrid/Batch`     | Marginal: Batch ~2% better p99.9 (24.8 vs 25.3µs)  |
| FixedRate, 2 stages     | `Spin/Spin`   | `Spin/Spin`        | No change (Sleep/Spin ties at 14.1µs)              |
| FixedRate, 3 stages     | `Sleep/Spin`  | `Sleep/Spin`       | No change (Hybrid/Batch ties at 19.0µs)            |
| BurstReserve, 1 reader  | `Hybrid/Batch`| `Hybrid/Batch`     | No change                                           |
| BurstReserve, 4 readers | `Hybrid/Batch`| `Hybrid/Batch`     | No change; **no p99.99 blow-up** (unlike x86)       |
| BurstReserve, 2 stages  | `Spin/Batch`  | **`Yield/Batch`**  | Yield/Batch ~7% better p99.9 (25 vs 27µs)          |
| BurstReserve, 3 stages  | `Yield/Batch` | **`Spin/Spin`**    | Spin/Spin = 2× better p99.9 (25 vs 52µs)           |

Key takeaways:

- **The winning poll strategy is platform-dependent for FixedRate.** On x86/Linux, `Batch` polling
  wins the steady-rate workload; on arm64/macOS, **`Spin` polling wins** (the FixedRate recommendations
  flip from `*/Batch` to `*/Spin`). `Batch` remains the durable winner for **BurstReserve on both**
  platforms.
- **arm64 configs are far more stable across percentiles.** Almost every arm64 row is "no change" from
  p99 to p99.9+, whereas x86 has several flips. The M4's tighter tails (see the `max` discussion above)
  mean the p99-optimal config usually stays optimal deeper into the tail.
- **The x86 4-reader BurstReserve p99.99 pathology does not reproduce on arm64.** On Ryzen, `Yield/Batch`
  blows up to 9.8ms at p99.99; on the M4 the same workload's p99.99 stays at 30µs, so no config swap is
  needed.
- **`Sleep` polling is rarely competitive on either platform**, but for a different reason on each:
  `time.Sleep(1µs)` granularity is ~15–50µs on Linux and similarly coarse on macOS, adding latency to
  writer stall and reader lag. (It does win a couple of arm64 cells at depth, where coarse polling
  happens to reduce contention.)
- **Pipeline stages are free with ≤4 total readers on both machines.** At 1–4 total readers all stage
  depths stay within noise of the single-stage baseline (x86 ~5.5–5.9 ns/op, arm64 ~7.2–7.4 ns/op).
  Overhead grows with total reader count, not stage count.
- **Stages ≥ 2 + readers = 8 is a buffer-overflow scenario** for 500 KHz FixedRate on both: the
  131K-slot ring can't absorb the throughput when every stage must finish before the writer laps. On x86
  every config in this quadrant hits ~10s max; on arm64 the *e2e* tails stay tighter but the
  **writer-stall** tail is worse (~62ms p99). Either way: size the ring up or cut per-stage readers.
