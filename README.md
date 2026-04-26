# ringring

Experimental single-writer, multiple-reader ring buffer in Go, inspired by the LMAX Disruptor pattern. The codebase focuses on cache-line awareness, false-sharing avoidance, and lock-free reader lifecycle management.

## Benchmarks

The tables below summarize the core ring benchmarks on this machine:

| Environment | Value |
| --- | --- |
| OS | Linux |
| CPU | AMD Ryzen 5 9600X 6-Core Processor |
| Go | 1.26.2 |

Results are machine-specific. The numbers below are the arithmetic mean of 100 benchmark runs.

Command used:

```bash
go test -run '^$' -bench 'BenchmarkRingBuffer_(Publish$|Publish_NoReaders$|Publish_Direct$|PublishBatch$|PublishBatchFunc$|Reserve$)' -count=100 .
```

### Core publish path

| Benchmark | Mean ns/op | Throughput | Notes |
| --- | ---: | ---: | --- |
| `Publish_NoReaders` | 5.658 | 176.8 M ops/s | Writer upper bound with no registered readers |
| `Publish` | 5.712 | 175.1 M ops/s | Keep-up reader adds about 1.0% overhead vs. no readers |
| `Publish_Direct` | 10.162 | 98.4 M ops/s | Direct value publish is about 1.78x slower than `Publish` |

### Multi-reader publish scaling

Command used:

```bash
go test -run '^$' -bench 'BenchmarkRingBuffer_Publish_MultiReader$' -count=100 .
```

| Readers | Mean ns/op | Throughput | Slowdown vs 1 reader | Notes |
| ---: | ---: | ---: | ---: | --- |
| 1 | 5.409 | 184.9 M ops/s | 1.000x | Single keep-up reader |
| 2 | 5.442 | 183.7 M ops/s | 1.006x | Essentially flat vs. 1 reader |
| 4 | 5.497 | 181.9 M ops/s | 1.016x | Small additional gating cost |
| 8 | 5.858 | 170.7 M ops/s | 1.083x | Noticeable but still modest overhead |

This is **not linear degradation** with reader count. On this machine, increasing from **1 to 8 keep-up readers** only reduced throughput by about **7.7%**.

### Side-by-side comparison with `smarty/go-disruptor`

For a local comparison on the same machine, the cloned `go-disruptor` workspace at `/home/pintomau/projects/go-disruptor` was updated to use:

```go
toolchain go1.26.2
```

Command used in that repository:

```bash
go test -run '^$' -bench '^BenchmarkChannel$/^SPSC$/^Blocking$' -count=100 .
go test -run '^$' -bench '^BenchmarkSequencer$/^SP_SC$' -count=100 .
go test -run '^$' -bench '^BenchmarkSequencer$/^SP_MC$' -count=100 .
go test -run '^$' -bench '^BenchmarkSharedSequencer$/^SP_SC$/^R1$' -count=100 .
```

| Scenario | `ringring` | `go-disruptor` | Go channel | Read |
| --- | ---: | ---: | ---: | --- |
| Single writer, 1 keep-up reader | **5.409 ns/op** (184.9 M ops/s) | 5.856 ns/op (170.8 M ops/s) `SP_SC` | 36.108 ns/op (27.7 M ops/s) `SPSC/Blocking` | `ringring` is about 1.08x faster than `go-disruptor`, both far ahead of channels |
| Single writer, 2 keep-up readers vs multi-consumer | **5.442 ns/op** (183.7 M ops/s) | 5.898 ns/op (169.6 M ops/s) `SP_MC` | N/A | `ringring` is about 1.08x faster on the closest comparable multi-consumer case |
| Single writer upper bound | **5.658 ns/op** (176.8 M ops/s) `Publish_NoReaders` | 5.811 ns/op (172.1 M ops/s) `SharedSequencer/SP_SC/R1` | N/A | Essentially the same ballpark |

These are the closest comparable control-path numbers we have today. The published `go-disruptor` batch benchmarks (`SP4SC`, `SP4MC`) are **not directly comparable** to `ringring`'s current batch tables because `go-disruptor` is mostly measuring reserve/commit coordination there, while `ringring`'s batch benchmarks include payload writes and real reader interaction.

### Batch scaling

| Batch size | `PublishBatch` ns/op | `PublishBatch` ns/item | `PublishBatchFunc` ns/op | `PublishBatchFunc` ns/item | `Reserve` ns/op | `Reserve` ns/item |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 7.610 | 7.610 | 7.072 | 7.072 | 6.588 | 6.588 |
| 10 | 23.156 | 2.316 | 23.074 | 2.307 | 24.380 | 2.438 |
| 100 | 252.247 | 2.522 | 220.855 | 2.209 | 239.197 | 2.392 |
| 1000 | 2433.050 | 2.433 | 2213.400 | 2.213 | 2169.650 | 2.170 |

At larger batch sizes the throughput settles in the **396-461 M items/s** range, and the per-item cost flattens to roughly **2.17-2.52 ns/item**.

### Is the growth linear?

For total batch latency, **yes, approximately** once batches are no longer tiny.

| Benchmark | 10→100 total-time ratio | 100→1000 total-time ratio | Interpretation |
| --- | ---: | ---: | --- |
| `PublishBatch` | 10.89x | 9.65x | Near-linear with slightly improving cost at larger sizes |
| `PublishBatchFunc` | 9.57x | 10.02x | Near-linear across both ranges |
| `Reserve` | 9.81x | 9.07x | Near-linear with improving per-item cost |

The more useful signal is the **per-item** cost:

- from size `1` to size `10`, fixed overhead is heavily amortized
- from size `10` onward, per-item cost is mostly flat
- there is **no sign of runaway superlinear growth** in the core batch path

So the practical conclusion is:

1. single-item operations pay a meaningful fixed cost
2. batching improves throughput substantially
3. for medium and large batches, total time grows close to linearly while cost per item stays nearly constant

## Other benchmark coverage

The repository also contains additional benchmark families that are not summarized in the tables above:

- `ring_buffer_access_bench_test.go` - direct buffer access comparisons
- `ring_buffer_false_sharing_bench_test.go` - element-size and reader-lag false-sharing scenarios
- `bitmap_reader_pool_bench_test.go` - reader-pool minimum scan costs
- `bitmap_reader_false_sharing_bench_test.go` - reader cursor layout effects

To run the full suite:

```bash
go test -bench=. ./...
```
