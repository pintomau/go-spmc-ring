package latencytest

// cacheLineSize matches the host's CacheLineSize constant. Hardcoded here to
// avoid importing the ringring package (which would create a cycle since tests
// live in the same module). On arm64 this should be 128; everywhere else 64.
const cacheLineSize = 64

// Payload is the ring-buffer element type for latency tests. All three
// timestamps are written by the producer; ConsumedAt is overwritten by each
// reader goroutine immediately on receipt.
type Payload struct {
	EnqueuedAt  int64 // ticker fire time — intended dispatch (coordinated-omission anchor)
	PublishedAt int64 // inside PublishFunc — after any writer stall resolves
	Seq         int64
	_           [cacheLineSize - 24]byte // pad to cache line to avoid false sharing
}
