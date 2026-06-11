package latencytest

// Payload is the ring-buffer element type for latency tests. All three
// timestamps are written by the producer; ConsumedAt is overwritten by each
// reader goroutine immediately on receipt.
type Payload struct {
	EnqueuedAt  int64 // ticker fire time, the intended dispatch (coordinated-omission anchor)
	PublishedAt int64 // inside PublishFunc, after any writer stall resolves
	Seq         int64
	_           [CacheLineSize - 24]byte // pad to cache line to avoid false sharing
}
