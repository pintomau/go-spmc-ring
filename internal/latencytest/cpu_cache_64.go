//go:build (amd64 && !cache128) || cache64

package latencytest

const CacheLineSize = 64
