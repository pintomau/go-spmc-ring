//go:build (amd64 && !cache128) || cache64

package ringring

const CacheLineSize = 64
