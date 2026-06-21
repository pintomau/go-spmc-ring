//go:build !((darwin && arm64) || ppc64 || ppc64le || s390x)

package spmc

const CacheLineSize = 64
