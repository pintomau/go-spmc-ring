package spmc

import (
	"runtime"
	"time"
)

// WaitStrategy defines the wait strategy for writer backpressure.
// Using a config enum + switch is usually faster than interface/generic dispatch
// in tight spin loops due to Go's interface/generic dictionary overhead.
// See docs/PERFORMANCE.md for the latency matrix.
type WaitStrategy uint8

const (
	// WaitStrategySpin performs pure busy-spinning.
	// Lowest latency, highest CPU usage. Use for ultra-low-latency scenarios
	// where backpressure is rare and brief.
	WaitStrategySpin WaitStrategy = iota

	// WaitStrategyYield calls runtime.Gosched() to yield to other goroutines.
	// Conservative default. For latency-sensitive workloads with 1-4 readers,
	// WaitStrategySpin is faster. For bursty workloads on arm64, consider
	// WaitStrategyHybrid.
	WaitStrategyYield

	// WaitStrategySleep sleeps for a configurable duration.
	// Lowest CPU usage, higher latency. Good for throughput-oriented scenarios.
	WaitStrategySleep

	// WaitStrategyHybrid starts with yielding and backs off to sleeping
	// under sustained contention. Best for variable workloads.
	WaitStrategyHybrid
)

// String returns the name of the wait strategy
func (w WaitStrategy) String() string {
	switch w {
	case WaitStrategySpin:
		return "Spin"
	case WaitStrategyYield:
		return "Yield"
	case WaitStrategySleep:
		return "Sleep"
	case WaitStrategyHybrid:
		return "Hybrid"
	default:
		return "Unknown"
	}
}

// WaitStrategyParams holds optional parameters for wait strategies
type WaitStrategyParams struct {
	// SleepDuration for WaitStrategySleep (default: 1μs)
	SleepDuration time.Duration // 8 bytes

	// HybridSpinCount before backing off to sleep (default: 100)
	HybridSpinCount uint64 // 8 bytes

	// HybridSleepDuration after spin count exceeded (default: 1μs)
	HybridSleepDuration time.Duration // 8 bytes
}

// wait executes the wait strategy based on configuration.
func wait(config WaitStrategy, params *WaitStrategyParams, spins uint64) {
	switch config {
	case WaitStrategySpin:
		// Pure spin - do nothing
		return

	case WaitStrategyYield:
		runtime.Gosched()

	case WaitStrategySleep:
		if params.SleepDuration > 0 {
			time.Sleep(params.SleepDuration)
		} else {
			time.Sleep(time.Microsecond)
		}

	case WaitStrategyHybrid:
		var maxSpins uint64 = 100
		sleepDur := time.Microsecond
		if params.HybridSpinCount > 0 {
			maxSpins = params.HybridSpinCount
		}
		if params.HybridSleepDuration > 0 {
			sleepDur = params.HybridSleepDuration
		}
		if spins < maxSpins {
			runtime.Gosched()
		} else {
			time.Sleep(sleepDur)
		}
	}
}
