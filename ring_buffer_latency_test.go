package ringring_test

import (
	"testing"
	"time"

	"github.com/pintomau/ringring"
	"github.com/pintomau/ringring/internal/latencytest"
)

// All TestLatency_* tests skip under testing.Short(); they run for 10s+ each.

func TestLatency_FixedRate_Baseline(t *testing.T) {
	if testing.Short() {
		t.Skip("latency tests skipped in short mode")
	}
	result := latencytest.Run(latencytest.Scenario{
		Shape:      latencytest.FixedRate{RateHz: 500_000},
		WriterWait: ringring.WaitStrategyYield,
		Polling:    latencytest.SpinReader,
		Readers:    1,
		Duration:   10 * time.Second,
	})
	result.AssertP99Under(t, 50*time.Millisecond)
	result.LogPercentileTable(t)
}

func TestLatency_FixedRate_MultiReader(t *testing.T) {
	if testing.Short() {
		t.Skip("latency tests skipped in short mode")
	}
	result := latencytest.Run(latencytest.Scenario{
		Shape:      latencytest.FixedRate{RateHz: 500_000},
		WriterWait: ringring.WaitStrategyYield,
		Polling:    latencytest.YieldReader,
		Readers:    4,
		Duration:   10 * time.Second,
	})
	result.AssertP99Under(t, 100*time.Millisecond)
	result.LogPercentileTable(t)
}

func TestLatency_Burst_SingleReader(t *testing.T) {
	if testing.Short() {
		t.Skip("latency tests skipped in short mode")
	}
	result := latencytest.Run(latencytest.Scenario{
		Shape:      latencytest.Burst{BurstSize: 1000, IdleMs: 5},
		WriterWait: ringring.WaitStrategyYield,
		Polling:    latencytest.SpinReader,
		Readers:    1,
		Duration:   10 * time.Second,
	})
	result.AssertP99Under(t, 200*time.Millisecond)
	result.LogPercentileTable(t)
}

func TestLatency_BurstReserve_SingleReader(t *testing.T) {
	if testing.Short() {
		t.Skip("latency tests skipped in short mode")
	}
	result := latencytest.Run(latencytest.Scenario{
		Shape:      latencytest.BurstReserve{BurstSize: 1000, IdleMs: 5},
		WriterWait: ringring.WaitStrategyYield,
		Polling:    latencytest.SpinReader,
		Readers:    1,
		Duration:   10 * time.Second,
	})
	result.AssertP99Under(t, 200*time.Millisecond)
	result.LogPercentileTable(t)
}

func TestLatency_FixedRate_BatchReader(t *testing.T) {
	if testing.Short() {
		t.Skip("latency tests skipped in short mode")
	}
	result := latencytest.Run(latencytest.Scenario{
		Shape:      latencytest.FixedRate{RateHz: 500_000},
		WriterWait: ringring.WaitStrategyYield,
		Polling:    latencytest.BatchReader,
		Readers:    1,
		Duration:   10 * time.Second,
	})
	result.AssertP99Under(t, 50*time.Millisecond)
	result.LogPercentileTable(t)
}

func TestLatency_BurstReserve_BatchReader(t *testing.T) {
	if testing.Short() {
		t.Skip("latency tests skipped in short mode")
	}
	result := latencytest.Run(latencytest.Scenario{
		Shape:      latencytest.BurstReserve{BurstSize: 1000, IdleMs: 5},
		WriterWait: ringring.WaitStrategyYield,
		Polling:    latencytest.BatchReader,
		Readers:    1,
		Duration:   10 * time.Second,
	})
	result.AssertP99Under(t, 200*time.Millisecond)
	result.LogPercentileTable(t)
}

func TestLatency_Hetero_SlowReader(t *testing.T) {
	if testing.Short() {
		t.Skip("latency tests skipped in short mode")
	}
	// 100µs/event slow reader can sustain ~10k events/s; use 5k/s for headroom.
	result := latencytest.Run(latencytest.Scenario{
		Shape: latencytest.Hetero{
			RateHz:        5_000,
			SleepPerEvent: 100 * time.Microsecond,
		},
		WriterWait: ringring.WaitStrategyHybrid,
		Polling:    latencytest.SpinReader,
		Duration:   10 * time.Second,
	})
	// No p99 assertion: Hetero is designed to show the slow-reader penalty, not
	// pass a ceiling. time.Sleep granularity (~1ms on Linux) means the slow reader
	// can't sustain 10k events/s regardless of configured sleep duration.
	result.LogPercentileTable(t)
}

func TestLatency_Pipeline_2Stage(t *testing.T) {
	if testing.Short() {
		t.Skip("latency tests skipped in short mode")
	}
	result := latencytest.RunPipeline(latencytest.PipelineScenario{
		Shape:           latencytest.FixedRate{RateHz: 500_000},
		WriterWait:      ringring.WaitStrategyYield,
		Polling:         latencytest.SpinReader,
		ReadersPerStage: 1,
		Stages:          2,
		Duration:        10 * time.Second,
	})
	result.Result.AssertP99Under(t, 100*time.Millisecond)
	result.Result.LogPercentileTable(t)
	for i, h := range result.StageToStage {
		t.Logf("stage %d→%d p99: %v", i, i+1, time.Duration(h.ValueAtQuantile(99)))
	}
}

func TestLatency_Pipeline_3Stage(t *testing.T) {
	if testing.Short() {
		t.Skip("latency tests skipped in short mode")
	}
	result := latencytest.RunPipeline(latencytest.PipelineScenario{
		Shape:           latencytest.FixedRate{RateHz: 100_000},
		WriterWait:      ringring.WaitStrategyYield,
		Polling:         latencytest.YieldReader,
		ReadersPerStage: 1,
		Stages:          3,
		Duration:        10 * time.Second,
	})
	result.Result.AssertP99Under(t, 200*time.Millisecond)
	result.Result.LogPercentileTable(t)
	for i, h := range result.StageToStage {
		t.Logf("stage %d→%d p99: %v", i, i+1, time.Duration(h.ValueAtQuantile(99)))
	}
}

func TestLatency_Pipeline_2Stage_MultiReader(t *testing.T) {
	if testing.Short() {
		t.Skip("latency tests skipped in short mode")
	}
	result := latencytest.RunPipeline(latencytest.PipelineScenario{
		Shape:           latencytest.FixedRate{RateHz: 500_000},
		WriterWait:      ringring.WaitStrategyYield,
		Polling:         latencytest.YieldReader,
		ReadersPerStage: 2,
		Stages:          2,
		Duration:        10 * time.Second,
	})
	result.Result.AssertP99Under(t, 100*time.Millisecond)
	result.Result.LogPercentileTable(t)
	for i, h := range result.StageToStage {
		t.Logf("stage %d→%d p99: %v", i, i+1, time.Duration(h.ValueAtQuantile(99)))
	}
}
