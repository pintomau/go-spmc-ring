package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pintomau/ringring"
	"github.com/pintomau/ringring/internal/latencytest"
)

// MatrixRow holds the result of one scenario run in the matrix sweep.
type MatrixRow struct {
	Shape   string `json:"shape"`
	Wait    string `json:"wait"`
	Polling string `json:"polling"`
	Readers int    `json:"readers"`
	Stages  int    `json:"stages"`
	// EndToEnd latency percentiles.
	P50   time.Duration `json:"e2e_p50_ns"`
	P95   time.Duration `json:"e2e_p95_ns"`
	P99   time.Duration `json:"e2e_p99_ns"`
	P999  time.Duration `json:"e2e_p99_9_ns"`
	P9999 time.Duration `json:"e2e_p99_99_ns"`
	Max   time.Duration `json:"e2e_max_ns"`
	// WriterStall: how long the writer was throttled waiting for slow readers.
	WS50   time.Duration `json:"writer_stall_p50_ns"`
	WS95   time.Duration `json:"writer_stall_p95_ns"`
	WS99   time.Duration `json:"writer_stall_p99_ns"`
	WS999  time.Duration `json:"writer_stall_p99_9_ns"`
	WS9999 time.Duration `json:"writer_stall_p99_99_ns"`
	WSMax  time.Duration `json:"writer_stall_max_ns"`
	// ReaderLag: how far behind readers fell from the writer cursor.
	RL50   time.Duration `json:"reader_lag_p50_ns"`
	RL95   time.Duration `json:"reader_lag_p95_ns"`
	RL99   time.Duration `json:"reader_lag_p99_ns"`
	RL999  time.Duration `json:"reader_lag_p99_9_ns"`
	RL9999 time.Duration `json:"reader_lag_p99_99_ns"`
	RLMax  time.Duration `json:"reader_lag_max_ns"`
}

func main() {
	rate := flag.Int("rate", 500_000, "target publish rate Hz (fixed/hetero shapes)")
	duration := flag.Duration("duration", 10*time.Second, "scenario duration")
	readers := flag.Int("readers", 1, "reader count")
	waitStr := flag.String("wait", "yield", "wait strategy: spin|yield|sleep|hybrid")
	pollingStr := flag.String("polling", "spin", "reader polling: spin|yield|sleep|batch")
	shapeStr := flag.String("shape", "fixed", "workload shape: fixed|burst|burst-reserve|hetero")
	stages := flag.Int("stages", 1, "pipeline stage count (1 = no pipeline)")
	outputStr := flag.String("output", "text", "output format: text|json|csv|")
	matrix := flag.Bool("matrix", false, "run full scenario matrix")
	burstSize := flag.Int("burst-size", 1000, "burst producer: events per burst")
	burstIdleMs := flag.Int("burst-idle-ms", 5, "burst producer: idle ms between bursts")
	heteroSleepUs := flag.Int("hetero-sleep-us", 100, "hetero slow reader: sleep µs per event")
	flag.Parse()

	fmt.Fprintf(os.Stderr,
		"cacheSize=%d\n",
		latencytest.CacheLineSize)

	wait := parseWait(*waitStr)
	polling := parsePolling(*pollingStr)

	if *matrix {
		runMatrix(*duration, *outputStr)
		return
	}

	shape := parseShape(*shapeStr, *rate, *burstSize, *burstIdleMs, *heteroSleepUs)

	if *stages > 1 {
		result := latencytest.RunPipeline(latencytest.PipelineScenario{
			Shape:           shape,
			WriterWait:      wait,
			Polling:         polling,
			ReadersPerStage: *readers,
			Stages:          *stages,
			Duration:        *duration,
		})
		printResult(result.Result, *outputStr)
		for i, h := range result.StageToStage {
			fmt.Printf("stage %d→%d  p50=%v  p99=%v  max=%v\n",
				i, i+1,
				time.Duration(h.ValueAtQuantile(50)),
				time.Duration(h.ValueAtQuantile(99)),
				time.Duration(h.Max()))
		}
		return
	}

	result := latencytest.Run(latencytest.Scenario{
		Shape:      shape,
		WriterWait: wait,
		Polling:    polling,
		Readers:    *readers,
		Duration:   *duration,
	})
	printResult(result, *outputStr)
}

func parseWait(s string) ringring.WaitStrategy {
	switch s {
	case "spin":
		return ringring.WaitStrategySpin
	case "sleep":
		return ringring.WaitStrategySleep
	case "hybrid":
		return ringring.WaitStrategyHybrid
	default:
		return ringring.WaitStrategyYield
	}
}

func parsePolling(s string) latencytest.ReaderPolling {
	switch s {
	case "yield":
		return latencytest.YieldReader
	case "sleep":
		return latencytest.SleepReader
	case "batch":
		return latencytest.BatchReader
	default:
		return latencytest.SpinReader
	}
}

func parseShape(s string, rate, burstSize, burstIdleMs, heteroSleepUs int) latencytest.WorkloadShape {
	switch s {
	case "burst":
		return latencytest.Burst{BurstSize: burstSize, IdleMs: burstIdleMs}
	case "burst-reserve":
		return latencytest.BurstReserve{BurstSize: burstSize, IdleMs: burstIdleMs}
	case "hetero":
		return latencytest.Hetero{RateHz: rate, SleepPerEvent: time.Duration(heteroSleepUs) * time.Microsecond}
	default:
		return latencytest.FixedRate{RateHz: rate}
	}
}

// runMatrixScenario executes one cell of the matrix. Stages > 1 uses
// RunPipeline; stages == 1 uses Run.
func runMatrixScenario(
	shape latencytest.WorkloadShape,
	wait ringring.WaitStrategy,
	polling latencytest.ReaderPolling,
	readers, stages int,
	duration time.Duration,
) latencytest.Result {
	if stages > 1 {
		pr := latencytest.RunPipeline(latencytest.PipelineScenario{
			Shape:           shape,
			WriterWait:      wait,
			Polling:         polling,
			ReadersPerStage: readers,
			Stages:          stages,
			Duration:        duration,
		})
		return pr.Result
	}
	return latencytest.Run(latencytest.Scenario{
		Shape:      shape,
		WriterWait: wait,
		Polling:    polling,
		Readers:    readers,
		Duration:   duration,
	})
}

// collectMatrix runs all shape × stages × wait × polling × reader-count
// combinations and returns the collected rows. Progress is written to stderr.
func collectMatrix(duration time.Duration) []MatrixRow {
	type shapeDef struct {
		name  string
		shape latencytest.WorkloadShape
	}
	shapes := []shapeDef{
		{"FixedRate", latencytest.FixedRate{RateHz: 500_000}},
		{"BurstReserve", latencytest.BurstReserve{BurstSize: 1000, IdleMs: 5}},
	}
	waits := []ringring.WaitStrategy{
		ringring.WaitStrategySpin,
		ringring.WaitStrategyYield,
		ringring.WaitStrategySleep,
		ringring.WaitStrategyHybrid,
	}
	pollings := []latencytest.ReaderPolling{
		latencytest.SpinReader,
		latencytest.YieldReader,
		latencytest.SleepReader,
		latencytest.BatchReader,
	}
	stageCounts := []int{1, 2, 3}
	readerCounts := []int{1, 4, 8}

	var rows []MatrixRow
	for _, sh := range shapes {
		for _, stages := range stageCounts {
			for _, w := range waits {
				for _, p := range pollings {
					for _, r := range readerCounts {
						fmt.Fprintf(os.Stderr,
							"running: shape=%s stages=%d wait=%s polling=%s readers=%d\n",
							sh.name, stages, w, p, r)
						result := runMatrixScenario(sh.shape, w, p, r, stages, duration)
						rows = append(rows, MatrixRow{
							Shape:   sh.name,
							Stages:  stages,
							Wait:    w.String(),
							Polling: p.String(),
							Readers: r,
							P50:     time.Duration(result.EndToEnd.ValueAtQuantile(50)),
							P95:     time.Duration(result.EndToEnd.ValueAtQuantile(95)),
							P99:     time.Duration(result.EndToEnd.ValueAtQuantile(99)),
							P999:    time.Duration(result.EndToEnd.ValueAtQuantile(99.9)),
							P9999:   time.Duration(result.EndToEnd.ValueAtQuantile(99.99)),
							Max:     time.Duration(result.EndToEnd.Max()),
							WS50:    time.Duration(result.WriterStall.ValueAtQuantile(50)),
							WS95:    time.Duration(result.WriterStall.ValueAtQuantile(95)),
							WS99:    time.Duration(result.WriterStall.ValueAtQuantile(99)),
							WS999:   time.Duration(result.WriterStall.ValueAtQuantile(99.9)),
							WS9999:  time.Duration(result.WriterStall.ValueAtQuantile(99.99)),
							WSMax:   time.Duration(result.WriterStall.Max()),
							RL50:    time.Duration(result.ReaderLag.ValueAtQuantile(50)),
							RL95:    time.Duration(result.ReaderLag.ValueAtQuantile(95)),
							RL99:    time.Duration(result.ReaderLag.ValueAtQuantile(99)),
							RL999:   time.Duration(result.ReaderLag.ValueAtQuantile(99.9)),
							RL9999:  time.Duration(result.ReaderLag.ValueAtQuantile(99.99)),
							RLMax:   time.Duration(result.ReaderLag.Max()),
						})
					}
				}
			}
		}
	}
	return rows
}

func runMatrix(duration time.Duration, outputFmt string) {
	rows := collectMatrix(duration)

	switch outputFmt {
	case "json":
		b, err := json.Marshal(rows)
		if err != nil {
			fmt.Fprintln(os.Stderr, "json marshal error:", err)
			os.Exit(1)
		}
		fmt.Println(string(b))
	case "csv":
		var sb strings.Builder
		w := csv.NewWriter(&sb)
		_ = w.Write([]string{
			"shape", "stages", "wait", "polling", "readers",
			"e2e_p50_ns", "e2e_p95_ns", "e2e_p99_ns", "e2e_p99_9_ns", "e2e_p99_99_ns", "e2e_max_ns",
			"writer_stall_p50_ns", "writer_stall_p95_ns", "writer_stall_p99_ns", "writer_stall_p99_9_ns", "writer_stall_p99_99_ns", "writer_stall_max_ns",
			"reader_lag_p50_ns", "reader_lag_p95_ns", "reader_lag_p99_ns", "reader_lag_p99_9_ns", "reader_lag_p99_99_ns", "reader_lag_max_ns",
		})
		for _, r := range rows {
			_ = w.Write([]string{
				r.Shape,
				fmt.Sprintf("%d", r.Stages),
				r.Wait,
				r.Polling,
				fmt.Sprintf("%d", r.Readers),
				fmt.Sprintf("%d", int64(r.P50)),
				fmt.Sprintf("%d", int64(r.P95)),
				fmt.Sprintf("%d", int64(r.P99)),
				fmt.Sprintf("%d", int64(r.P999)),
				fmt.Sprintf("%d", int64(r.P9999)),
				fmt.Sprintf("%d", int64(r.Max)),
				fmt.Sprintf("%d", int64(r.WS50)),
				fmt.Sprintf("%d", int64(r.WS95)),
				fmt.Sprintf("%d", int64(r.WS99)),
				fmt.Sprintf("%d", int64(r.WS999)),
				fmt.Sprintf("%d", int64(r.WS9999)),
				fmt.Sprintf("%d", int64(r.WSMax)),
				fmt.Sprintf("%d", int64(r.RL50)),
				fmt.Sprintf("%d", int64(r.RL95)),
				fmt.Sprintf("%d", int64(r.RL99)),
				fmt.Sprintf("%d", int64(r.RL999)),
				fmt.Sprintf("%d", int64(r.RL9999)),
				fmt.Sprintf("%d", int64(r.RLMax)),
			})
		}
		w.Flush()
		if err := w.Error(); err != nil {
			fmt.Fprintln(os.Stderr, "csv error:", err)
			os.Exit(1)
		}
		fmt.Print(sb.String())
	default:
		fmt.Printf("\n%-12s %-6s %-8s %-8s %-8s %14s %14s %14s\n",
			"shape", "stg", "wait", "polling", "readers", "e2e_p50", "e2e_p99", "e2e_max")
		fmt.Printf("%s\n", "--------------------------------------------------------------------------------------------")
		for _, r := range rows {
			fmt.Printf("%-12s %-6d %-8s %-8s %-8d %14s %14s %14s\n",
				r.Shape, r.Stages, r.Wait, r.Polling, r.Readers, r.P50, r.P99, r.Max)
		}
	}
}

func printResult(result latencytest.Result, outputFmt string) {
	switch outputFmt {
	case "json":
		b, err := result.MarshalJSON()
		if err != nil {
			fmt.Fprintln(os.Stderr, "json marshal error:", err)
			os.Exit(1)
		}
		fmt.Println(string(b))
	case "csv":
		s, err := result.MarshalCSV()
		if err != nil {
			fmt.Fprintln(os.Stderr, "csv marshal error:", err)
			os.Exit(1)
		}
		fmt.Print(s)
	default:
		fmt.Print(result.PercentileTableText())
	}
}
