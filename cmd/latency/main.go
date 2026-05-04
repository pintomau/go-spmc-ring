package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/pintomau/ringring"
	"github.com/pintomau/ringring/internal/latencytest"
)

func main() {
	rate := flag.Int("rate", 500_000, "target publish rate Hz (fixed/hetero shapes)")
	duration := flag.Duration("duration", 30*time.Second, "scenario duration")
	readers := flag.Int("readers", 1, "reader count")
	waitStr := flag.String("wait", "yield", "wait strategy: spin|yield|sleep|hybrid")
	pollingStr := flag.String("polling", "spin", "reader polling: spin|yield|sleep")
	shapeStr := flag.String("shape", "fixed", "workload shape: fixed|burst|hetero")
	stages := flag.Int("stages", 1, "pipeline stage count (1 = no pipeline)")
	outputStr := flag.String("output", "text", "output format: text|json|csv")
	matrix := flag.Bool("matrix", false, "run full scenario matrix")
	burstSize := flag.Int("burst-size", 1000, "burst producer: events per burst")
	burstIdleMs := flag.Int("burst-idle-ms", 5, "burst producer: idle ms between bursts")
	heteroSleepUs := flag.Int("hetero-sleep-us", 100, "hetero slow reader: sleep µs per event")
	flag.Parse()

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

func runMatrix(duration time.Duration, outputFmt string) {
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
	}
	readerCounts := []int{1, 4, 8}

	type row struct {
		wait      string
		polling   string
		readers   int
		p50, p99  time.Duration
		max       time.Duration
	}
	var rows []row

	for _, w := range waits {
		for _, p := range pollings {
			for _, r := range readerCounts {
				fmt.Fprintf(os.Stderr, "running: wait=%s polling=%s readers=%d\n", w, p, r)
				result := latencytest.Run(latencytest.Scenario{
					Shape:      latencytest.FixedRate{RateHz: 500_000},
					WriterWait: w,
					Polling:    p,
					Readers:    r,
					Duration:   duration,
				})
				rows = append(rows, row{
					wait:    w.String(),
					polling: p.String(),
					readers: r,
					p50:     time.Duration(result.EndToEnd.ValueAtQuantile(50)),
					p99:     time.Duration(result.EndToEnd.ValueAtQuantile(99)),
					max:     time.Duration(result.EndToEnd.Max()),
				})
			}
		}
	}

	fmt.Printf("\n%-8s %-8s %-8s %14s %14s %14s\n",
		"wait", "polling", "readers", "e2e_p50", "e2e_p99", "e2e_max")
	fmt.Printf("%s\n", "----------------------------------------------------------------------")
	for _, r := range rows {
		fmt.Printf("%-8s %-8s %-8d %14s %14s %14s\n",
			r.wait, r.polling, r.readers, r.p50, r.p99, r.max)
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
