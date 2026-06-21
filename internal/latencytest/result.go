package latencytest

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

// Result holds the merged histograms from all reader goroutines in a scenario.
type Result struct {
	WriterStall *hdrhistogram.Histogram
	ReaderLag   *hdrhistogram.Histogram
	EndToEnd    *hdrhistogram.Histogram
}

// buildResult merges per-reader recorders into a single Result.
func buildResult(recs []*LatencyRecorder) Result {
	merged := Result{
		WriterStall: hdrhistogram.New(1, int64(10*time.Second), 3),
		ReaderLag:   hdrhistogram.New(1, int64(10*time.Second), 3),
		EndToEnd:    hdrhistogram.New(1, int64(10*time.Second), 3),
	}
	for _, r := range recs {
		merged.WriterStall.Merge(r.WriterStall)
		merged.ReaderLag.Merge(r.ReaderLag)
		merged.EndToEnd.Merge(r.EndToEnd)
	}
	return merged
}

// AssertP99Under fails t if the p99 end-to-end latency exceeds ceiling. This is
// a sanity ceiling against catastrophic regressions, not a hardware SLA.
func (r Result) AssertP99Under(t *testing.T, ceiling time.Duration) {
	t.Helper()
	p99 := time.Duration(r.EndToEnd.ValueAtQuantile(99))
	if p99 > ceiling {
		t.Errorf("p99 end-to-end latency %v exceeds ceiling %v", p99, ceiling)
	}
}

// LogPercentileTable logs p50/p95/p99/p99.9/p99.99/max for all three histograms.
func (r Result) LogPercentileTable(t *testing.T) {
	t.Helper()
	t.Logf("%-14s %12s %12s %12s %12s %12s %12s",
		"histogram", "p50", "p95", "p99", "p99.9", "p99.99", "max")
	for _, row := range r.rows() {
		t.Logf("%-14s %12s %12s %12s %12s %12s %12s",
			row.name,
			fmtDur(row.p50), fmtDur(row.p95), fmtDur(row.p99),
			fmtDur(row.p999), fmtDur(row.p9999), fmtDur(row.max))
	}
}

type percentileRow struct {
	name                       string
	p50, p95, p99, p999, p9999 int64
	max                        int64
}

func (r Result) rows() []percentileRow {
	return []percentileRow{
		histRow("writer_stall", r.WriterStall),
		histRow("reader_lag", r.ReaderLag),
		histRow("end_to_end", r.EndToEnd),
	}
}

func histRow(name string, h *hdrhistogram.Histogram) percentileRow {
	return percentileRow{
		name:  name,
		p50:   h.ValueAtQuantile(50),
		p95:   h.ValueAtQuantile(95),
		p99:   h.ValueAtQuantile(99),
		p999:  h.ValueAtQuantile(99.9),
		p9999: h.ValueAtQuantile(99.99),
		max:   h.Max(),
	}
}

func fmtDur(ns int64) string {
	return time.Duration(ns).String()
}

// PercentileTableText returns the percentile table as a formatted string.
func (r Result) PercentileTableText() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%-14s %12s %12s %12s %12s %12s %12s\n",
		"histogram", "p50", "p95", "p99", "p99.9", "p99.99", "max")
	for _, row := range r.rows() {
		fmt.Fprintf(&sb, "%-14s %12s %12s %12s %12s %12s %12s\n",
			row.name,
			fmtDur(row.p50), fmtDur(row.p95), fmtDur(row.p99),
			fmtDur(row.p999), fmtDur(row.p9999), fmtDur(row.max))
	}
	return sb.String()
}

type jsonRow struct {
	Histogram string `json:"histogram"`
	P50ns     int64  `json:"p50_ns"`
	P95ns     int64  `json:"p95_ns"`
	P99ns     int64  `json:"p99_ns"`
	P999ns    int64  `json:"p99_9_ns"`
	P9999ns   int64  `json:"p99_99_ns"`
	MaxNs     int64  `json:"max_ns"`
}

// MarshalJSON returns the result as a JSON array of percentile rows.
func (r Result) MarshalJSON() ([]byte, error) {
	rows := r.rows()
	out := make([]jsonRow, len(rows))
	for i, row := range rows {
		out[i] = jsonRow{
			Histogram: row.name,
			P50ns:     row.p50,
			P95ns:     row.p95,
			P99ns:     row.p99,
			P999ns:    row.p999,
			P9999ns:   row.p9999,
			MaxNs:     row.max,
		}
	}
	return json.Marshal(out)
}

// MarshalCSV returns the result as a CSV string.
func (r Result) MarshalCSV() (string, error) {
	var sb strings.Builder
	w := csv.NewWriter(&sb)
	_ = w.Write([]string{"histogram", "p50_ns", "p95_ns", "p99_ns", "p99_9_ns", "p99_99_ns", "max_ns"})
	for _, row := range r.rows() {
		_ = w.Write([]string{
			row.name,
			fmt.Sprintf("%d", row.p50),
			fmt.Sprintf("%d", row.p95),
			fmt.Sprintf("%d", row.p99),
			fmt.Sprintf("%d", row.p999),
			fmt.Sprintf("%d", row.p9999),
			fmt.Sprintf("%d", row.max),
		})
	}
	w.Flush()
	return sb.String(), w.Error()
}
