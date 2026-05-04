package main

import (
	"fmt"
	"time"
)

// formatNs formats a nanosecond value as a human-readable duration string,
// used by the matrix report for aligned column output.
func formatNs(ns int64) string {
	return time.Duration(ns).String()
}

// printComparisonTable prints a side-by-side comparison of two result sets.
// Intended for future use comparing branches via captured output.
func printComparisonTable(headers []string, rows [][]string) {
	if len(rows) == 0 {
		return
	}
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}
	fmtRow := func(cells []string) {
		for i, cell := range cells {
			if i < len(widths) {
				fmt.Printf("%-*s  ", widths[i], cell)
			}
		}
		fmt.Println()
	}
	fmtRow(headers)
	sep := make([]string, len(headers))
	for i, w := range widths {
		s := ""
		for range w {
			s += "-"
		}
		sep[i] = s
	}
	fmtRow(sep)
	for _, row := range rows {
		fmtRow(row)
	}
}

// durationCell formats a Duration pointer for table output ("n/a" when nil).
func durationCell(d *time.Duration) string {
	if d == nil {
		return "n/a"
	}
	return d.String()
}

// _ suppresses unused-import warnings for packages referenced only in future
// extensions of this file.
var _ = formatNs
var _ = printComparisonTable
var _ = durationCell
