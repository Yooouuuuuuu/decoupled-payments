package report

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// writeSummary prints the result for people.
func writeSummary(w io.Writer, r Result) {
	tx := "on"
	if !r.KafkaTransactions {
		tx = "off"
	}
	fmt.Fprintf(w, "Report: %s model, Kafka transactions %s\n", r.Model, tx)
	fmt.Fprintf(w, "  generated   %8d\n", r.Generated)
	fmt.Fprintf(w, "  accepted    %8d\n", r.Accepted)
	fmt.Fprintf(w, "  rejected    %8d\n", r.Rejected)
	fmt.Fprintf(w, "  completed   %8d\n", r.Completed)
	fmt.Fprintf(w, "  in flight   %8d\n", r.InFlight)
	fmt.Fprintf(w, "  throughput  %10.1f trades/s\n", r.ThroughputTPS)
	fmt.Fprintf(w, "  latency (ms)   p50 %6d  p95 %6d  p99 %6d  max %6d  (completion)\n", r.Latency.P50, r.Latency.P95, r.Latency.P99, r.Latency.Max)
	v := r.ValidationLatency
	fmt.Fprintf(w, "                 p50 %6d  p95 %6d  p99 %6d  max %6d  (validation)\n", v.P50, v.P95, v.P99, v.Max)
	fmt.Fprintln(w, "Checks")
	for _, c := range r.Checks {
		mark := "✓"
		if !c.Passed {
			mark = "✗"
		}
		fmt.Fprintf(w, "  %s %-22s %s\n", mark, c.Name, c.Detail)
	}
	fmt.Fprintln(w, verdict(r))
}

func verdict(r Result) string {
	if r.Passed() {
		return "PASS"
	}
	return "FAIL"
}

// The REPORT_JSON line, for scripts. The Java implementation prints the
// same keys.
type jsonReport struct {
	Result              string          `json:"result"`
	Model               string          `json:"model"`
	KafkaTransactions   bool            `json:"kafkaTransactions"`
	Generated           int             `json:"generated"`
	Accepted            int             `json:"accepted"`
	Rejected            int             `json:"rejected"`
	Completed           int             `json:"completed"`
	InFlight            int             `json:"inFlight"`
	ThroughputTps       float64         `json:"throughputTps"`
	LatencyMs           jsonPercentiles `json:"latencyMs"`
	ValidationLatencyMs jsonPercentiles `json:"validationLatencyMs"`
	Checks              []jsonCheck     `json:"checks"`
}

type jsonPercentiles struct {
	P50 int64 `json:"p50"`
	P95 int64 `json:"p95"`
	P99 int64 `json:"p99"`
	Max int64 `json:"max"`
}

type jsonCheck struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail"`
}

// writeJSON prints the result as one line: REPORT_JSON {...}.
func writeJSON(w io.Writer, r Result) error {
	out := jsonReport{
		Result: verdict(r), Model: string(r.Model), KafkaTransactions: r.KafkaTransactions,
		Generated: r.Generated, Accepted: r.Accepted, Rejected: r.Rejected, Completed: r.Completed, InFlight: r.InFlight,
		ThroughputTps:       r.ThroughputTPS,
		LatencyMs:           jsonPercentiles(r.Latency),
		ValidationLatencyMs: jsonPercentiles(r.ValidationLatency),
	}
	for _, c := range r.Checks {
		out.Checks = append(out.Checks, jsonCheck(c))
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(out); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, "REPORT_JSON %s", buf.Bytes()) // Encode ends the line
	return err
}
