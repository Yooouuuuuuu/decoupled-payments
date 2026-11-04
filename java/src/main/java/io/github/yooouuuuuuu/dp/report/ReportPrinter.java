package io.github.yooouuuuuuu.dp.report;

import io.github.yooouuuuuuu.dp.report.Report.Check;
import io.github.yooouuuuuuu.dp.report.Report.Latency;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Formats a report: a readable summary, and the one-line {@code REPORT_JSON} form for scripts. */
final class ReportPrinter {
    private ReportPrinter() {
    }

    static String summary(Report report) {
        StringBuilder text = new StringBuilder();
        line(text, "Report (model=%s, kafkaTransactions=%s)", report.model(), report.kafkaTransactions());
        line(text, "  trades      %d generated, %d accepted, %d rejected, %d completed, %d in flight",
                report.generated(), report.accepted(), report.rejected(), report.completed(), report.inFlight());
        line(text, "  throughput  %.1f trades/s", report.throughputTps());
        line(text, "  latency     completion %s", latency(report.latencyMs()));
        line(text, "              validation %s", latency(report.validationLatencyMs()));
        line(text, "  checks");
        for (Check check : report.checks()) {
            line(text, "    %s %-22s %s", check.passed() ? "✓" : "✗", check.name(), check.detail());
        }
        line(text, report.passed() ? "PASS" : "FAIL");
        return text.toString();
    }

    /** One line, {@code REPORT_JSON {...}}, with the same keys in the Java and the Go implementation. */
    static String jsonLine(Report report) {
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("result", report.passed() ? "PASS" : "FAIL");
        json.put("model", report.model());
        json.put("kafkaTransactions", report.kafkaTransactions());
        json.put("generated", report.generated());
        json.put("accepted", report.accepted());
        json.put("rejected", report.rejected());
        json.put("completed", report.completed());
        json.put("inFlight", report.inFlight());
        json.put("throughputTps", report.throughputTps());
        json.put("latencyMs", latencyJson(report.latencyMs()));
        json.put("validationLatencyMs", latencyJson(report.validationLatencyMs()));
        json.put("checks", report.checks().stream().map(ReportPrinter::checkJson).toList());
        return "REPORT_JSON " + Json.write(json);
    }

    private static void line(StringBuilder text, String format, Object... args) {
        text.append(String.format(Locale.ROOT, format, args)).append(System.lineSeparator());
    }

    private static String latency(Latency latency) {
        return String.format(Locale.ROOT, "p50 %d ms, p95 %d ms, p99 %d ms, max %d ms",
                latency.p50(), latency.p95(), latency.p99(), latency.max());
    }

    private static Map<String, Object> latencyJson(Latency latency) {
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("p50", latency.p50());
        json.put("p95", latency.p95());
        json.put("p99", latency.p99());
        json.put("max", latency.max());
        return json;
    }

    private static Map<String, Object> checkJson(Check check) {
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("name", check.name());
        json.put("passed", check.passed());
        json.put("detail", check.detail());
        return json;
    }
}
