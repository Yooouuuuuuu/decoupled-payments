package io.github.yooouuuuuuu.dp.report;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.yooouuuuuuu.dp.Locales;
import io.github.yooouuuuuuu.dp.report.Report.Check;
import io.github.yooouuuuuuu.dp.report.Report.Latency;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

class ReportFormatTest {

    @Test
    void percentileIsTheValueAtIndexCeilQTimesNMinusOne() {
        long[] oneToHundred = LongStream.rangeClosed(1, 100).toArray();
        assertEquals(new Latency(50, 95, 99, 100), Latency.of(oneToHundred));

        long[] oneToTen = LongStream.rangeClosed(1, 10).toArray();
        assertEquals(new Latency(5, 10, 10, 10), Latency.of(oneToTen), "ceil(9.5) = 10, ceil(9.9) = 10");

        assertEquals(new Latency(7, 7, 7, 7), Latency.of(new long[] {7}));
        assertEquals(new Latency(0, 0, 0, 0), Latency.of(new long[0]));
        assertEquals(new Latency(2, 3, 3, 3), Latency.of(new long[] {3, 1, 2}), "unsorted input");
    }

    @Test
    void percentileMatchesTheDefinitionForEverySize() {
        for (int n = 1; n <= 5_000; n++) {
            long[] values = LongStream.rangeClosed(1, n).toArray();
            for (int percent : new int[] {50, 95, 99}) {
                long expected = values[(int) Math.ceil(percent / 100.0 * n) - 1];
                assertEquals(expected, Latency.percentile(values, percent), "n=" + n + " p" + percent);
            }
        }
    }

    @Test
    void jsonWriterEscapesStrings() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("text", "a \"quoted\" \\ line\nbreak\u0001");
        value.put("list", List.of(1L, 2.5, true));
        assertEquals("{\"text\":\"a \\\"quoted\\\" \\\\ line\\nbreak\\u0001\",\"list\":[1,2.5,true]}",
                Json.write(value));
        assertThrows(IllegalArgumentException.class, () -> Json.write(Double.NaN));
    }

    private static final Report SAMPLE = new Report("sequential", false, 10, 8, 1, 7, 2, 123.4,
            new Latency(1, 2, 3, 4), new Latency(5, 6, 7, 8),
            List.of(new Check("complete", false, "1 of 8 accepted trades not completed")));
    private static final String SAMPLE_JSON = "REPORT_JSON {\"result\":\"FAIL\",\"model\":\"sequential\","
            + "\"kafkaTransactions\":false,\"generated\":10,\"accepted\":8,\"rejected\":1,\"completed\":7,"
            + "\"inFlight\":2,\"throughputTps\":123.4,"
            + "\"latencyMs\":{\"p50\":1,\"p95\":2,\"p99\":3,\"max\":4},"
            + "\"validationLatencyMs\":{\"p50\":5,\"p95\":6,\"p99\":7,\"max\":8},"
            + "\"checks\":[{\"name\":\"complete\",\"passed\":false,"
            + "\"detail\":\"1 of 8 accepted trades not completed\"}]}";

    @Test
    void jsonLineHasTheAgreedKeys() {
        assertEquals(SAMPLE_JSON, ReportPrinter.jsonLine(SAMPLE));
    }

    @Test
    void numbersDontDependOnTheDefaultLocale() {
        Locales.withEachNonAsciiDefault(() -> {
            assertEquals(SAMPLE_JSON, ReportPrinter.jsonLine(SAMPLE));
            String summary = ReportPrinter.summary(SAMPLE);
            assertTrue(summary.contains("10 generated, 8 accepted, 1 rejected, 7 completed, 2 in flight"), summary);
            assertTrue(summary.contains("throughput  123.4 trades/s"), summary);
            assertTrue(summary.contains("p50 1 ms, p95 2 ms, p99 3 ms, max 4 ms"), summary);
            assertEquals("{\"text\":\"\\u0001\"}", Json.write(Map.of("text", "\u0001")));
        });
    }

    @Test
    void summaryShowsEachCheckAndTheResult() {
        Report report = new Report("concurrent", true, 1, 1, 0, 1, 0, 2.0, new Latency(1, 1, 1, 1),
                new Latency(1, 1, 1, 1), List.of(new Check("complete", true, "all fine")));
        String summary = ReportPrinter.summary(report);
        assertTrue(summary.contains("✓ complete"), summary);
        assertTrue(summary.strip().endsWith("PASS"), summary);
    }
}
