package io.github.yooouuuuuuu.dp.report;

import java.util.Arrays;
import java.util.List;

/** The result of the report role: counts, throughput, latencies and the correctness checks. */
record Report(
        String model,
        boolean kafkaTransactions,
        long generated,
        long accepted,
        long rejected,
        long completed,
        long inFlight,
        double throughputTps,
        Latency latencyMs,
        Latency validationLatencyMs,
        List<Check> checks) {

    boolean passed() {
        return checks.stream().allMatch(Check::passed);
    }

    /** One correctness check, under a stable name. */
    record Check(String name, boolean passed, String detail) {
    }

    /** Percentiles of a latency distribution, in ms. */
    record Latency(long p50, long p95, long p99, long max) {

        static Latency of(long[] values) {
            if (values.length == 0) {
                return new Latency(0, 0, 0, 0);
            }
            long[] sorted = values.clone();
            Arrays.sort(sorted);
            return new Latency(percentile(sorted, 50), percentile(sorted, 95), percentile(sorted, 99),
                    sorted[sorted.length - 1]);
        }

        /** The value at index ceil(percent/100 * n) - 1 of the sorted values, in integer arithmetic. */
        static long percentile(long[] sorted, int percent) {
            long rank = ((long) percent * sorted.length + 99) / 100;
            return sorted[(int) Math.max(rank, 1) - 1];
        }
    }
}
