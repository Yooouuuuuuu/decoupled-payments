package io.github.yooouuuuuuu.dp.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** The validation table, shared with the Go implementation: every boundary, just inside and just outside. */
class ConfigValidationTest {

    private static Config load(String... flags) throws ConfigException {
        List<Flag> parsed = Arrays.stream(flags).map(flag -> {
            int equals = flag.indexOf('=');
            return new Flag(flag.substring(0, equals), flag.substring(equals + 1));
        }).toList();
        return ConfigLoader.load("generator", Optional.empty(), parsed, Map.of());
    }

    private static void assertValid(String... flags) {
        assertDoesNotThrow(() -> load(flags), String.join(" ", flags));
    }

    /** The settings break exactly one rule, of {@code key}, and the problem names the key and the flag. */
    private static void assertInvalid(String key, String... flags) {
        ConfigException e = assertThrows(ConfigException.class, () -> load(flags), String.join(" ", flags));
        assertEquals(1, e.problems().size(), e.problems().toString());
        String problem = e.problems().getFirst();
        assertTrue(problem.startsWith(key + " "), problem);
        assertTrue(problem.contains("flag --"), problem);
    }

    /** {@code min} and {@code max} are valid, the values just outside them are not. */
    private static void assertRange(String key, long min, long max) {
        assertValid(key + "=" + min);
        assertValid(key + "=" + max);
        assertInvalid(key, key + "=" + (min - 1));
        assertInvalid(key, key + "=" + (max + 1));
    }

    @Test
    void integerRanges() {
        long maxMillis = 9_223_372_036_854L;
        assertRange("siteId", 0, Integer.MAX_VALUE);
        assertRange("statsIntervalMs", 1, maxMillis);
        for (String key : List.of("kafka.sessionTimeoutMs", "kafka.transactionTimeoutMs",
                "kafka.maxPartitionFetchBytes", "topics.tradesPartitions", "aggregator.blockSize", "aggregator.maxPollRecords", "validator.blocksPerTransaction",
                "validator.creditsPerTransaction")) {
            assertRange(key, 1, Integer.MAX_VALUE);
        }
        assertRange("topics.replicationFactor", 1, Short.MAX_VALUE);
        assertRange("bank.count", 1, 10_000);
        assertRange("bank.accountsPerBank", 1, 50_000);
        assertRange("generator.trades", 0, 4_294_967_295L);
        assertRange("aggregator.blockTimeoutMs", 0, maxMillis);
        assertRange("report.timeoutMs", 0, maxMillis);
    }

    @Test
    void noUpperBoundBelowTheLargestInteger() {
        assertValid("generator.rps=" + Long.MAX_VALUE);
        assertInvalid("generator.rps", "generator.rps=0");
        assertValid("generator.minAmount=1", "generator.maxAmount=" + Long.MAX_VALUE);
        assertInvalid("generator.minAmount", "generator.minAmount=0");
        assertValid("generator.seed=" + Long.MIN_VALUE);
        assertValid("bank.initialBalance=0");
        assertInvalid("bank.initialBalance", "bank.initialBalance=-1");
    }

    @Test
    void maxAmountIsAtLeastMinAmount() {
        assertValid("generator.minAmount=50", "generator.maxAmount=50");
        assertInvalid("generator.maxAmount", "generator.minAmount=50", "generator.maxAmount=49");
    }

    @Test
    void theTotalMoneyFitsIn64Bits() {
        // 3 banks of 100 accounts by default: 300 accounts.
        long largest = Long.MAX_VALUE / 300;
        assertValid("bank.initialBalance=" + largest);
        ConfigException e = assertThrows(ConfigException.class, () -> load("bank.initialBalance=" + (largest + 1)));
        String problem = e.problems().getFirst();
        assertTrue(problem.startsWith("bank.count * bank.accountsPerBank * bank.initialBalance must be at most "
                + Long.MAX_VALUE), problem);
        assertTrue(problem.contains("bank.initialBalance: flag --bank.initialBalance"), problem);
        // The most accounts there can be: 10,000 banks of 50,000 accounts.
        long mostAccounts = 10_000L * 50_000;
        assertValid("bank.count=10000", "bank.accountsPerBank=50000",
                "bank.initialBalance=" + Long.MAX_VALUE / mostAccounts);
        assertValid("bank.count=10000", "bank.accountsPerBank=50000", "bank.initialBalance=0");
        assertInvalid("bank.count * bank.accountsPerBank * bank.initialBalance", "bank.count=10000",
                "bank.accountsPerBank=50000", "bank.initialBalance=" + (Long.MAX_VALUE / mostAccounts + 1));
    }

    @Test
    void zipfExponentIsFromZeroToFive() {
        assertValid("generator.zipfExponent=0");
        assertValid("generator.zipfExponent=5");
        assertValid("generator.zipfExponent=5.0");
        assertInvalid("generator.zipfExponent", "generator.zipfExponent=5.000001");
        assertInvalid("generator.zipfExponent", "generator.zipfExponent=-0.5");
    }

    @Test
    void bootstrapServersNameAtLeastOneBrokerAndNoEmptyEntry() {
        assertValid("kafka.bootstrapServers=kafka-1:19092");
        assertValid("kafka.bootstrapServers=kafka-1:19092, kafka-2:19092");
        for (String bad : List.of("", " ", ",", " , ", "kafka-1:19092,", ",kafka-1:19092", "a:1,,b:2", "a:1, ,b:2")) {
            assertInvalid("kafka.bootstrapServers", "kafka.bootstrapServers=" + bad);
        }
    }

    @Test
    void choices() {
        assertValid("model=sequential", "logLevel=error", "generator.distribution=zipf");
        assertInvalid("model", "model=Sequential");
        assertInvalid("logLevel", "logLevel=trace");
        assertInvalid("generator.distribution", "generator.distribution=normal");
    }

    @Test
    void everyViolationIsReportedWithItsKeyAndSource() {
        ConfigException e = assertThrows(ConfigException.class, () -> ConfigLoader.load("aggregator",
                Optional.empty(), List.of(new Flag("blockSize", "0"), new Flag("model", "parallel")),
                Map.of("DP_REPORT_TIMEOUT_MS", "9223372036855")));
        assertEquals(List.of(
                "model must be one of sequential, concurrent, but is 'parallel' (flag --model)",
                "aggregator.blockSize must be at least 1, but is 0 (flag --blockSize)",
                "report.timeoutMs must be at most 9223372036854, but is 9223372036855 "
                        + "(environment variable DP_REPORT_TIMEOUT_MS)"), e.problems());
    }
}
