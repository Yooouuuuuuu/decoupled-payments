package io.github.yooouuuuuuu.dp.config;

import io.github.yooouuuuuuu.dp.avro.Model;
import java.util.Map;

/**
 * The validated settings of one process. The record mirrors the sections of default.yaml, where every
 * setting is documented.
 */
public record Config(
        Model model,
        boolean kafkaTransactions,
        int siteId,
        LogLevel logLevel,
        long statsIntervalMs,
        Kafka kafka,
        Topics topics,
        Bank bank,
        Init init,
        Generator generator,
        Aggregator aggregator,
        Validator validator,
        Report report) {

    public record Kafka(String bootstrapServers, int sessionTimeoutMs, int transactionTimeoutMs,
            int maxPartitionFetchBytes) {
    }

    public record Topics(short replicationFactor, int tradesPartitions) {
    }

    public record Bank(int count, int accountsPerBank, long initialBalance) {
    }

    public record Init(boolean reset) {
    }

    public record Generator(long rps, long trades, Distribution distribution, double zipfExponent, long minAmount,
            long maxAmount, long seed) {
    }

    public record Aggregator(int blockSize, long blockTimeoutMs, int maxPollRecords) {
    }

    public record Validator(int blocksPerTransaction, int creditsPerTransaction) {
    }

    public record Report(long timeoutMs, boolean json) {
    }

    /**
     * The largest duration in ms that still converts to nanoseconds in 64 bits, so that no timer or deadline
     * computed from a setting can overflow.
     */
    static final long MAX_MILLIS = 9_223_372_036_854L;
    /** The sequence part of a trade's serial number has 32 bits (serial number = runId * 2^32 + sequence). */
    public static final long MAX_TRADES = 4_294_967_295L;
    /**
     * init writes each bank's account list as one accountInfo record, which must stay under Kafka's 1 MiB
     * message limit. With at most this many banks (IDs up to bank9999) and accounts per bank, it stays
     * under 800 KB.
     */
    static final long MAX_BANKS = 10_000;
    static final long MAX_ACCOUNTS_PER_BANK = 50_000;
    /** Above this exponent nearly every Zipf draw is rank 1, and receivers would be re-drawn almost forever. */
    static final double MAX_ZIPF_EXPONENT = 5;

    /**
     * Validates the merged settings. Every violation is reported, each naming the key and the source of its
     * value; the Go implementation applies exactly the same rules.
     */
    static Config from(Map<String, Setting> settings) throws ConfigException {
        SettingsReader s = new SettingsReader(settings);
        Config config = new Config(
                s.choice("model", Model.class),
                s.bool("kafkaTransactions"),
                s.intValue("siteId", 0),
                s.choice("logLevel", LogLevel.class),
                s.integer("statsIntervalMs", 1, MAX_MILLIS),
                new Kafka(
                        s.brokers("kafka.bootstrapServers"),
                        s.intValue("kafka.sessionTimeoutMs", 1),
                        s.intValue("kafka.transactionTimeoutMs", 1),
                        s.intValue("kafka.maxPartitionFetchBytes", 1)),
                new Topics(
                        (short) s.integer("topics.replicationFactor", 1, Short.MAX_VALUE),
                        s.intValue("topics.tradesPartitions", 1)),
                new Bank(
                        (int) s.integer("bank.count", 1, MAX_BANKS),
                        (int) s.integer("bank.accountsPerBank", 1, MAX_ACCOUNTS_PER_BANK),
                        s.integer("bank.initialBalance", 0, Long.MAX_VALUE)),
                new Init(s.bool("init.reset")),
                new Generator(
                        s.integer("generator.rps", 1, Long.MAX_VALUE),
                        s.integer("generator.trades", 0, MAX_TRADES),
                        s.choice("generator.distribution", Distribution.class),
                        s.number("generator.zipfExponent", 0, MAX_ZIPF_EXPONENT),
                        s.integer("generator.minAmount", 1, Long.MAX_VALUE),
                        s.integer("generator.maxAmount", 1, Long.MAX_VALUE),
                        s.integer("generator.seed", Long.MIN_VALUE, Long.MAX_VALUE)),
                new Aggregator(
                        s.intValue("aggregator.blockSize", 1),
                        s.integer("aggregator.blockTimeoutMs", 0, MAX_MILLIS),
                        s.intValue("aggregator.maxPollRecords", 1)),
                new Validator(
                        s.intValue("validator.blocksPerTransaction", 1),
                        s.intValue("validator.creditsPerTransaction", 1)),
                new Report(
                        s.integer("report.timeoutMs", 0, MAX_MILLIS),
                        s.bool("report.json")));
        s.requireAtLeast("generator.maxAmount", "generator.minAmount");
        // Money only moves between accounts, so if the total fits, no balance and no sum of balances overflows.
        s.requireProductAtMost(Long.MAX_VALUE, "the total money must fit in 64 bits",
                "bank.count", "bank.accountsPerBank", "bank.initialBalance");
        s.throwIfInvalid();
        return config;
    }
}
