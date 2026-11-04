package io.github.yooouuuuuuu.dp.aggregator;

import io.github.yooouuuuuuu.dp.avro.Block;
import io.github.yooouuuuuuu.dp.cli.Shutdown;
import io.github.yooouuuuuuu.dp.cli.StatsTimer;
import io.github.yooouuuuuuu.dp.config.Config;
import io.github.yooouuuuuuu.dp.kafka.AvroSerde;
import io.github.yooouuuuuuu.dp.kafka.BankDirectory;
import io.github.yooouuuuuuu.dp.kafka.Groups;
import io.github.yooouuuuuuu.dp.kafka.Ids;
import io.github.yooouuuuuuu.dp.kafka.KafkaClients;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * The aggregator role (layer 1). Aggregators share the trades partitions through the consumer group
 * {@code aggregators}; any aggregator may take any trade and builds blocks for every bank.
 */
public final class AggregatorRole {
    private static final String ROLE = "aggregator";

    private final Config config;
    private final Shutdown shutdown;

    public AggregatorRole(Config config, Shutdown shutdown) {
        this.config = config;
        this.shutdown = shutdown;
    }

    public void run() {
        BankDirectory.read(config, clientId("accountInfo")).requireModelOf(config);
        try (KafkaProducer<String, SpecificRecord> producer = createProducer();
             KafkaConsumer<String, Block> consumer = KafkaClients.groupConsumer(config, Groups.AGGREGATORS,
                     ROLE + "-" + config.siteId(), clientId("trades"), AvroSerde.blocks(),
                     config.aggregator().maxPollRecords())) {
            if (config.kafkaTransactions()) {
                // Fences an earlier process with this siteId (same transactional.id) and aborts its unfinished
                // transaction, so its blocks and offsets disappear before we read the trades again.
                producer.initTransactions();
            }
            PendingBlocks pending =
                    new PendingBlocks(config.aggregator().blockSize(), config.aggregator().blockTimeoutMs());
            Aggregator aggregator = new Aggregator(consumer, producer, config.kafkaTransactions(), pending);
            shutdown.stopCleanly(consumer::wakeup);
            StatsTimer stats = StatsTimer.start(config.statsIntervalMs(), aggregator::logStats);
            try {
                aggregator.run(shutdown);
            } finally {
                stats.close();
            }
            // Only reached after a stop request. A failure throws past this, and the plain close of the
            // try-with-resources keeps the membership, so a restart with the same siteId takes its place.
            KafkaClients.leaveGroupAndClose(consumer);
        }
    }

    private KafkaProducer<String, SpecificRecord> createProducer() {
        String clientId = clientId("blocks");
        return config.kafkaTransactions()
                ? KafkaClients.transactionalProducer(config, clientId, ROLE + "-" + config.siteId())
                : KafkaClients.producer(config, clientId);
    }

    private String clientId(String suffix) {
        return Ids.client(ROLE, config.siteId(), suffix);
    }
}
