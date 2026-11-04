package io.github.yooouuuuuuu.dp.validator;

import io.github.yooouuuuuuu.dp.avro.LocalBalance;
import io.github.yooouuuuuuu.dp.avro.ValidatorOffset;
import io.github.yooouuuuuuu.dp.cli.Failure;
import io.github.yooouuuuuuu.dp.config.Config;
import io.github.yooouuuuuuu.dp.kafka.AvroSerde;
import io.github.yooouuuuuuu.dp.kafka.BankDirectory;
import io.github.yooouuuuuuu.dp.kafka.Ids;
import io.github.yooouuuuuuu.dp.kafka.KafkaClients;
import io.github.yooouuuuuuu.dp.kafka.PartitionReader;
import io.github.yooouuuuuuu.dp.kafka.Topics;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Takes over a bank when the consumer group assigns its partition to this validator: fence the previous
 * owner, then rebuild the bank's balances and read positions from Kafka (docs/consistency, recovery).
 */
final class BankTakeover {
    private final Config config;
    private final BankDirectory directory;

    BankTakeover(Config config, BankDirectory directory) {
        this.config = config;
        this.directory = directory;
    }

    /** A bank taken over: the bank, its balances, and where to continue reading its blocks. */
    record TakenOver(OwnedBank bank, Map<String, Long> balances, long blocksOffset) {
    }

    TakenOver takeOver(int partition) {
        BankDirectory.Bank bank = directory.bank(partition);
        // Fence first, read second: once the writer exists, no transaction of the previous owner is still
        // open, so the read_committed reads below see the bank's final state, matching the positions.
        BankWriter writer = BankWriter.open(config, clientId(bank.id()), bank.id());
        try {
            // The bank's accounts are the ones accountInfo lists, not whatever keys localBalance holds.
            Set<String> accounts = Set.copyOf(bank.accounts());
            Map<String, Long> balances = readBalances(partition, accounts);
            if (balances.size() < accounts.size()) {
                throw new Failure("localBalance has no balance for some accounts of " + bank.id()
                        + ": run init --reset");
            }
            Map<String, Long> positions = readPositions(partition);
            OwnedBank owned = new OwnedBank(partition, bank.id(), writer, accounts,
                    positions.getOrDefault(Topics.CREDITS, 0L));
            return new TakenOver(owned, balances, positions.getOrDefault(Topics.BLOCKS, 0L));
        } catch (RuntimeException e) {
            writer.close();
            throw e;
        }
    }

    /** The latest balance of each of the bank's accounts: the last record per account wins. */
    private Map<String, Long> readBalances(int partition, Set<String> accounts) {
        Map<String, Long> balances = new HashMap<>();
        try (KafkaConsumer<String, LocalBalance> consumer =
                     KafkaClients.consumer(config, clientId("localBalance"), AvroSerde.localBalances(), 500)) {
            PartitionReader.readToEnd(config, consumer, List.of(new TopicPartition(Topics.LOCAL_BALANCE, partition)),
                    record -> {
                        if (accounts.contains(record.key())) {
                            balances.put(record.key(), record.value().getBalance());
                        }
                    });
        }
        return balances;
    }

    /**
     * How far the bank's blocks and credits were processed: the latest record per key, "blocks" or
     * "credits"; missing means 0.
     */
    private Map<String, Long> readPositions(int partition) {
        Map<String, Long> positions = new HashMap<>();
        try (KafkaConsumer<String, ValidatorOffset> consumer =
                     KafkaClients.consumer(config, clientId("validatorOffsets"), AvroSerde.validatorOffsets(), 500)) {
            PartitionReader.readToEnd(config, consumer,
                    List.of(new TopicPartition(Topics.VALIDATOR_OFFSETS, partition)),
                    record -> positions.put(record.key(), positionOf(record, partition)));
        }
        return positions;
    }

    /** The key names the topic of the position, and the value must say the same, for the bank's partition. */
    static long positionOf(ConsumerRecord<String, ValidatorOffset> record, int partition) {
        ValidatorOffset position = record.value();
        boolean knownTopic = Topics.BLOCKS.equals(record.key()) || Topics.CREDITS.equals(record.key());
        if (!knownTopic || !position.getTopic().equals(record.key()) || position.getPartition() != partition) {
            throw new IllegalStateException("validatorOffsets partition " + partition + " has a record with key "
                    + record.key() + " and value " + position + ": run init --reset");
        }
        return position.getOffset();
    }

    private String clientId(String suffix) {
        return Ids.client("validator", config.siteId(), suffix);
    }
}
