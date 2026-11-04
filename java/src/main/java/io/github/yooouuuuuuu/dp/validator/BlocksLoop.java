package io.github.yooouuuuuuu.dp.validator;

import io.github.yooouuuuuuu.dp.avro.Block;
import io.github.yooouuuuuuu.dp.avro.Model;
import io.github.yooouuuuuuu.dp.cli.Shutdown;
import io.github.yooouuuuuuu.dp.kafka.Ids;
import io.github.yooouuuuuuu.dp.kafka.Topics;
import io.github.yooouuuuuuu.dp.validator.BankBatch.ProcessedBlock;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread B: validates the blocks of the banks this validator owns (the validation phase; in the Sequential
 * model also the credit phase). Its consumer is this validator's member of the consumer group
 * {@code validators}, so the group decides which banks the validator owns, and the rebalance callbacks take
 * banks over and hand them back.
 *
 * <p>The validator never commits group offsets. Its read position for a bank is a validatorOffsets record,
 * written in the same transaction as the results, so the position and the results can't disagree.
 */
final class BlocksLoop implements ConsumerRebalanceListener {
    private static final Logger log = LoggerFactory.getLogger(BlocksLoop.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    private final KafkaConsumer<String, Block> consumer;
    private final Ledger ledger;
    private final BankTakeover takeover;
    private final Model model;
    private final ValidatorStats stats;

    BlocksLoop(KafkaConsumer<String, Block> consumer, Ledger ledger, BankTakeover takeover, Model model,
            ValidatorStats stats) {
        this.consumer = consumer;
        this.ledger = ledger;
        this.takeover = takeover;
        this.model = model;
        this.stats = stats;
    }

    /** Validates until a stop is requested (by a signal, or by the credits thread after it failed). */
    void run(Shutdown shutdown) {
        consumer.subscribe(List.of(Topics.BLOCKS), this);
        try {
            while (!shutdown.requested()) {
                // The rebalance callbacks below run inside poll, never while a batch is being processed.
                ConsumerRecords<String, Block> records = consumer.poll(POLL_TIMEOUT);
                for (TopicPartition partition : records.partitions()) {
                    validate(partition.partition(), records.records(partition));
                }
            }
        } catch (WakeupException e) {
            // A stop was requested.
        }
    }

    /** One Kafka transaction: validate the polled blocks of one bank and write all the results atomically. */
    private void validate(int partition, List<ConsumerRecord<String, Block>> records) {
        ledger.lock();
        try {
            BankBatch batch = BankTransaction.run(ledger, partition, model, Topics.BLOCKS, (bank, validation) -> {
                for (ConsumerRecord<String, Block> record : records) {
                    ProcessedBlock processed = validation.validate(record.value(), System.currentTimeMillis());
                    bank.writer().send(ValidatorRecords.order(bank, processed.order()));
                    processed.credits().forEach((receiver, credits) ->
                            bank.writer().send(ValidatorRecords.credits(model, receiver, credits)));
                }
                return records.getLast().offset() + 1;
            });
            stats.committed(batch);
        } finally {
            ledger.unlock();
        }
    }

    /** The group gave us these banks: take each over and continue where its last owner committed. */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return;
        }
        ledger.lock();
        try {
            for (TopicPartition partition : partitions) {
                BankTakeover.TakenOver taken = takeover.takeOver(partition.partition());
                ledger.add(taken.bank(), taken.balances());
                consumer.seek(partition, taken.blocksOffset());
            }
            log.info("now owns {}", ledger.ownedBankIds());
        } finally {
            ledger.unlock();
        }
    }

    /** The group takes these banks away (the range assignor revokes everything before reassigning). */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        handBack(partitions);
    }

    /** We were dropped from the group (e.g. a missed session): another validator may own these banks already. */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        handBack(partitions);
    }

    private void handBack(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return;
        }
        ledger.lock();
        try {
            partitions.forEach(partition -> ledger.remove(partition.partition()));
            log.info("handed back {}, now owns {}",
                    partitions.stream().map(partition -> Ids.bank(partition.partition())).sorted().toList(),
                    ledger.ownedBankIds());
        } finally {
            ledger.unlock();
        }
    }
}
