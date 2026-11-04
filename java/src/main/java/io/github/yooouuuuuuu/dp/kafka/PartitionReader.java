package io.github.yooouuuuuuu.dp.kafka;

import io.github.yooouuuuuuu.dp.cli.Failure;
import io.github.yooouuuuuuu.dp.config.Config;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Reads partitions up to their end with a read_committed consumer, e.g. to rebuild state from a compacted
 * topic. The end is taken when the read starts, and is one of two offsets:
 * <ul>
 *   <li>the last stable offset ({@code endOffsets} of a read_committed consumer): every record before it
 *       belongs to a committed or an aborted Kafka transaction, none to an open one;</li>
 *   <li>the high watermark, the end of the log: it also covers the records of the transactions open at that
 *       moment, so the read waits until each of them has committed or aborted.</li>
 * </ul>
 * A read gives up (exit status 1) once the consumer's positions haven't advanced for
 * kafka.transactionTimeoutMs + 60 s: by then the broker has aborted any transaction that was open, so the
 * read is stuck for another reason, such as a partition without a leader.
 */
public final class PartitionReader {
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    private static final Duration NO_PROGRESS_MARGIN = Duration.ofSeconds(60);

    private PartitionReader() {
    }

    /** Assigns the partitions to the consumer and reads them from the beginning to their last stable offset. */
    public static <V> void readToEnd(Config config, KafkaConsumer<String, V> consumer,
            Collection<TopicPartition> partitions, Consumer<ConsumerRecord<String, V>> handler) {
        startAtTheBeginning(consumer, partitions);
        readUntil(config, consumer, consumer.endOffsets(partitions), handler);
    }

    /**
     * Assigns the partitions to the consumer and reads them from the beginning to their high watermark.
     *
     * @param clientId the consumer's client ID, also used to look up the high watermarks
     */
    public static <V> void readToHighWatermark(Config config, String clientId, KafkaConsumer<String, V> consumer,
            Collection<TopicPartition> partitions, Consumer<ConsumerRecord<String, V>> handler) {
        startAtTheBeginning(consumer, partitions);
        readOnToHighWatermark(config, clientId, consumer, handler);
    }

    /** Reads the consumer's partitions on from where it is, up to their high watermark. */
    public static <V> void readOnToHighWatermark(Config config, String clientId, KafkaConsumer<String, V> consumer,
            Consumer<ConsumerRecord<String, V>> handler) {
        readUntil(config, consumer, KafkaClients.highWatermarks(config, clientId, consumer.assignment()), handler);
    }

    private static void startAtTheBeginning(KafkaConsumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
    }

    /**
     * Compares the consumer's positions with the end, not the offsets of the records it returned: transaction
     * markers and aborted records occupy offsets but are never returned, so the record just before the end
     * may never be seen.
     */
    private static <V> void readUntil(Config config, KafkaConsumer<String, V> consumer, Map<TopicPartition, Long> end,
            Consumer<ConsumerRecord<String, V>> handler) {
        Duration noProgressLimit = Duration.ofMillis(config.kafka().transactionTimeoutMs()).plus(NO_PROGRESS_MARGIN);
        Map<TopicPartition, Long> positions = positions(consumer, end.keySet());
        long lastProgress = System.nanoTime();
        while (!notReached(positions, end).isEmpty()) {
            consumer.poll(POLL_TIMEOUT).forEach(handler);
            Map<TopicPartition, Long> now = positions(consumer, end.keySet());
            if (!now.equals(positions)) {
                positions = now;
                lastProgress = System.nanoTime();
            } else if (System.nanoTime() - lastProgress > noProgressLimit.toNanos()) {
                throw new Failure("gave up reading " + notReached(positions, end) + ": no progress for "
                        + noProgressLimit.toSeconds() + " s (is a broker down?)");
            }
        }
    }

    private static Map<TopicPartition, Long> positions(KafkaConsumer<?, ?> consumer,
            Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> positions = new HashMap<>();
        partitions.forEach(partition -> positions.put(partition, consumer.position(partition)));
        return positions;
    }

    private static List<TopicPartition> notReached(Map<TopicPartition, Long> positions, Map<TopicPartition, Long> end) {
        return end.keySet().stream().filter(partition -> positions.get(partition) < end.get(partition)).toList();
    }
}
