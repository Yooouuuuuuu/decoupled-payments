package io.github.yooouuuuuuu.dp.kafka;

import io.github.yooouuuuuuu.dp.config.Config;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Creates the Kafka clients of every role with the settings the design relies on. The settings that shape
 * what goes on the wire, and when, are set explicitly, to the same values as in the Go implementation, so
 * that the two behave alike and a library upgrade can't change them unnoticed.
 */
public final class KafkaClients {
    /**
     * The largest request a producer sends. A block is at most 512 KiB, and so is every record a validator
     * derives from it (see aggregator.BlockBytes), well under this and Kafka's 1 MiB message limit.
     */
    public static final int MAX_REQUEST_BYTES = 1_048_576;

    private KafkaClients() {
    }

    /** A consumer without a group: it is assigned partitions explicitly and never commits offsets. */
    public static <V> KafkaConsumer<String, V> consumer(Config config, String clientId, Deserializer<V> values,
            int maxPollRecords) {
        Map<String, Object> settings = consumerSettings(config, clientId, maxPollRecords);
        return new KafkaConsumer<>(settings, new StringDeserializer(), values);
    }

    /**
     * A member of a consumer group, used to share partitions among the processes of one role.
     *
     * @param instanceId the static group member ID {@code <role>-<siteId>}: a process restarted with the same
     *                   siteId takes its place in the group again, without waiting for its session to time out
     */
    public static <V> KafkaConsumer<String, V> groupConsumer(Config config, String groupId, String instanceId,
            String clientId, Deserializer<V> values, int maxPollRecords) {
        Map<String, Object> settings = consumerSettings(config, clientId, maxPollRecords);
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        settings.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId);
        // The classic group protocol with the range assignor: each bank's partition goes to exactly one member.
        settings.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic");
        settings.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        int sessionTimeoutMs = config.kafka().sessionTimeoutMs();
        settings.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        settings.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, sessionTimeoutMs / 3);
        // Also the rebalance timeout: how long the group waits for its members to rejoin.
        settings.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);
        return new KafkaConsumer<>(settings, new StringDeserializer(), values);
    }

    /**
     * Closes a group consumer after a clean stop, leaving the group right away so its partitions move to the
     * other members now. Only for a stop that was asked for: after a failure, a plain {@code close()} keeps a
     * static member in the group until its session times out, so that a restart with the same siteId takes
     * its place without a rebalance. Closing again afterwards does nothing.
     */
    public static void leaveGroupAndClose(KafkaConsumer<?, ?> consumer) {
        consumer.close(CloseOptions.groupMembershipOperation(CloseOptions.GroupMembershipOperation.LEAVE_GROUP));
    }

    /** An idempotent producer: retries never duplicate or reorder records within a partition. */
    public static KafkaProducer<String, SpecificRecord> producer(Config config, String clientId) {
        return new KafkaProducer<>(producerSettings(config, clientId), new StringSerializer(), AvroSerde.serializer());
    }

    /**
     * A transactional producer. A new producer with the same transactional ID fences this one when it calls
     * {@code initTransactions()}: after that, this one can no longer commit.
     */
    public static KafkaProducer<String, SpecificRecord> transactionalProducer(Config config, String clientId,
            String transactionalId) {
        Map<String, Object> settings = producerSettings(config, clientId);
        settings.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        settings.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, config.kafka().transactionTimeoutMs());
        return new KafkaProducer<>(settings, new StringSerializer(), AvroSerde.serializer());
    }

    public static Admin admin(Config config, String clientId) {
        return Admin.create(Map.of(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.kafka().bootstrapServers(),
                CommonClientConfigs.CLIENT_ID_CONFIG, clientId));
    }

    /**
     * The high watermark of each partition: the end of its log, including the records of Kafka transactions
     * that are still open. (A read_committed consumer's {@code endOffsets} stops before those, at the last
     * stable offset.) A short-lived admin client looks them up, with the client ID of the reader they are for.
     */
    public static Map<TopicPartition, Long> highWatermarks(Config config, String clientId,
            Collection<TopicPartition> partitions) {
        Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
        partitions.forEach(partition -> latest.put(partition, OffsetSpec.latest()));
        Map<TopicPartition, Long> ends = new HashMap<>();
        try (Admin admin = admin(config, clientId)) {
            admin.listOffsets(latest, new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED)).all().get()
                    .forEach((partition, info) -> ends.put(partition, info.offset()));
        } catch (ExecutionException e) {
            throw e.getCause() instanceof KafkaException cause ? cause : new KafkaException(e.getCause());
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
        return ends;
    }

    private static Map<String, Object> consumerSettings(Config config, String clientId, int maxPollRecords) {
        Map<String, Object> settings = new HashMap<>();
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka().bootstrapServers());
        settings.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        // Only records of committed Kafka transactions are visible: this makes every transaction atomic for
        // its readers, which is what exactly-once processing builds on.
        settings.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        settings.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        settings.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, config.kafka().maxPartitionFetchBytes());
        settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        settings.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        return settings;
    }

    private static Map<String, Object> producerSettings(Config config, String clientId) {
        Map<String, Object> settings = new HashMap<>();
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka().bootstrapServers());
        settings.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        settings.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        settings.put(ProducerConfig.ACKS_CONFIG, "all");
        settings.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        settings.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        settings.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        settings.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, MAX_REQUEST_BYTES);
        // Records without a key (new trades) stick to one partition until a batch of this size is full.
        settings.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_384);
        // A send that isn't acknowledged within this time fails: the role then exits with status 1.
        settings.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        // The longest a call may block, e.g. initTransactions while it fences the previous owner of a bank.
        settings.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60_000);
        return settings;
    }
}
