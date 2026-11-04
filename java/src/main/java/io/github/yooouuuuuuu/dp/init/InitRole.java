package io.github.yooouuuuuuu.dp.init;

import io.github.yooouuuuuuu.dp.avro.AccountInfo;
import io.github.yooouuuuuuu.dp.avro.LocalBalance;
import io.github.yooouuuuuuu.dp.cli.Failure;
import io.github.yooouuuuuuu.dp.config.Config;
import io.github.yooouuuuuuu.dp.kafka.BankDirectory;
import io.github.yooouuuuuuu.dp.kafka.Groups;
import io.github.yooouuuuuuu.dp.kafka.Ids;
import io.github.yooouuuuuuu.dp.kafka.KafkaClients;
import io.github.yooouuuuuuu.dp.kafka.SendErrors;
import io.github.yooouuuuuuu.dp.kafka.Topics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The init role: creates the topics and writes the banks, their accounts and the starting balances. With
 * {@code init.reset} it first deletes all topics and consumer groups, i.e. all data.
 */
public final class InitRole {
    private static final Logger log = LoggerFactory.getLogger(InitRole.class);
    private static final Duration WAIT_LIMIT = Duration.ofSeconds(60);
    /** localBalance and validatorOffsets only need the latest record per key, so Kafka may drop older ones. */
    private static final Map<String, String> COMPACTED = Map.of(
            "cleanup.policy", "compact",
            "segment.ms", "60000",
            "min.cleanable.dirty.ratio", "0.1");
    /**
     * accountInfo is compacted too (one record per bank), so it is never deleted after the retention time:
     * the cluster stays usable however long it is idle.
     */
    private static final Map<String, String> KEPT = Map.of("cleanup.policy", "compact");

    private final Config config;

    public InitRole(Config config) {
        this.config = config;
    }

    public void run() throws InterruptedException {
        try (Admin admin = KafkaClients.admin(config, clientId("admin"))) {
            if (config.init().reset()) {
                deleteGroups(admin);
                deleteTopics(admin);
            }
            Set<String> existing = existingTopics(admin);
            if (existing.containsAll(Topics.ALL)) {
                checkAlreadyInitialized();
                return;
            }
            if (!existing.isEmpty()) {
                throw new Failure("some of the topics exist already " + new TreeSet<>(existing)
                        + ": run init --reset");
            }
            createTopics(admin);
        }
        writeAccounts();
    }

    private void deleteGroups(Admin admin) throws InterruptedException {
        Map<String, KafkaFuture<Void>> groups = admin.deleteConsumerGroups(Groups.ALL).deletedGroups();
        for (Map.Entry<String, KafkaFuture<Void>> group : groups.entrySet()) {
            try {
                await(group.getValue());
                log.info("deleted consumer group {}", group.getKey());
            } catch (GroupIdNotFoundException e) {
                // Nothing to delete.
            } catch (GroupNotEmptyException e) {
                throw new Failure("consumer group " + group.getKey() + " still has members: stop the aggregators "
                        + "and validators first (a killed process stays a member for kafka.sessionTimeoutMs)");
            }
        }
    }

    private void deleteTopics(Admin admin) throws InterruptedException {
        Map<String, KafkaFuture<Void>> topics = admin.deleteTopics(Topics.ALL).topicNameValues();
        for (Map.Entry<String, KafkaFuture<Void>> topic : topics.entrySet()) {
            try {
                await(topic.getValue());
                log.info("deleted topic {}", topic.getKey());
            } catch (UnknownTopicOrPartitionException e) {
                // Nothing to delete.
            }
        }
        waitUntil("the deleted topics are gone", () -> existingTopics(admin).isEmpty());
    }

    private void checkAlreadyInitialized() {
        Optional<BankDirectory> found = BankDirectory.readIfPresent(config, clientId("accountInfo"));
        if (found.isEmpty()) {
            throw new Failure("the topics exist but accountInfo is empty, so an earlier init didn't finish: "
                    + "run init --reset");
        }
        BankDirectory cluster = found.get();
        List<String> differences = new ArrayList<>();
        compare(differences, "model", BankDirectory.name(cluster.model()), BankDirectory.name(config.model()));
        compare(differences, "kafkaTransactions", cluster.kafkaTransactions(), config.kafkaTransactions());
        compare(differences, "bank.count", cluster.banks().size(), config.bank().count());
        // init gives every bank the same number of accounts.
        compare(differences, "bank.accountsPerBank", cluster.banks().getFirst().accounts().size(),
                config.bank().accountsPerBank());
        compare(differences, "bank.initialBalance", cluster.initialBalance(), config.bank().initialBalance());
        if (!differences.isEmpty()) {
            throw new Failure("already initialized with other settings: " + String.join("; ", differences)
                    + ". Run init --reset to start over with these settings (deletes all data)");
        }
        log.info("already initialized: {} banks with {} accounts each", cluster.banks().size(),
                config.bank().accountsPerBank());
    }

    private static void compare(List<String> differences, String setting, Object cluster, Object configured) {
        if (!cluster.equals(configured)) {
            differences.add(setting + " is " + cluster + " in the cluster but " + configured + " in the settings");
        }
    }

    private void createTopics(Admin admin) throws InterruptedException {
        int banks = config.bank().count();
        short replication = config.topics().replicationFactor();
        List<NewTopic> topics = List.of(
                new NewTopic(Topics.TRADES, config.topics().tradesPartitions(), replication),
                new NewTopic(Topics.BLOCKS, banks, replication),
                new NewTopic(Topics.CREDITS, banks, replication),
                new NewTopic(Topics.ORDERS, banks, replication),
                new NewTopic(Topics.LOCAL_BALANCE, banks, replication).configs(COMPACTED),
                new NewTopic(Topics.VALIDATOR_OFFSETS, banks, replication).configs(COMPACTED),
                new NewTopic(Topics.ACCOUNT_INFO, 1, replication).configs(KEPT));
        await(admin.createTopics(topics).all());
        waitUntil("every partition has a leader", () -> everyPartitionHasALeader(admin));
        awaitLeadersServing(admin);
        log.info("created topics {}", Topics.ALL);
    }

    /**
     * Writes the starting balances first and accountInfo last: accountInfo is what tells the other roles
     * (and a later init) that the system is set up, so it must not exist without the balances.
     */
    private void writeAccounts() {
        int banks = config.bank().count();
        long initialBalance = config.bank().initialBalance();
        SendErrors errors = new SendErrors();
        try (KafkaProducer<String, SpecificRecord> producer = KafkaClients.producer(config, clientId("producer"))) {
            List<AccountInfo> infos = new ArrayList<>();
            for (int partition = 0; partition < banks; partition++) {
                String bank = Ids.bank(partition);
                List<String> accounts = IntStream.range(0, config.bank().accountsPerBank())
                        .mapToObj(index -> Ids.account(bank, index))
                        .toList();
                for (String account : accounts) {
                    producer.send(new ProducerRecord<>(Topics.LOCAL_BALANCE, partition, account,
                            new LocalBalance(initialBalance)), errors);
                }
                infos.add(AccountInfo.newBuilder()
                        .setBank(bank)
                        .setBankPartition(partition)
                        .setAccounts(accounts)
                        .setInitialBalance(initialBalance)
                        .setModel(config.model())
                        .setKafkaTransactions(config.kafkaTransactions())
                        .build());
            }
            producer.flush();
            errors.throwIfAny();
            for (AccountInfo info : infos) {
                producer.send(new ProducerRecord<>(Topics.ACCOUNT_INFO, 0, info.getBank(), info), errors);
            }
            producer.flush();
            errors.throwIfAny();
        }
        long accounts = (long) banks * config.bank().accountsPerBank();
        log.info("initialized {} banks with {} accounts each ({} accounts, total money {}), "
                + "model={} kafkaTransactions={}", banks, config.bank().accountsPerBank(), accounts,
                Math.multiplyExact(accounts, initialBalance), BankDirectory.name(config.model()),
                config.kafkaTransactions());
    }

    private static Set<String> existingTopics(Admin admin) throws InterruptedException {
        Set<String> ours = new HashSet<>(await(admin.listTopics().names()));
        ours.retainAll(Topics.ALL);
        return ours;
    }

    private static boolean everyPartitionHasALeader(Admin admin) throws InterruptedException {
        try {
            Map<String, TopicDescription> topics = await(admin.describeTopics(Topics.ALL).allTopicNames());
            return topics.values().stream()
                    .flatMap(topic -> topic.partitions().stream())
                    .allMatch(partition -> partition.leader() != null);
        } catch (UnknownTopicOrPartitionException e) {
            return false; // the new topics haven't reached every broker's metadata yet
        }
    }

    /**
     * The metadata names a leader a moment before that broker serves the new partition; writing in between
     * only causes retries. An offset lookup waits (retrying quietly) until every leader answers.
     */
    private static void awaitLeadersServing(Admin admin) throws InterruptedException {
        Map<TopicPartition, OffsetSpec> partitions = new HashMap<>();
        for (TopicDescription topic : await(admin.describeTopics(Topics.ALL).allTopicNames()).values()) {
            for (TopicPartitionInfo partition : topic.partitions()) {
                partitions.put(new TopicPartition(topic.name(), partition.partition()), OffsetSpec.latest());
            }
        }
        await(admin.listOffsets(partitions).all());
    }

    private String clientId(String suffix) {
        return Ids.client("init", config.siteId(), suffix);
    }

    /** Waits for an admin call; a failure is rethrown as the Kafka exception that caused it. */
    private static <T> T await(KafkaFuture<T> future) throws InterruptedException {
        try {
            return future.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof KafkaException cause) {
                throw cause;
            }
            throw new KafkaException(e.getCause());
        }
    }

    private interface Condition {
        boolean holds() throws InterruptedException;
    }

    private static void waitUntil(String what, Condition condition) throws InterruptedException {
        long start = System.nanoTime();
        while (!condition.holds()) {
            if (System.nanoTime() - start > WAIT_LIMIT.toNanos()) {
                throw new Failure("gave up after " + WAIT_LIMIT.toSeconds() + " s waiting until " + what);
            }
            Thread.sleep(200);
        }
    }
}
