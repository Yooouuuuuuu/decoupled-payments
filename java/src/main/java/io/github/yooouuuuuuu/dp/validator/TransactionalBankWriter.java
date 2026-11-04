package io.github.yooouuuuuuu.dp.validator;

import io.github.yooouuuuuuu.dp.config.Config;
import io.github.yooouuuuuuu.dp.kafka.KafkaClients;
import java.time.Duration;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Writes a bank's results in Kafka transactions: the orders, the credits, the changed balances and the new
 * read position of a batch become visible together, or not at all.
 */
final class TransactionalBankWriter implements BankWriter {
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);

    private final Producer<String, SpecificRecord> producer;

    /** @param producer a transactional producer whose {@code initTransactions()} has returned */
    TransactionalBankWriter(Producer<String, SpecificRecord> producer) {
        this.producer = producer;
    }

    /**
     * The transactional.id is per bank ({@code validator-bank0}), not per process. When a bank changes owner,
     * the new owner's {@code initTransactions()} bumps the producer epoch of that ID: the old owner, even a
     * paused "zombie", can't commit any more, and its unfinished transaction is completed or aborted before
     * this method returns.
     */
    static TransactionalBankWriter open(Config config, String clientId, String transactionalId) {
        KafkaProducer<String, SpecificRecord> producer =
                KafkaClients.transactionalProducer(config, clientId, transactionalId);
        try {
            producer.initTransactions();
        } catch (RuntimeException e) {
            producer.close(Duration.ZERO);
            throw e;
        }
        return new TransactionalBankWriter(producer);
    }

    @Override
    public void begin() {
        producer.beginTransaction();
    }

    @Override
    public void send(ProducerRecord<String, SpecificRecord> record) {
        producer.send(record);
    }

    /** Throws if any send of the transaction failed: then nothing of it becomes visible. */
    @Override
    public void commit(ProducerRecord<String, SpecificRecord> position) {
        producer.send(position);
        producer.commitTransaction();
    }

    @Override
    public void abort(Throwable cause) {
        try {
            producer.abortTransaction();
        } catch (RuntimeException e) {
            cause.addSuppressed(e); // e.g. fenced: then the broker has already aborted it
        }
    }

    @Override
    public void close() {
        producer.close(CLOSE_TIMEOUT);
    }
}
