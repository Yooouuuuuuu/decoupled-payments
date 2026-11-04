package io.github.yooouuuuuuu.dp.validator;

import io.github.yooouuuuuuu.dp.config.Config;
import io.github.yooouuuuuuu.dp.kafka.KafkaClients;
import io.github.yooouuuuuuu.dp.kafka.SendErrors;
import java.time.Duration;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * kafkaTransactions=false ("w/o Tx"): the same writes with an idempotent producer and no transaction. That
 * isn't atomic, so the system isn't consistent under failures; this mode only measures what transactions cost.
 */
final class PlainBankWriter implements BankWriter {
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(5);

    private final KafkaProducer<String, SpecificRecord> producer;
    private final SendErrors errors = new SendErrors();

    private PlainBankWriter(KafkaProducer<String, SpecificRecord> producer) {
        this.producer = producer;
    }

    static PlainBankWriter open(Config config, String clientId) {
        return new PlainBankWriter(KafkaClients.producer(config, clientId));
    }

    @Override
    public void begin() {
        // Nothing to begin without a transaction.
    }

    @Override
    public void send(ProducerRecord<String, SpecificRecord> record) {
        producer.send(record, errors);
    }

    /**
     * The results first, then the position: a crash in between makes the batch run again (duplicates), but
     * can never skip it (lost trades).
     */
    @Override
    public void commit(ProducerRecord<String, SpecificRecord> position) {
        producer.flush();
        errors.throwIfAny();
        producer.send(position, errors);
        producer.flush();
        errors.throwIfAny();
    }

    @Override
    public void abort(Throwable cause) {
        // Nothing can be undone without a transaction.
    }

    @Override
    public void close() {
        producer.close(CLOSE_TIMEOUT);
    }
}
