package io.github.yooouuuuuuu.dp.validator;

import io.github.yooouuuuuuu.dp.config.Config;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Writes the results of one bank. Every write for a bank (validating its blocks and applying its credits)
 * goes through the bank's own writer, and so through the bank's own producer.
 */
sealed interface BankWriter permits TransactionalBankWriter, PlainBankWriter {

    /** Creates the writer of a bank; with Kafka transactions, this fences the bank's previous owner. */
    static BankWriter open(Config config, String clientId, String bankId) {
        return config.kafkaTransactions()
                ? TransactionalBankWriter.open(config, clientId, "validator-" + bankId)
                : PlainBankWriter.open(config, clientId);
    }

    void begin();

    void send(ProducerRecord<String, SpecificRecord> record);

    /** Makes the batch's results durable together with the new read position (a validatorOffsets record). */
    void commit(ProducerRecord<String, SpecificRecord> position);

    /**
     * Gives up on the current batch after {@code cause}, if that is still possible. The process exits
     * next; a failure to abort is added to {@code cause}.
     */
    void abort(Throwable cause);

    void close();
}
