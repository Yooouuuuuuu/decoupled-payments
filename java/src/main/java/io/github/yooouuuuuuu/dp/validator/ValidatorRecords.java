package io.github.yooouuuuuuu.dp.validator;

import io.github.yooouuuuuuu.dp.avro.Block;
import io.github.yooouuuuuuu.dp.avro.LocalBalance;
import io.github.yooouuuuuuu.dp.avro.Model;
import io.github.yooouuuuuuu.dp.avro.ValidatorOffset;
import io.github.yooouuuuuuu.dp.kafka.Topics;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

/** The records a validator writes for a bank. Each names its partition: the partition of the bank it is for. */
final class ValidatorRecords {
    private ValidatorRecords() {
    }

    static ProducerRecord<String, SpecificRecord> order(OwnedBank bank, Block block) {
        return new ProducerRecord<>(Topics.ORDERS, bank.partition(), bank.id(), block);
    }

    static ProducerRecord<String, SpecificRecord> balance(OwnedBank bank, String account, long balance) {
        return new ProducerRecord<>(Topics.LOCAL_BALANCE, bank.partition(), account, new LocalBalance(balance));
    }

    /** How far the bank has read {@code topic}: the next offset to read after this transaction. */
    static ProducerRecord<String, SpecificRecord> position(OwnedBank bank, String topic, long nextOffset) {
        return new ProducerRecord<>(Topics.VALIDATOR_OFFSETS, bank.partition(), topic,
                new ValidatorOffset(topic, bank.partition(), nextOffset));
    }

    /** The credits of accepted trades for one receiving bank. This is where the two models differ. */
    static ProducerRecord<String, SpecificRecord> credits(Model model, int receiverPartition, Block credits) {
        return switch (model) {
            // Back into trades: an aggregator puts the credits into a block for the receiver's bank.
            case SEQUENTIAL -> new ProducerRecord<>(Topics.TRADES, credits);
            // Straight to the receiver's bank, whose credits thread applies them.
            case CONCURRENT -> new ProducerRecord<>(Topics.CREDITS, receiverPartition,
                    credits.getTrades().getFirst().getInbank(), credits);
        };
    }
}
