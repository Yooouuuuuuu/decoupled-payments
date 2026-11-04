package io.github.yooouuuuuuu.dp.validator;

import static io.github.yooouuuuuuu.dp.TestTrades.block;
import static io.github.yooouuuuuuu.dp.TestTrades.send;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.yooouuuuuuu.dp.avro.Model;
import io.github.yooouuuuuuu.dp.avro.ValidatorOffset;
import io.github.yooouuuuuuu.dp.cli.Failure;
import io.github.yooouuuuuuu.dp.kafka.AvroSerde;
import io.github.yooouuuuuuu.dp.kafka.Topics;
import io.github.yooouuuuuuu.dp.validator.BankBatch.ProcessedBlock;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The failure policy of a validator transaction, with Kafka's mock producer as bank 0's producer. */
class BankTransactionTest {
    private final Ledger ledger = new Ledger();
    private final MockProducer<String, SpecificRecord> producer =
            new MockProducer<>(true, null, new StringSerializer(), AvroSerde.serializer());

    @BeforeEach
    void ownBank0() {
        producer.initTransactions();
        ledger.lock();
        ledger.add(new OwnedBank(0, "bank0", new TransactionalBankWriter(producer), Set.of("bank0-0000"), 0),
                Map.of("bank0-0000", 100L));
    }

    @AfterEach
    void releaseTheLock() {
        ledger.unlock();
    }

    /** Validates one trade of 30 from bank0-0000, sends its orders record, and says it read up to offset 7. */
    private static long validateOneTrade(OwnedBank bank, BankBatch batch) {
        ProcessedBlock processed = batch.validate(block(send(1, "bank0-0000", "bank1-0000", 30)), 5);
        bank.writer().send(ValidatorRecords.order(bank, processed.order()));
        return 8;
    }

    @Test
    void aTransactionCommitsTheResultsTheBalancesAndThePositionTogether() {
        BankBatch batch = BankTransaction.run(ledger, 0, Model.CONCURRENT, Topics.BLOCKS,
                BankTransactionTest::validateOneTrade);

        assertTrue(producer.transactionCommitted());
        assertEquals(1, batch.accepted());
        List<ProducerRecord<String, SpecificRecord>> written = producer.history();
        assertEquals(List.of(Topics.ORDERS, Topics.LOCAL_BALANCE, Topics.VALIDATOR_OFFSETS),
                written.stream().map(ProducerRecord::topic).toList());
        assertEquals(new ValidatorOffset(Topics.BLOCKS, 0, 8L), written.getLast().value());
        assertEquals(70L, ledger.balances().get("bank0-0000"));
    }

    @Test
    void anErrorInATransactionMarksTheLedgerDirtyAndAbortsIt() {
        OutOfMemoryError error = new OutOfMemoryError("while serializing an orders record");
        OutOfMemoryError thrown = assertThrows(OutOfMemoryError.class,
                () -> BankTransaction.run(ledger, 0, Model.CONCURRENT, Topics.BLOCKS, (bank, batch) -> {
                    validateOneTrade(bank, batch); // the balance map now holds the debit
                    throw error;
                }));

        assertSame(error, thrown);
        assertTrue(producer.transactionAborted());
        assertFalse(producer.transactionCommitted());
        assertEquals(List.of(), producer.history());
        // The debit can't be undone, so no later transaction may commit it.
        assertThrows(Failure.class, ledger::requireClean);
        assertThrows(Failure.class,
                () -> BankTransaction.run(ledger, 0, Model.CONCURRENT, Topics.CREDITS, (bank, batch) -> 1));
    }

    @Test
    void aRuntimeExceptionDoesTheSame() {
        assertThrows(IllegalStateException.class, () -> BankTransaction.run(ledger, 0, Model.CONCURRENT,
                Topics.BLOCKS, (bank, batch) -> {
                    batch.validate(block(send(1, "bank0-9999", "bank1-0000", 30)), 5);
                    return 1;
                }));
        assertTrue(producer.transactionAborted());
        assertThrows(Failure.class, ledger::requireClean);
    }

    @Test
    void aPositionNamesItsTopicInTheKeyAndTheValue() {
        assertEquals(42, BankTakeover.positionOf(position("blocks", "blocks", 0), 0));
        assertEquals(42, BankTakeover.positionOf(position("credits", "credits", 3), 3));
        for (ConsumerRecord<String, ValidatorOffset> bad : List.of(position("credits", "blocks", 0),
                position("blocks", "blocks", 1), position("trades", "trades", 0), position(null, "blocks", 0))) {
            assertThrows(IllegalStateException.class, () -> BankTakeover.positionOf(bad, 0), bad.toString());
        }
    }

    private static ConsumerRecord<String, ValidatorOffset> position(String key, String topic, int partition) {
        return new ConsumerRecord<>(Topics.VALIDATOR_OFFSETS, partition, 5, key,
                new ValidatorOffset(topic, partition, 42L));
    }
}
