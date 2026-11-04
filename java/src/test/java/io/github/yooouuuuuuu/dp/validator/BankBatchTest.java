package io.github.yooouuuuuuu.dp.validator;

import static io.github.yooouuuuuuu.dp.TestTrades.block;
import static io.github.yooouuuuuuu.dp.TestTrades.credit;
import static io.github.yooouuuuuuu.dp.TestTrades.send;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.yooouuuuuuu.dp.TestTrades;
import io.github.yooouuuuuuu.dp.avro.Block;
import io.github.yooouuuuuuu.dp.avro.Category;
import io.github.yooouuuuuuu.dp.avro.Model;
import io.github.yooouuuuuuu.dp.avro.Trade;
import io.github.yooouuuuuuu.dp.validator.BankBatch.ProcessedBlock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class BankBatchTest {
    private static final long NOW = 1_730_000_000_000L;

    /** The balances of the validator, which owns bank 0 and bank 1. */
    private final Map<String, Long> balances =
            new HashMap<>(Map.of("bank0-0000", 100L, "bank0-0001", 50L, "bank1-0005", 70L));

    /** A batch of bank 0. */
    private BankBatch batch(Model model) {
        return new BankBatch(0, "bank0", Set.of("bank0-0000", "bank0-0001"), model, balances);
    }

    @Test
    void acceptsATradeWhenTheSenderHasEnoughMoney() {
        BankBatch batch = batch(Model.CONCURRENT);
        ProcessedBlock processed = batch.validate(block(send(1, "bank0-0000", "bank1-0005", 100)), NOW);

        Trade order = processed.order().getTrades().getFirst();
        assertEquals(Category.SEND, order.getCategory(), "accepted trades stay SEND on orders");
        assertEquals(NOW, order.getTimestamp2());
        assertEquals(0L, balances.get("bank0-0000"));
        assertEquals(Map.of("bank0-0000", 0L), batch.changedBalances());
        assertEquals(1, batch.accepted());
    }

    @Test
    void rejectsATradeWhenTheSenderHasTooLittle() {
        BankBatch batch = batch(Model.CONCURRENT);
        ProcessedBlock processed = batch.validate(block(send(1, "bank0-0001", "bank1-0005", 51)), NOW);

        Trade order = processed.order().getTrades().getFirst();
        assertEquals(Category.REJECTED, order.getCategory());
        assertEquals(NOW, order.getTimestamp2());
        assertEquals(50L, balances.get("bank0-0001"));
        assertEquals(Map.of(), processed.credits());
        assertEquals(Map.of(), batch.changedBalances());
        assertEquals(1, batch.rejected());
    }

    @Test
    void tradesOfABlockAreValidatedInOrder() {
        BankBatch batch = batch(Model.CONCURRENT);
        ProcessedBlock processed = batch.validate(block(
                send(1, "bank0-0001", "bank1-0005", 30),
                send(2, "bank0-0001", "bank1-0005", 30),   // only 20 left
                send(3, "bank0-0001", "bank2-0005", 20)), NOW);

        assertEquals(List.of(Category.SEND, Category.REJECTED, Category.SEND),
                processed.order().getTrades().stream().map(Trade::getCategory).toList());
        assertEquals(0L, balances.get("bank0-0001"));
    }

    @Test
    void creditsAreGroupedByReceivingBank() {
        BankBatch batch = batch(Model.CONCURRENT);
        ProcessedBlock processed = batch.validate(block(
                send(1, "bank0-0000", "bank2-0001", 10),
                send(2, "bank0-0000", "bank1-0002", 20),
                send(3, "bank0-0001", "bank2-0003", 30),
                send(4, "bank0-0001", "bank1-0004", 999)), NOW);   // rejected: no credit

        assertEquals(List.of(1, 2), List.copyOf(processed.credits().keySet()));
        assertEquals(List.of(2L), serialNumbers(processed.credits().get(1)));
        assertEquals(List.of(1L, 3L), serialNumbers(processed.credits().get(2)));
        for (Block credits : processed.credits().values()) {
            for (Trade credit : credits.getTrades()) {
                assertEquals(Category.RECEIVE, credit.getCategory());
                assertEquals(NOW, credit.getTimestamp2());
            }
        }
        assertEquals(Category.SEND, processed.order().getTrades().getFirst().getCategory(),
                "the credit is a copy; the orders record keeps SEND");
    }

    @Test
    void changedBalancesHoldTheFinalBalanceOfEveryChangedAccount() {
        BankBatch batch = batch(Model.SEQUENTIAL);
        batch.validate(block(send(1, "bank0-0000", "bank1-0001", 10), send(2, "bank0-0000", "bank1-0001", 5)), NOW);
        batch.validate(block(credit(3, "bank1-0001", "bank0-0000", 1)), NOW);

        assertEquals(Map.of("bank0-0000", 86L), batch.changedBalances(), "bank0-0001 didn't change");
        assertEquals(2, batch.blocks());
    }

    @Test
    void sequentialModelAppliesCreditsFoundInBlocks() {
        BankBatch batch = batch(Model.SEQUENTIAL);
        ProcessedBlock processed = batch.validate(block(
                send(1, "bank0-0000", "bank1-0001", 10),
                credit(2, "bank2-0007", "bank0-0001", 25)), NOW);

        assertEquals(90L, balances.get("bank0-0000"));
        assertEquals(75L, balances.get("bank0-0001"));
        Trade completion = processed.order().getTrades().get(1);
        assertEquals(Category.RECEIVE, completion.getCategory(), "RECEIVE on orders = completed");
        assertEquals(0, completion.getTimestamp2(), "a credit keeps the validation time it came with");
        assertEquals(List.of(1), List.copyOf(processed.credits().keySet()));
        assertEquals(1, batch.credited());
    }

    @Test
    void concurrentModelRejectsCreditsInBlocks() {
        Block withCredit = block(credit(2, "bank2-0007", "bank0-0001", 25));
        assertThrows(IllegalStateException.class, () -> batch(Model.CONCURRENT).validate(withCredit, NOW));
    }

    @Test
    void creditsTopicBlocksAreApplied() {
        BankBatch batch = batch(Model.CONCURRENT);
        batch.applyCredits(block(credit(1, "bank1-0001", "bank0-0000", 7), credit(2, "bank2-0001", "bank0-0000", 3)));
        assertEquals(110L, balances.get("bank0-0000"));
        assertEquals(Map.of("bank0-0000", 110L), batch.changedBalances());
        assertEquals(2, batch.credited());
    }

    @Test
    void tradesOfAnotherBankAreRejectedAsFatal() {
        assertThrows(IllegalStateException.class,
                () -> batch(Model.CONCURRENT).validate(block(send(1, "bank1-0000", "bank0-0001", 1)), NOW));
        assertThrows(IllegalStateException.class,
                () -> batch(Model.CONCURRENT).applyCredits(block(credit(1, "bank0-0000", "bank1-0001", 1))));
        assertThrows(IllegalStateException.class,
                () -> batch(Model.SEQUENTIAL).validate(block(credit(1, "bank0-0000", "bank1-0001", 1)), NOW));
    }

    @Test
    void anAccountOfAnotherOwnedBankIsFatal() {
        Trade foreign = send(1, "bank1-0005", "bank2-0001", 10);
        foreign.setOutbankPartition(0); // claims to be bank 0's
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> batch(Model.CONCURRENT).validate(block(foreign), NOW));
        assertTrue(e.getMessage().startsWith("account bank1-0005 isn't one of bank0's own accounts"), e.getMessage());
        assertEquals(70L, balances.get("bank1-0005"), "bank 1's balance is untouched");

        Trade foreignCredit = credit(2, "bank2-0001", "bank1-0005", 10);
        foreignCredit.setInbankPartition(0);
        assertThrows(IllegalStateException.class, () -> batch(Model.CONCURRENT).applyCredits(block(foreignCredit)));
        assertThrows(IllegalStateException.class, () -> batch(Model.SEQUENTIAL).validate(block(foreignCredit), NOW));
    }

    @Test
    void amountsMustBePositive() {
        for (long amount : new long[] {0, -500}) {
            assertThrows(IllegalStateException.class,
                    () -> batch(Model.CONCURRENT).validate(block(send(1, "bank0-0000", "bank1-0005", amount)), NOW));
            assertThrows(IllegalStateException.class,
                    () -> batch(Model.CONCURRENT).applyCredits(block(credit(2, "bank1-0005", "bank0-0000", amount))));
            assertThrows(IllegalStateException.class,
                    () -> batch(Model.SEQUENTIAL).validate(block(credit(3, "bank1-0005", "bank0-0000", amount)), NOW));
        }
        assertEquals(100L, balances.get("bank0-0000"));
    }

    @Test
    void aBalanceOverflowIsFatal() {
        balances.put("bank0-0000", Long.MAX_VALUE);
        assertThrows(ArithmeticException.class,
                () -> batch(Model.CONCURRENT).applyCredits(block(credit(1, "bank1-0005", "bank0-0000", 1))));
    }

    @Test
    void unknownAccountsAndRejectedTradesAreFatal() {
        assertThrows(IllegalStateException.class,
                () -> batch(Model.CONCURRENT).validate(block(send(1, "bank0-0099", "bank1-0001", 1)), NOW));
        Trade rejected = TestTrades.trade(1, "bank0-0000", "bank1-0001", 1, Category.REJECTED);
        assertThrows(IllegalStateException.class, () -> batch(Model.CONCURRENT).validate(block(rejected), NOW));
        assertThrows(IllegalStateException.class,
                () -> batch(Model.CONCURRENT).applyCredits(block(send(1, "bank1-0000", "bank0-0001", 1))));
    }

    private static List<Long> serialNumbers(Block block) {
        return block.getTrades().stream().map(Trade::getSerialNumber).toList();
    }
}
