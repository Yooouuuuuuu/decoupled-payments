package io.github.yooouuuuuuu.dp.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.yooouuuuuuu.dp.avro.Category;
import io.github.yooouuuuuuu.dp.avro.Trade;
import io.github.yooouuuuuuu.dp.config.Distribution;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TradeFactoryTest {

    private static TradeFactory factory(List<Account> accounts, Distribution distribution, boolean interBankOnly) {
        AccountPicker picker = AccountPicker.of(distribution, accounts, 1.5);
        return new TradeFactory(7, picker, new Random(42), interBankOnly, 1, 100);
    }

    @Test
    void serialNumberIsRunIdTimesTwoToThe32PlusSequence() {
        assertEquals(4294967297L, TradeFactory.serialNumber(1, 1));
        assertEquals(Integer.MAX_VALUE * 4294967296L + 5, TradeFactory.serialNumber(Integer.MAX_VALUE, 5));
        TradeFactory factory = factory(AccountPickerTest.accounts(2, 3), Distribution.UNIFORM, true);
        assertEquals(TradeFactory.serialNumber(7, 1), factory.next(0).getSerialNumber());
        assertEquals(TradeFactory.serialNumber(7, 2), factory.next(0).getSerialNumber());
    }

    @Test
    void newTradesAreInterBankWhenThereAreSeveralBanks() {
        for (Distribution distribution : Distribution.values()) {
            TradeFactory factory = factory(AccountPickerTest.accounts(3, 4), distribution, true);
            for (int i = 0; i < 10_000; i++) {
                Trade trade = factory.next(123);
                assertNotEquals(trade.getOutbankPartition(), trade.getInbankPartition());
                assertNotEquals(trade.getOutbank(), trade.getInbank());
                assertTrue(trade.getOutAccount().startsWith(trade.getOutbank() + "-"));
                assertTrue(trade.getInAccount().startsWith(trade.getInbank() + "-"));
            }
        }
    }

    @Test
    void withOneBankTheAccountsStillDiffer() {
        TradeFactory factory = factory(AccountPickerTest.accounts(1, 2), Distribution.ZIPF, false);
        for (int i = 0; i < 1_000; i++) {
            Trade trade = factory.next(0);
            assertNotEquals(trade.getOutAccount(), trade.getInAccount());
            assertEquals(trade.getOutbank(), trade.getInbank());
        }
    }

    @Test
    void newTradeFields() {
        TradeFactory factory = factory(AccountPickerTest.accounts(2, 3), Distribution.UNIFORM, true);
        Set<Long> amounts = new HashSet<>();
        for (int i = 0; i < 10_000; i++) {
            Trade trade = factory.next(1_730_000_000_000L);
            assertEquals(Category.SEND, trade.getCategory());
            assertEquals(1_730_000_000_000L, trade.getTimestamp1());
            assertEquals(0, trade.getTimestamp2());
            assertTrue(trade.getAmount() >= 1 && trade.getAmount() <= 100, "amount " + trade.getAmount());
            amounts.add(trade.getAmount());
        }
        assertEquals(100, amounts.size(), "every amount from 1 to 100 occurs");
    }

    @Test
    void sameSeedSameTrades() {
        List<Account> accounts = AccountPickerTest.accounts(3, 5);
        TradeFactory first = factory(accounts, Distribution.ZIPF, true);
        TradeFactory second = factory(accounts, Distribution.ZIPF, true);
        for (int i = 0; i < 100; i++) {
            assertEquals(first.next(0), second.next(0));
        }
    }

    @Test
    void pacingDueCount() {
        assertEquals(1, Pacing.dueAfter(0, 1000));
        assertEquals(1, Pacing.dueAfter(999_999, 1000));
        assertEquals(2, Pacing.dueAfter(1_000_000, 1000));
        assertEquals(2001, Pacing.dueAfter(1_000_000_000, 2000));
        assertEquals(36_001, Pacing.dueAfter(3_600_000_000_000L, 10));
    }
}
