package io.github.yooouuuuuuu.dp;

import io.github.yooouuuuuuu.dp.avro.Block;
import io.github.yooouuuuuuu.dp.avro.Category;
import io.github.yooouuuuuuu.dp.avro.Trade;
import java.util.List;

/** Builds trades for tests. Accounts are named like the real ones (bank1-0007): bank i owns partition i. */
public final class TestTrades {
    private TestTrades() {
    }

    public static Trade send(long serialNumber, String fromAccount, String toAccount, long amount) {
        return trade(serialNumber, fromAccount, toAccount, amount, Category.SEND);
    }

    public static Trade credit(long serialNumber, String fromAccount, String toAccount, long amount) {
        return trade(serialNumber, fromAccount, toAccount, amount, Category.RECEIVE);
    }

    public static Trade trade(long serialNumber, String fromAccount, String toAccount, long amount, Category category) {
        return Trade.newBuilder()
                .setSerialNumber(serialNumber)
                .setOutbank(bankOf(fromAccount))
                .setOutAccount(fromAccount)
                .setInbank(bankOf(toAccount))
                .setInAccount(toAccount)
                .setOutbankPartition(partitionOf(fromAccount))
                .setInbankPartition(partitionOf(toAccount))
                .setAmount(amount)
                .setCategory(category)
                .setTimestamp1(1_000)
                .setTimestamp2(0)
                .build();
    }

    public static Block block(Trade... trades) {
        return new Block(List.of(trades));
    }

    public static String bankOf(String account) {
        return account.substring(0, account.indexOf('-'));
    }

    public static int partitionOf(String account) {
        return Integer.parseInt(bankOf(account).substring("bank".length()));
    }
}
