package io.github.yooouuuuuuu.dp.validator;

import io.github.yooouuuuuuu.dp.avro.Block;
import io.github.yooouuuuuuu.dp.avro.Category;
import io.github.yooouuuuuuu.dp.avro.Model;
import io.github.yooouuuuuuu.dp.avro.Trade;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The work of one Kafka transaction of one bank, as pure code (no Kafka): it validates trades, applies
 * credits, and remembers which balances changed. The caller sends what it returns; all of it becomes
 * visible at once when the transaction commits.
 */
final class BankBatch {
    private final int partition;
    private final String bankId;
    private final Set<String> accounts;
    private final Model model;
    private final Map<String, Long> balances;
    private final Map<String, Long> changedBalances = new TreeMap<>();
    private int blocks;
    private int accepted;
    private int rejected;
    private int credited;

    /**
     * @param partition the bank's partition: every trade in the batch must belong to this bank
     * @param accounts  the bank's own accounts, as accountInfo lists them
     * @param balances  the validator's balance map, of all the banks it owns; updated in place
     */
    BankBatch(int partition, String bankId, Set<String> accounts, Model model, Map<String, Long> balances) {
        this.partition = partition;
        this.bankId = bankId;
        this.accounts = accounts;
        this.model = model;
        this.balances = balances;
    }

    /**
     * The validation phase, for a block from the blocks topic. A new trade (SEND) is accepted if the sender
     * has enough money, which is then deducted, and stays SEND; otherwise it becomes REJECTED. Either way
     * it gets its validation time ({@code timestamp2}). In the Sequential model the block may also hold
     * credits (RECEIVE) that came back through trades: they are added to the receiver's balance without a
     * check, because the trade was already validated at the sender's bank.
     *
     * <p>The block is updated in place and becomes the orders record. A trade that can't be valid input
     * (another bank's, a non-positive amount, a rejected one) is fatal.
     *
     * @return the block for orders, and the credits of the accepted trades by receiving bank
     */
    ProcessedBlock validate(Block block, long nowMs) {
        blocks++;
        Map<Integer, List<Trade>> credits = new TreeMap<>();
        for (Trade trade : block.getTrades()) {
            switch (trade.getCategory()) {
                case SEND -> {
                    requireOwnBank(trade.getOutbankPartition(), trade.getOutAccount(), trade);
                    requirePositiveAmount(trade);
                    trade.setTimestamp2(nowMs);
                    if (balanceOf(trade.getOutAccount()) >= trade.getAmount()) {
                        add(trade.getOutAccount(), -trade.getAmount());
                        credits.computeIfAbsent(trade.getInbankPartition(), receiver -> new ArrayList<>())
                                .add(creditOf(trade));
                        accepted++;
                    } else {
                        trade.setCategory(Category.REJECTED);
                        rejected++;
                    }
                }
                case RECEIVE -> {
                    if (model != Model.SEQUENTIAL) {
                        throw new IllegalStateException("a credit on the blocks topic, but in the Concurrent model "
                                + "credits travel through the credits topic: " + trade);
                    }
                    applyCredit(trade);
                }
                case REJECTED -> throw new IllegalStateException("a rejected trade on the blocks topic: " + trade);
            }
        }
        Map<Integer, Block> creditBlocks = new TreeMap<>();
        credits.forEach((receiver, trades) -> creditBlocks.put(receiver, new Block(trades)));
        return new ProcessedBlock(block, creditBlocks);
    }

    /** The credit phase of the Concurrent model, for a block from the credits topic: every trade is a credit. */
    void applyCredits(Block credits) {
        for (Trade trade : credits.getTrades()) {
            if (trade.getCategory() != Category.RECEIVE) {
                throw new IllegalStateException("not a credit on the credits topic: " + trade);
            }
            applyCredit(trade);
        }
    }

    /** The final balance of every account this batch changed, for localBalance. */
    Map<String, Long> changedBalances() {
        return Collections.unmodifiableMap(changedBalances);
    }

    int blocks() {
        return blocks;
    }

    int accepted() {
        return accepted;
    }

    int rejected() {
        return rejected;
    }

    int credited() {
        return credited;
    }

    private void applyCredit(Trade trade) {
        requireOwnBank(trade.getInbankPartition(), trade.getInAccount(), trade);
        requirePositiveAmount(trade);
        add(trade.getInAccount(), trade.getAmount());
        credited++;
    }

    private long balanceOf(String account) {
        Long balance = balances.get(account);
        if (balance == null) {
            throw new IllegalStateException("no balance for account " + account + " of " + bankId);
        }
        return balance;
    }

    private void add(String account, long amount) {
        long balance = Math.addExact(balanceOf(account), amount);
        balances.put(account, balance);
        changedBalances.put(account, balance);
    }

    /**
     * Every balance has exactly one owner, so a trade for another bank must never reach this one: its
     * partition, and the account it changes here, must be this bank's.
     */
    private void requireOwnBank(int tradePartition, String account, Trade trade) {
        if (tradePartition != partition) {
            throw new IllegalStateException("a trade of partition " + tradePartition + " in a batch of partition "
                    + partition + ": " + trade);
        }
        if (!accounts.contains(account)) {
            throw new IllegalStateException("account " + account + " isn't one of " + bankId + "'s own accounts: "
                    + trade);
        }
    }

    /**
     * The amount must be positive: a negative one would turn the sender's debit into a credit and the
     * receiver's credit into a debit, and a zero one moves no money.
     */
    private static void requirePositiveAmount(Trade trade) {
        if (trade.getAmount() <= 0) {
            throw new IllegalStateException("a trade with an amount of " + trade.getAmount() + ": " + trade);
        }
    }

    /**
     * The receiving half of an accepted trade, on its way to the receiver's bank: a copy with category
     * RECEIVE. (Copied field by field; Avro's deep copy would look the enum class up by reflection.)
     */
    private static Trade creditOf(Trade trade) {
        return Trade.newBuilder()
                .setSerialNumber(trade.getSerialNumber())
                .setOutbank(trade.getOutbank())
                .setOutAccount(trade.getOutAccount())
                .setInbank(trade.getInbank())
                .setInAccount(trade.getInAccount())
                .setOutbankPartition(trade.getOutbankPartition())
                .setInbankPartition(trade.getInbankPartition())
                .setAmount(trade.getAmount())
                .setCategory(Category.RECEIVE)
                .setTimestamp1(trade.getTimestamp1())
                .setTimestamp2(trade.getTimestamp2())
                .build();
    }

    /**
     * A validated block for orders, and the credits of its accepted trades: partition of the receiving
     * bank → a block of credits for that bank.
     */
    record ProcessedBlock(Block order, Map<Integer, Block> credits) {
    }
}
