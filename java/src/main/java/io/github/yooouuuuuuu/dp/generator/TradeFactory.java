package io.github.yooouuuuuuu.dp.generator;

import io.github.yooouuuuuuu.dp.avro.Category;
import io.github.yooouuuuuuu.dp.avro.Trade;
import java.util.random.RandomGenerator;

/** Creates the trades of one run of the trade generator. */
final class TradeFactory {
    private final long runId;
    private final AccountPicker picker;
    private final RandomGenerator random;
    private final boolean interBankOnly;
    private final long minAmount;
    private final long maxAmount;
    private long sequence;

    /**
     * @param runId         identifies this run, so serial numbers never repeat across runs
     * @param interBankOnly true when there is more than one bank: the thesis generates inter-bank trades only
     */
    TradeFactory(long runId, AccountPicker picker, RandomGenerator random, boolean interBankOnly, long minAmount,
            long maxAmount) {
        this.runId = runId;
        this.picker = picker;
        this.random = random;
        this.interBankOnly = interBankOnly;
        this.minAmount = minAmount;
        this.maxAmount = maxAmount;
    }

    /** Serial number of the {@code sequence}-th trade (from 1) of a run: runId * 2^32 + sequence. */
    static long serialNumber(long runId, long sequence) {
        return (runId << 32) + sequence;
    }

    Trade next(long nowMs) {
        Account sender = picker.pick(random);
        Account receiver = pickReceiver(sender);
        sequence++;
        return Trade.newBuilder()
                .setSerialNumber(serialNumber(runId, sequence))
                .setOutbank(sender.bank())
                .setOutAccount(sender.id())
                .setInbank(receiver.bank())
                .setInAccount(receiver.id())
                .setOutbankPartition(sender.partition())
                .setInbankPartition(receiver.partition())
                .setAmount(minAmount + random.nextLong(maxAmount - minAmount + 1))
                .setCategory(Category.SEND)
                .setTimestamp1(nowMs)
                .setTimestamp2(0)
                .build();
    }

    /** Picks the receiver like the sender, again until it is another account (at another bank, if required). */
    private Account pickReceiver(Account sender) {
        Account receiver;
        do {
            receiver = picker.pick(random);
        } while (receiver.equals(sender) || (interBankOnly && receiver.partition() == sender.partition()));
        return receiver;
    }
}
