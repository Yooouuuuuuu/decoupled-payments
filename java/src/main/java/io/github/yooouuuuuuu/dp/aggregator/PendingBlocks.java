package io.github.yooouuuuuuu.dp.aggregator;

import io.github.yooouuuuuuu.dp.avro.Block;
import io.github.yooouuuuuuu.dp.avro.Trade;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * What the aggregator has read but not emitted yet (pure bookkeeping, no Kafka): trades grouped by the
 * blocks partition they go to, and how far each trades partition has been read.
 *
 * <p>Exactly-once rule: a trades offset may be committed only once every trade before it has been emitted.
 * Offsets are per trades partition but the trades are grouped per bank, so all pending blocks are emitted
 * together, in one Kafka transaction with the offsets: see {@link #shouldFlush} and {@link #blocks}.
 */
final class PendingBlocks {
    private final int blockSize;
    private final long blockTimeoutMs;
    /** blocks partition (a bank) → its pending trades, in the order they were read. */
    private final Map<Integer, List<Trade>> tradesByBank = new TreeMap<>();
    /** trades partition → the next offset to read there, i.e. what to commit once everything is emitted. */
    private final Map<Integer, Long> nextOffsets = new TreeMap<>();
    /** When the first record of the current window was read; meaningful only while something is pending. */
    private long windowStartMs;

    PendingBlocks(int blockSize, long blockTimeoutMs) {
        this.blockSize = blockSize;
        this.blockTimeoutMs = blockTimeoutMs;
    }

    /** Adds the trades of a record read from offset {@code offset} of trades partition {@code tradesPartition}. */
    void add(int tradesPartition, long offset, Block block, long nowMs) {
        if (isEmpty()) {
            windowStartMs = nowMs;
        }
        for (Trade trade : block.getTrades()) {
            tradesByBank.computeIfAbsent(targetPartition(trade), partition -> new ArrayList<>()).add(trade);
        }
        nextOffsets.put(tradesPartition, offset + 1);
    }

    /** A new trade is validated at the sender's bank; a credit (Sequential model) is applied at the receiver's. */
    static int targetPartition(Trade trade) {
        return switch (trade.getCategory()) {
            case SEND -> trade.getOutbankPartition();
            case RECEIVE -> trade.getInbankPartition();
            case REJECTED -> throw new IllegalStateException("a rejected trade on the trades topic: " + trade);
        };
    }

    /** The bank that owns {@link #targetPartition}: it is the key of the block. */
    static String targetBank(Trade trade) {
        return switch (trade.getCategory()) {
            case SEND -> trade.getOutbank();
            case RECEIVE -> trade.getInbank();
            case REJECTED -> throw new IllegalStateException("a rejected trade on the trades topic: " + trade);
        };
    }

    /** Emit when any bank has a full block, or when the window has been open for the block timeout. */
    boolean shouldFlush(long nowMs) {
        if (isEmpty()) {
            return false;
        }
        boolean anyBlockFull = tradesByBank.values().stream().anyMatch(trades -> trades.size() >= blockSize);
        return anyBlockFull || nowMs - windowStartMs >= blockTimeoutMs;
    }

    /** How long the next poll may wait: {@code maxMs}, or less if the current window ends sooner. */
    long pollTimeoutMs(long nowMs, long maxMs) {
        if (isEmpty()) {
            return maxMs;
        }
        // From the time elapsed, not from an end time: windowStartMs + blockTimeoutMs could overflow.
        return Math.clamp(blockTimeoutMs - (nowMs - windowStartMs), 0, maxMs);
    }

    /** Every pending trade, as blocks within both limits (see {@link #split}), in partition order. */
    List<OutgoingBlock> blocks() {
        List<OutgoingBlock> blocks = new ArrayList<>();
        tradesByBank.forEach((partition, trades) -> {
            String bank = targetBank(trades.getFirst());
            for (List<Trade> chunk : split(trades, blockSize)) {
                blocks.add(new OutgoingBlock(partition, bank, new Block(chunk)));
            }
        });
        return blocks;
    }

    /**
     * Splits a bank's trades, in order, into blocks of at most {@code blockSize} trades and at most
     * {@link BlockBytes#MAX} bytes: a new block starts when the next trade would break either limit. A single
     * trade always fits.
     */
    static List<List<Trade>> split(List<Trade> trades, int blockSize) {
        List<List<Trade>> blocks = new ArrayList<>();
        List<Trade> block = new ArrayList<>();
        long bytes = BlockBytes.OVERHEAD;
        for (Trade trade : trades) {
            int tradeBytes = BlockBytes.of(trade);
            if (!block.isEmpty() && (block.size() == blockSize || bytes + tradeBytes > BlockBytes.MAX)) {
                blocks.add(block);
                block = new ArrayList<>();
                bytes = BlockBytes.OVERHEAD;
            }
            block.add(trade);
            bytes += tradeBytes;
        }
        if (!block.isEmpty()) {
            blocks.add(block);
        }
        return blocks;
    }

    /** trades partition → next offset to read: committed together with {@link #blocks}. */
    Map<Integer, Long> nextOffsets() {
        return Map.copyOf(nextOffsets);
    }

    int tradeCount() {
        return tradesByBank.values().stream().mapToInt(List::size).sum();
    }

    boolean isEmpty() {
        return nextOffsets.isEmpty();
    }

    void clear() {
        tradesByBank.clear();
        nextOffsets.clear();
    }

    /** A block to send to partition {@code partition} of blocks, with the bank ID as its key. */
    record OutgoingBlock(int partition, String bank, Block block) {
    }
}
