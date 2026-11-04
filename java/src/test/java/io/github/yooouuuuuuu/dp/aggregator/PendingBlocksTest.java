package io.github.yooouuuuuuu.dp.aggregator;

import static io.github.yooouuuuuuu.dp.TestTrades.block;
import static io.github.yooouuuuuuu.dp.TestTrades.credit;
import static io.github.yooouuuuuuu.dp.TestTrades.send;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.yooouuuuuuu.dp.TestTrades;
import io.github.yooouuuuuuu.dp.aggregator.PendingBlocks.OutgoingBlock;
import io.github.yooouuuuuuu.dp.avro.Block;
import io.github.yooouuuuuuu.dp.avro.Category;
import io.github.yooouuuuuuu.dp.avro.Trade;
import io.github.yooouuuuuuu.dp.kafka.AvroSerde;
import io.github.yooouuuuuuu.dp.kafka.Topics;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PendingBlocksTest {

    @Test
    void newTradesGoToTheSendersBankAndCreditsToTheReceivers() {
        PendingBlocks pending = new PendingBlocks(10, 1000);
        pending.add(0, 5, block(send(1, "bank0-0001", "bank1-0001", 10)), 0);
        pending.add(1, 7, block(credit(2, "bank0-0001", "bank2-0003", 20)), 0);

        List<OutgoingBlock> blocks = pending.blocks();
        assertEquals(2, blocks.size());
        assertEquals(0, blocks.get(0).partition());
        assertEquals("bank0", blocks.get(0).bank());
        assertEquals(2, blocks.get(1).partition());
        assertEquals("bank2", blocks.get(1).bank());
        assertEquals(1, blocks.get(0).block().getTrades().getFirst().getSerialNumber());
    }

    @Test
    void rejectedTradesNeverAppearOnTrades() {
        PendingBlocks pending = new PendingBlocks(10, 1000);
        Trade rejected = TestTrades.trade(1, "bank0-0001", "bank1-0001", 10, Category.REJECTED);
        assertThrows(IllegalStateException.class, () -> pending.add(0, 0, block(rejected), 0));
    }

    @Test
    void flushesWhenAnyBankHasAFullBlock() {
        PendingBlocks pending = new PendingBlocks(3, 1000);
        pending.add(0, 0, block(send(1, "bank0-0001", "bank1-0001", 1)), 0);
        pending.add(0, 1, block(send(2, "bank1-0001", "bank0-0001", 1)), 0);
        pending.add(0, 2, block(send(3, "bank0-0002", "bank1-0001", 1)), 0);
        assertFalse(pending.shouldFlush(0));
        pending.add(0, 3, block(send(4, "bank0-0003", "bank1-0001", 1)), 0);
        assertTrue(pending.shouldFlush(0), "bank0 has 3 trades");
    }

    @Test
    void flushesWhenTheWindowTimesOut() {
        PendingBlocks pending = new PendingBlocks(100, 1000);
        assertFalse(pending.shouldFlush(5000), "nothing pending");
        pending.add(0, 0, block(send(1, "bank0-0001", "bank1-0001", 1)), 2000);
        pending.add(1, 0, block(send(2, "bank1-0001", "bank0-0001", 1)), 2900);
        assertFalse(pending.shouldFlush(2999));
        assertTrue(pending.shouldFlush(3000), "1000 ms after the first trade of the window");
    }

    @Test
    void pollWaitsAtMostUntilTheWindowEnds() {
        PendingBlocks pending = new PendingBlocks(100, 1000);
        assertEquals(100, pending.pollTimeoutMs(0, 100));
        pending.add(0, 0, block(send(1, "bank0-0001", "bank1-0001", 1)), 1000);
        assertEquals(100, pending.pollTimeoutMs(1000, 100));
        assertEquals(40, pending.pollTimeoutMs(1960, 100));
        assertEquals(0, pending.pollTimeoutMs(2500, 100));
    }

    @Test
    void aHugeBlockTimeoutNeverOverflows() {
        PendingBlocks pending = new PendingBlocks(100, Long.MAX_VALUE);
        pending.add(0, 0, block(send(1, "bank0-0001", "bank1-0001", 1)), 5_000_000);
        assertEquals(100, pending.pollTimeoutMs(5_000_100, 100), "no busy polling with a timeout of 0");
        assertFalse(pending.shouldFlush(Long.MAX_VALUE / 2));
    }

    @Test
    void blocksHoldAtMostBlockSizeTrades() {
        PendingBlocks pending = new PendingBlocks(2, 1000);
        for (int i = 0; i < 5; i++) {
            pending.add(0, i, block(send(i, "bank1-0001", "bank0-0001", 1)), 0);
        }
        List<OutgoingBlock> blocks = pending.blocks();
        assertEquals(List.of(2, 2, 1), blocks.stream().map(b -> b.block().getTrades().size()).toList());
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L), blocks.stream()
                .flatMap(b -> b.block().getTrades().stream()).map(Trade::getSerialNumber).toList());
        assertEquals(5, pending.tradeCount());
    }

    /** Trades of the same encoded size, with long account names so that a few hundred fill 512 KiB. */
    private static List<Trade> bigTrades(int count) {
        List<Trade> trades = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Trade trade = send((1L << 20) + i, "bank1-0001", "bank0-0001", 1);
            trade.setInAccount("bank0-" + "x".repeat(1000));
            trades.add(trade);
        }
        return trades;
    }

    @Test
    void blocksStayWithin512KiBAlsoWithTimestamp2Set() {
        List<Trade> trades = bigTrades(1200);
        int tradeBytes = BlockBytes.of(trades.getFirst());
        int perBlock = (BlockBytes.MAX - BlockBytes.OVERHEAD) / tradeBytes;

        List<List<Trade>> blocks = PendingBlocks.split(trades, 100_000);
        assertEquals(List.of(perBlock, perBlock, 1200 - 2 * perBlock), blocks.stream().map(List::size).toList());
        for (List<Trade> blockTrades : blocks) {
            blockTrades.forEach(trade -> trade.setTimestamp2(Long.MIN_VALUE)); // its largest size, as on orders
            int encoded = AvroSerde.serializer().serialize(Topics.ORDERS, new Block(blockTrades)).length;
            assertTrue(encoded <= BlockBytes.MAX, encoded + " bytes");
        }
        assertTrue(BlockBytes.OVERHEAD + (perBlock + 1) * tradeBytes > BlockBytes.MAX, "one more would not fit");
    }

    @Test
    void theSmallerOfTheTwoLimitsCutsTheBlocks() {
        assertEquals(List.of(100, 100, 100),
                PendingBlocks.split(bigTrades(300), 100).stream().map(List::size).toList());
        int perBlock = (BlockBytes.MAX - BlockBytes.OVERHEAD) / BlockBytes.of(bigTrades(1).getFirst());
        assertEquals(List.of(perBlock, 1), PendingBlocks.split(bigTrades(perBlock + 1), perBlock + 1).stream()
                .map(List::size).toList());
    }

    @Test
    void aSingleTradeAlwaysFits() {
        Trade huge = send(1, "bank1-0001", "bank0-0001", 1);
        huge.setInAccount("bank0-" + "x".repeat(BlockBytes.MAX));
        Trade small = send(2, "bank1-0001", "bank0-0002", 1);
        assertEquals(List.of(List.of(huge), List.of(small)), PendingBlocks.split(List.of(huge, small), 10));
    }

    @Test
    void offsetsToCommitAreTheNextOffsetOfEveryTradesPartitionRead() {
        PendingBlocks pending = new PendingBlocks(10, 1000);
        pending.add(0, 5, block(send(1, "bank0-0001", "bank1-0001", 1)), 0);
        pending.add(2, 9, block(send(2, "bank2-0001", "bank1-0001", 1)), 0);
        pending.add(0, 6, block(send(3, "bank1-0001", "bank0-0001", 1)), 0);
        assertEquals(Map.of(0, 7L, 2, 10L), pending.nextOffsets());
    }

    @Test
    void clearStartsANewWindow() {
        PendingBlocks pending = new PendingBlocks(10, 1000);
        pending.add(0, 5, block(send(1, "bank0-0001", "bank1-0001", 1)), 0);
        pending.clear();
        assertTrue(pending.isEmpty());
        assertEquals(List.of(), pending.blocks());
        assertEquals(Map.of(), pending.nextOffsets());
        pending.add(0, 6, block(send(2, "bank0-0001", "bank1-0001", 1)), 5000);
        assertFalse(pending.shouldFlush(5999), "the window starts with the first trade after clear");
        assertTrue(pending.shouldFlush(6000));
    }
}
