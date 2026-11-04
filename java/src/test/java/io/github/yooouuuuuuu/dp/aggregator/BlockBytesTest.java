package io.github.yooouuuuuuu.dp.aggregator;

import static io.github.yooouuuuuuu.dp.TestTrades.send;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.yooouuuuuuu.dp.TestTrades;
import io.github.yooouuuuuuu.dp.avro.Block;
import io.github.yooouuuuuuu.dp.avro.Category;
import io.github.yooouuuuuuu.dp.avro.Trade;
import io.github.yooouuuuuuu.dp.kafka.AvroSerde;
import io.github.yooouuuuuuu.dp.kafka.Topics;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BlockBytesTest {
    /** A timestamp2 whose zigzag varint has 10 bytes, the most a long can take. */
    private static final long LONGEST_TIMESTAMP2 = Long.MIN_VALUE;

    private static int encodedBytes(List<Trade> trades) {
        return AvroSerde.serializer().serialize(Topics.BLOCKS, new Block(trades)).length;
    }

    private static List<Trade> variedTrades() {
        List<Trade> trades = new ArrayList<>();
        trades.add(send(1, "bank0-0000", "bank1-0000", 1));
        trades.add(send(4_294_967_297L * 2_147_483_647L, "bank2-0042", "bank12-12345", Long.MAX_VALUE));
        trades.add(TestTrades.trade(-1, "bank1-0007", "bank0-0099", -64, Category.REJECTED));
        Trade unicode = send(Long.MIN_VALUE, "bank3-0001", "bank0-0002", 63);
        unicode.setOutAccount("bänk3-ünïcödé-€-𝄞");
        unicode.setOutbankPartition(Integer.MAX_VALUE);
        unicode.setInbankPartition(Integer.MIN_VALUE);
        unicode.setTimestamp1(1_730_700_000_000L);
        trades.add(unicode);
        Trade longStrings = send(64, "bank0-0001", "bank1-0001", 8_192);
        longStrings.setInAccount("x".repeat(200));
        longStrings.setCategory(Category.RECEIVE);
        trades.add(longStrings);
        return trades;
    }

    @Test
    void aTradesSizeIsItsExactAvroSizeWithTheLongestTimestamp2() {
        for (Trade trade : variedTrades()) {
            int computed = BlockBytes.of(trade);
            // As the validator writes it to orders, with a timestamp2 of the largest size.
            trade.setTimestamp2(LONGEST_TIMESTAMP2);
            // A block of one trade: 10 header bytes, the item count 1 (one byte), the trade, the end marker 0.
            assertEquals(encodedBytes(List.of(trade)) - 12, computed, trade.toString());
        }
    }

    @Test
    void aBlocksSizeIsAtMostTheSumOfItsTradesPlusTheOverhead() {
        List<Trade> trades = variedTrades();
        int computed = trades.stream().mapToInt(BlockBytes::of).sum() + BlockBytes.OVERHEAD;
        trades.forEach(trade -> trade.setTimestamp2(LONGEST_TIMESTAMP2));
        int actual = encodedBytes(trades);
        assertTrue(actual <= computed, actual + " > " + computed);
        assertTrue(computed - actual <= 6, "only the array framing is estimated");
    }
}
