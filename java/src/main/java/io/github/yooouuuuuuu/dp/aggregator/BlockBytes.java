package io.github.yooouuuuuuu.dp.aggregator;

import io.github.yooouuuuuuu.dp.avro.Trade;
import java.nio.charset.StandardCharsets;

/**
 * The size of a block on Kafka, computed from its trades without encoding them. A block is kept at most
 * {@link #MAX} bytes, so that the block and every record a validator derives from it stay well under Kafka's
 * 1 MiB message limit: the orders record has the same trades with timestamp2 set (about 9% larger than the
 * block was), and the credits are subsets of them. The Go implementation computes the same sizes, so both
 * cut the same blocks.
 */
final class BlockBytes {
    /** 512 KiB. */
    static final int MAX = 512 * 1024;
    /** The single-object header (2 marker bytes, an 8-byte fingerprint) and the array's framing (at most 6). */
    static final int OVERHEAD = 16;
    /** timestamp2 at its largest, because the validator sets it after the block is made. */
    private static final int TIMESTAMP2_BYTES = 10;

    private BlockBytes() {
    }

    /** The exact Avro binary size of the trade, with timestamp2 counted at its largest. */
    static int of(Trade trade) {
        return longBytes(trade.getSerialNumber())
                + stringBytes(trade.getOutbank())
                + stringBytes(trade.getOutAccount())
                + stringBytes(trade.getInbank())
                + stringBytes(trade.getInAccount())
                + longBytes(trade.getOutbankPartition())
                + longBytes(trade.getInbankPartition())
                + longBytes(trade.getAmount())
                + 1 // category: the index of an enum symbol, 0 to 2
                + longBytes(trade.getTimestamp1())
                + TIMESTAMP2_BYTES;
    }

    /** A string is its length in bytes, then its UTF-8 bytes. */
    private static int stringBytes(String text) {
        int length = text.getBytes(StandardCharsets.UTF_8).length;
        return longBytes(length) + length;
    }

    /**
     * Avro writes an int or a long zigzag-encoded (so that small negative numbers are short too) as a varint
     * of 7 bits per byte. An int has the same size as the long of the same value.
     */
    private static int longBytes(long value) {
        long zigzag = (value << 1) ^ (value >> 63);
        int bytes = 1;
        while ((zigzag >>>= 7) != 0) {
            bytes++;
        }
        return bytes;
    }
}
