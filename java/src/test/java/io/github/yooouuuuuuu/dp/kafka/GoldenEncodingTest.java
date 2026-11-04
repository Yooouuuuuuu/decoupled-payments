package io.github.yooouuuuuuu.dp.kafka;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.yooouuuuuuu.dp.avro.AccountInfo;
import io.github.yooouuuuuuu.dp.avro.Block;
import io.github.yooouuuuuuu.dp.avro.Category;
import io.github.yooouuuuuuu.dp.avro.LocalBalance;
import io.github.yooouuuuuuu.dp.avro.Model;
import io.github.yooouuuuuuu.dp.avro.Trade;
import io.github.yooouuuuuuu.dp.avro.ValidatorOffset;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.specific.SpecificRecord;
import org.junit.jupiter.api.Test;

/**
 * The golden single-object encodings in schemas/testdata, shared with the Go implementation: both must
 * read and write exactly these bytes.
 */
class GoldenEncodingTest {

    static final Block BLOCK = new Block(List.of(
            new Trade(4294967297L, "bank0", "bank0-0007", "bank2", "bank2-0042", 0, 2, 1250L, Category.SEND,
                    1730700000000L, 0L),
            new Trade(4294967298L, "bank1", "bank1-0003", "bank0", "bank0-0099", 1, 0, 7L, Category.REJECTED,
                    1730700000123L, 1730700000456L)));
    static final LocalBalance LOCAL_BALANCE = new LocalBalance(987654321L);
    static final AccountInfo ACCOUNT_INFO = new AccountInfo("bank1", 1,
            List.of("bank1-0000", "bank1-0001", "bank1-0002"), 10000L, Model.CONCURRENT, true);
    static final ValidatorOffset VALIDATOR_OFFSET = new ValidatorOffset("credits", 2, 12345L);

    @Test
    void decodesToTheDocumentedValues() {
        assertEquals(BLOCK, AvroSerde.blocks().deserialize(Topics.BLOCKS, golden("block.bin")));
        assertEquals(LOCAL_BALANCE,
                AvroSerde.localBalances().deserialize(Topics.LOCAL_BALANCE, golden("localBalance.bin")));
        assertEquals(ACCOUNT_INFO,
                AvroSerde.accountInfos().deserialize(Topics.ACCOUNT_INFO, golden("accountInfo.bin")));
        assertEquals(VALIDATOR_OFFSET,
                AvroSerde.validatorOffsets().deserialize(Topics.VALIDATOR_OFFSETS, golden("validatorOffset.bin")));
    }

    @Test
    void encodesToTheSameBytes() {
        assertEncodes("block.bin", BLOCK);
        assertEncodes("localBalance.bin", LOCAL_BALANCE);
        assertEncodes("accountInfo.bin", ACCOUNT_INFO);
        assertEncodes("validatorOffset.bin", VALIDATOR_OFFSET);
    }

    private static void assertEncodes(String file, SpecificRecord value) {
        assertArrayEquals(golden(file), AvroSerde.serializer().serialize("any", value), file);
    }

    @Test
    void fingerprintsMatchTheListAndTheHeaders() {
        Map<String, Schema> schemas = Map.of(
                "block", Block.getClassSchema(),
                "localBalance", LocalBalance.getClassSchema(),
                "accountInfo", AccountInfo.getClassSchema(),
                "validatorOffset", ValidatorOffset.getClassSchema());
        Map<String, Long> listed = new HashMap<>();
        for (String line : new String(golden("fingerprints.txt"), StandardCharsets.UTF_8).strip().split("\n")) {
            String[] fields = line.strip().split("\\s+");
            assertEquals(schemas.get(fields[0]).getFullName(), fields[1]);
            listed.put(fields[0], Long.parseUnsignedLong(fields[2].substring(2), 16));
        }
        assertEquals(schemas.keySet(), listed.keySet());

        schemas.forEach((name, schema) -> {
            assertEquals(new String(golden(name + ".canonical.json"), StandardCharsets.UTF_8).strip(),
                    SchemaNormalization.toParsingForm(schema), name);
            long fingerprint = SchemaNormalization.parsingFingerprint64(schema);
            assertEquals(listed.get(name), fingerprint, name);

            // Single-object header: C3 01, then the fingerprint, little-endian.
            ByteBuffer header = ByteBuffer.wrap(golden(name + ".bin")).order(ByteOrder.LITTLE_ENDIAN);
            assertEquals((byte) 0xC3, header.get());
            assertEquals((byte) 0x01, header.get());
            assertEquals(fingerprint, header.getLong(), name);
        });
    }

    static byte[] golden(String file) {
        try (InputStream in = GoldenEncodingTest.class.getResourceAsStream("/testdata/" + file)) {
            if (in == null) {
                throw new IllegalStateException("missing test resource testdata/" + file);
            }
            return in.readAllBytes();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
