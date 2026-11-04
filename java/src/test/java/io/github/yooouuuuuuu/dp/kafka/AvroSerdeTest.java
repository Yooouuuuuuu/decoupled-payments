package io.github.yooouuuuuuu.dp.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.yooouuuuuuu.dp.avro.Block;
import io.github.yooouuuuuuu.dp.avro.LocalBalance;
import java.util.List;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

class AvroSerdeTest {

    @Test
    void roundTrip() {
        byte[] bytes = AvroSerde.serializer().serialize(Topics.LOCAL_BALANCE, new LocalBalance(-3L));
        assertEquals(new LocalBalance(-3L), AvroSerde.localBalances().deserialize(Topics.LOCAL_BALANCE, bytes));
    }

    @Test
    void valueOfAnotherSchemaIsRejected() {
        byte[] balance = GoldenEncodingTest.golden("localBalance.bin");
        SerializationException e = assertThrows(SerializationException.class,
                () -> AvroSerde.blocks().deserialize(Topics.BLOCKS, balance));
        assertTrue(e.getMessage().contains("topic blocks") && e.getMessage().contains("Block"), e.getMessage());
    }

    @Test
    void valueWithoutTheHeaderIsRejected() {
        assertThrows(SerializationException.class,
                () -> AvroSerde.blocks().deserialize(Topics.BLOCKS, new byte[] {0x02, 0x04, 0x06}));
        assertThrows(SerializationException.class, () -> AvroSerde.blocks().deserialize(Topics.BLOCKS, null));
    }

    @Test
    void anEmptyBlockIsValid() {
        Block empty = new Block(List.of());
        byte[] bytes = AvroSerde.serializer().serialize(Topics.BLOCKS, empty);
        assertEquals(empty, AvroSerde.blocks().deserialize(Topics.BLOCKS, bytes));
    }
}
