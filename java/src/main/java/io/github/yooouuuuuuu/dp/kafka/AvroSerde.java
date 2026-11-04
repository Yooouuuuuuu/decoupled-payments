package io.github.yooouuuuuuu.dp.kafka;

import io.github.yooouuuuuuu.dp.avro.AccountInfo;
import io.github.yooouuuuuuu.dp.avro.Block;
import io.github.yooouuuuuuu.dp.avro.Category;
import io.github.yooouuuuuuu.dp.avro.LocalBalance;
import io.github.yooouuuuuuu.dp.avro.Model;
import io.github.yooouuuuuuu.dp.avro.Trade;
import io.github.yooouuuuuuu.dp.avro.ValidatorOffset;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.ClassSecurityValidator;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Kafka (de)serializers for record values in Avro single-object encoding: the marker {@code C3 01}, the
 * schema's 8-byte CRC-64-AVRO fingerprint (little-endian), then the Avro binary payload. The fingerprint
 * tells the reader which schema wrote the value, so no Schema Registry is needed (docs/data-model).
 */
public final class AvroSerde {
    static {
        // When decoding, Avro creates the classes named by the schema through reflection, and it only creates
        // classes it has been told to trust. Trust exactly our generated ones.
        ClassSecurityValidator.setGlobal(ClassSecurityValidator.composite(ClassSecurityValidator.getGlobal(),
                ClassSecurityValidator.builder()
                        .add(Block.class).add(Trade.class).add(Category.class)
                        .add(LocalBalance.class)
                        .add(AccountInfo.class).add(Model.class)
                        .add(ValidatorOffset.class)
                        .build()));
    }

    private AvroSerde() {
    }

    /** One serializer for all our value types, so a producer can write blocks, balances and offsets. */
    public static Serializer<SpecificRecord> serializer() {
        return (topic, value) -> {
            try {
                ByteBuffer encoded = switch (value) {
                    case Block block -> block.toByteBuffer();
                    case LocalBalance balance -> balance.toByteBuffer();
                    case AccountInfo info -> info.toByteBuffer();
                    case ValidatorOffset offset -> offset.toByteBuffer();
                    default -> throw new SerializationException("no encoder for " + value.getClass().getName());
                };
                byte[] bytes = new byte[encoded.remaining()];
                encoded.get(bytes);
                return bytes;
            } catch (IOException e) {
                throw new SerializationException("cannot encode a value for topic " + topic, e);
            }
        };
    }

    public static Deserializer<Block> blocks() {
        return deserializer("Block", Block.getDecoder());
    }

    public static Deserializer<LocalBalance> localBalances() {
        return deserializer("LocalBalance", LocalBalance.getDecoder());
    }

    public static Deserializer<AccountInfo> accountInfos() {
        return deserializer("AccountInfo", AccountInfo.getDecoder());
    }

    public static Deserializer<ValidatorOffset> validatorOffsets() {
        return deserializer("ValidatorOffset", ValidatorOffset.getDecoder());
    }

    /**
     * A value with a wrong header or an unknown fingerprint is fatal: the consumer's poll then throws a
     * {@code RecordDeserializationException} that names the partition and offset of the record.
     */
    private static <T> Deserializer<T> deserializer(String type, BinaryMessageDecoder<T> decoder) {
        return (topic, data) -> {
            if (data == null) {
                throw new SerializationException("unexpected null value on topic " + topic);
            }
            try {
                return decoder.decode(data);
            } catch (IOException | AvroRuntimeException e) {
                throw new SerializationException("a value on topic " + topic + " is not a single-object encoded "
                        + type + ": " + e.getMessage(), e);
            }
        };
    }
}
