package io.github.yooouuuuuuu.dp.kafka;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;

/**
 * A send callback that remembers the first failure. Sends are asynchronous, so without it a failed send
 * would go unnoticed (a {@code flush()} doesn't throw); the sending thread checks it and fails instead.
 */
public final class SendErrors implements Callback {
    private final AtomicReference<Exception> first = new AtomicReference<>();
    private final AtomicLong failed = new AtomicLong();

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            first.compareAndSet(null, exception);
            failed.incrementAndGet();
        }
    }

    /** @throws KafkaException if any send has failed */
    public void throwIfAny() {
        Exception exception = first.get();
        if (exception != null) {
            throw new KafkaException("a send to Kafka failed (" + failed.get() + " in all): "
                    + exception.getMessage(), exception);
        }
    }
}
