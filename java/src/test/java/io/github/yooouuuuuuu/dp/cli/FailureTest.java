package io.github.yooouuuuuuu.dp.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;

class FailureTest {

    @Test
    void aFailureIsFoundAlsoWhenARebalanceCallbackWrappedIt() {
        Failure failure = new Failure("localBalance has no balance for some accounts of bank0: run init --reset");
        assertEquals(Optional.of(failure), Failure.in(failure));
        assertEquals(Optional.of(failure),
                Failure.in(new KafkaException("User rebalance callback throws an error", failure)));
        assertEquals(Optional.empty(), Failure.in(new IllegalStateException("a bug", new RuntimeException())));
    }
}
