package io.github.yooouuuuuuu.dp.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.yooouuuuuuu.dp.Locales;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class StatsTimerTest {

    @Test
    void secondsHaveOneDecimalWhateverTheLocale() {
        Locales.withEachNonAsciiDefault(() -> {
            assertEquals("5.0", StatsTimer.seconds(5.0));
            assertEquals("5.0", StatsTimer.seconds(4.96));
            assertEquals("12.3", StatsTimer.seconds(12.34));
        });
    }

    @Test
    void eachLineGetsTheRealTimeSinceThePreviousOne() throws InterruptedException {
        List<Double> seconds = new CopyOnWriteArrayList<>();
        CountDownLatch threeLines = new CountDownLatch(3);
        StatsTimer timer = StatsTimer.start(20, elapsed -> {
            seconds.add(elapsed);
            threeLines.countDown();
        });
        try {
            assertTrue(threeLines.await(10, TimeUnit.SECONDS));
        } finally {
            timer.close();
        }
        for (double elapsed : seconds) {
            assertTrue(elapsed > 0.01 && elapsed < 5, elapsed + " s");
        }
    }
}
