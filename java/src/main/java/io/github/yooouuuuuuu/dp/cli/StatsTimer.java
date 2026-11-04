package io.github.yooouuuuuuu.dp.cli;

import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleConsumer;

/**
 * Logs a role's progress every statsIntervalMs from a thread of its own, so that the lines keep coming while
 * the role's loop is busy or waits (for a takeover, say), and each line covers the real time since the
 * previous one. After a pause (a long GC, a SIGSTOP) the next line covers the whole pause, and the one after
 * comes an interval later: missed lines aren't made up for.
 */
public final class StatsTimer implements AutoCloseable {
    private final ScheduledExecutorService timer;
    private long previousNanos = System.nanoTime(); // from now on only used on the timer's thread

    private StatsTimer(long intervalMs, DoubleConsumer line) {
        timer = Executors.newSingleThreadScheduledExecutor(
                task -> Thread.ofPlatform().daemon().name("stats").unstarted(task));
        timer.scheduleWithFixedDelay(() -> {
            long now = System.nanoTime();
            line.accept((now - previousNanos) / 1e9);
            previousNanos = now;
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    /** @param line writes one stats line, given the seconds since the previous one (or since the start) */
    public static StatsTimer start(long intervalMs, DoubleConsumer line) {
        return new StatsTimer(intervalMs, line);
    }

    /** Seconds with one decimal, as the stats lines show them ({@code 5.0}), whatever the locale. */
    public static String seconds(double seconds) {
        return String.format(Locale.ROOT, "%.1f", seconds);
    }

    @Override
    public void close() {
        timer.shutdownNow();
    }
}
