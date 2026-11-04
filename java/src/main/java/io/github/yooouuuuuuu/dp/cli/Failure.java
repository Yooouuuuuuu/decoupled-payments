package io.github.yooouuuuuuu.dp.cli;

import java.util.Optional;

/**
 * A runtime failure whose message says everything the user needs (for example "run init first"), so it
 * is logged without a stack trace. The process exits with status 1.
 */
public final class Failure extends RuntimeException {
    public Failure(String message) {
        super(message);
    }

    /**
     * The failure that caused {@code error}, if any. Kafka wraps an exception thrown by a rebalance callback
     * ("User rebalance callback throws an error"), so a failure during a takeover arrives as the cause.
     */
    public static Optional<Failure> in(Throwable error) {
        for (Throwable e = error; e != null; e = e.getCause()) {
            if (e instanceof Failure failure) {
                return Optional.of(failure);
            }
        }
        return Optional.empty();
    }
}
