package io.github.yooouuuuuuu.dp.generator;

/** The trade generator's schedule: trade i (counting from 0) is due at start + i / rps. */
final class Pacing {
    private Pacing() {
    }

    /** How many trades are due once {@code elapsedNanos} have passed since the start. */
    static long dueAfter(long elapsedNanos, long rps) {
        long beforeNow = (long) (elapsedNanos / 1e9 * rps); // saturates at Long.MAX_VALUE for a huge rps
        return beforeNow == Long.MAX_VALUE ? beforeNow : beforeNow + 1;
    }
}
