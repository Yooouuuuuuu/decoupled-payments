package io.github.yooouuuuuuu.dp.cli;

/** The command line isn't valid; the process prints the message and exits with status 2. */
public final class UsageException extends Exception {
    public UsageException(String message) {
        super(message);
    }
}
