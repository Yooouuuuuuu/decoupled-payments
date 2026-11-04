package io.github.yooouuuuuuu.dp.config;

import java.util.List;

/**
 * Settings are unknown or have bad values. Each problem names the key and where its value came from; the
 * validation of the merged settings reports every problem it finds, not just the first.
 */
public final class ConfigException extends Exception {
    private final List<String> problems;

    public ConfigException(String problem) {
        this(List.of(problem));
    }

    public ConfigException(List<String> problems) {
        super(String.join("; ", problems));
        this.problems = List.copyOf(problems);
    }

    /** One line per problem. */
    public List<String> problems() {
        return problems;
    }
}
