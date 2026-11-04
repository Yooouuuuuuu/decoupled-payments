package io.github.yooouuuuuuu.dp.config;

/**
 * A command-line flag: {@code --name=value}, or {@code --name} alone (then {@code value} is null,
 * which means {@code true} for a boolean setting).
 */
public record Flag(String name, String value) {
}
