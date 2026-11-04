package io.github.yooouuuuuuu.dp.config;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Typed, range-checked access to the merged settings, for building a {@link Config}. It collects every
 * violation instead of stopping at the first, so that one run reports them all; each names the key and the
 * source of its value. A value that breaks a rule is still returned, but {@link #throwIfInvalid} then fails.
 */
final class SettingsReader {
    private final Map<String, Setting> settings;
    private final List<String> problems = new ArrayList<>();
    private final Set<String> invalidKeys = new HashSet<>();

    SettingsReader(Map<String, Setting> settings) {
        this.settings = settings;
    }

    boolean bool(String key) {
        return (Boolean) value(key);
    }

    long integer(String key, long min, long max) {
        long value = (Long) value(key);
        if (value < min) {
            invalid(key, "must be at least " + min + ", but is " + value);
        } else if (value > max) {
            invalid(key, "must be at most " + max + ", but is " + value);
        }
        return value;
    }

    int intValue(String key, int min) {
        return (int) integer(key, min, Integer.MAX_VALUE);
    }

    double number(String key, double min, double max) {
        double value = (Double) value(key);
        if (value < min) {
            invalid(key, "must be at least " + min + ", but is " + value);
        } else if (value > max) {
            invalid(key, "must be at most " + max + ", but is " + value);
        }
        return value;
    }

    /** A setting whose value is one of the enum's constants, written in lowercase. */
    <E extends Enum<E>> E choice(String key, Class<E> type) {
        String value = (String) value(key);
        for (E constant : type.getEnumConstants()) {
            if (lowercase(constant).equals(value)) {
                return constant;
            }
        }
        String choices = Arrays.stream(type.getEnumConstants()).map(SettingsReader::lowercase)
                .collect(Collectors.joining(", "));
        invalid(key, "must be one of " + choices + ", but is '" + value + "'");
        return null;
    }

    /** A comma-separated list of brokers ({@code host:port}): at least one, and no empty entry. */
    String brokers(String key) {
        String value = (String) value(key);
        // -1 keeps trailing empty entries: "kafka-1:19092," has an empty second entry.
        boolean anyEmpty = Arrays.stream(value.split(",", -1)).anyMatch(entry -> entry.isBlank());
        if (anyEmpty) {
            invalid(key, "must name at least one broker, as host:port separated by commas, with no empty entry, "
                    + "but is '" + value + "'");
        }
        return value;
    }

    /** {@code key} must be at least the value of {@code lowerKey} (checked only if both are valid). */
    void requireAtLeast(String key, String lowerKey) {
        if (allValid(key, lowerKey) && (Long) value(key) < (Long) value(lowerKey)) {
            invalid(key, "must be at least " + lowerKey + " (" + value(lowerKey) + "), but is " + value(key));
        }
    }

    /**
     * The product of the keys' values must be at most {@code max}, computed exactly, so without overflow
     * (checked only if every key is valid).
     */
    void requireProductAtMost(long max, String why, String... keys) {
        if (!allValid(keys)) {
            return;
        }
        BigInteger product = Arrays.stream(keys).map(key -> BigInteger.valueOf((Long) value(key)))
                .reduce(BigInteger.ONE, BigInteger::multiply);
        if (product.compareTo(BigInteger.valueOf(max)) > 0) {
            String names = String.join(" * ", keys);
            String values = Arrays.stream(keys).map(key -> String.valueOf(value(key)))
                    .collect(Collectors.joining(" * "));
            String sources = Arrays.stream(keys).map(key -> key + ": " + source(key))
                    .collect(Collectors.joining(", "));
            problems.add(names + " must be at most " + max + " (" + why + "), but is " + values + " (" + sources + ")");
        }
    }

    /** @throws ConfigException listing every violation found, if there was any */
    void throwIfInvalid() throws ConfigException {
        if (!problems.isEmpty()) {
            throw new ConfigException(problems);
        }
    }

    private boolean allValid(String... keys) {
        return Arrays.stream(keys).noneMatch(invalidKeys::contains);
    }

    private void invalid(String key, String problem) {
        invalidKeys.add(key);
        problems.add(key + " " + problem + " (" + source(key) + ")");
    }

    private Object value(String key) {
        return setting(key).value();
    }

    private String source(String key) {
        return setting(key).source();
    }

    private Setting setting(String key) {
        Setting setting = settings.get(key);
        if (setting == null) {
            throw new IllegalStateException(key + " is missing from default.yaml");
        }
        return setting;
    }

    private static String lowercase(Enum<?> constant) {
        return constant.name().toLowerCase(Locale.ROOT);
    }
}
