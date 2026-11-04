package io.github.yooouuuuuuu.dp.config;

import java.util.regex.Pattern;

/**
 * The type of a setting. It is the type of the setting's value in default.yaml, and the value from every
 * source (default.yaml, a config file, an environment variable, a flag) is parsed as text to that type, by
 * the same strict rules in the Java and the Go implementation.
 */
enum ValueType {
    INTEGER("an integer"),
    FLOAT("a number"),
    BOOLEAN("true or false"),
    STRING("a string");

    // Plain decimal notation only: no '+', hex, octal, underscores, NaN or Infinity, and leading zeros are
    // still decimal (010 is ten), so that every source and both implementations read a value the same way.
    private static final Pattern INTEGER_TEXT = Pattern.compile("-?[0-9]+");
    private static final Pattern FLOAT_TEXT = Pattern.compile("-?[0-9]+(\\.[0-9]+)?([eE][+-]?[0-9]+)?");

    private final String description;

    ValueType(String description) {
        this.description = description;
    }

    /**
     * Parses text to this type: integers to {@code Long}, numbers to {@code Double}, booleans to
     * {@code Boolean}; strings stay as they are.
     *
     * @throws IllegalArgumentException with a short reason, if the text isn't a value of this type
     */
    Object parse(String text) {
        return switch (this) {
            case INTEGER -> parseInteger(text);
            case FLOAT -> parseFloat(text);
            case BOOLEAN -> parseBoolean(text);
            case STRING -> text;
        };
    }

    String description() {
        return description;
    }

    private Long parseInteger(String text) {
        if (!INTEGER_TEXT.matcher(text).matches()) {
            throw invalid(text);
        }
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("'" + text + "' is out of the 64-bit integer range");
        }
    }

    private Double parseFloat(String text) {
        if (!FLOAT_TEXT.matcher(text).matches()) {
            throw invalid(text);
        }
        double value = Double.parseDouble(text);
        if (Double.isInfinite(value)) {
            throw new IllegalArgumentException("'" + text + "' is too large");
        }
        return value;
    }

    private Boolean parseBoolean(String text) {
        return switch (text) {
            case "true" -> true;
            case "false" -> false;
            default -> throw invalid(text);
        };
    }

    private IllegalArgumentException invalid(String text) {
        return new IllegalArgumentException("expected " + description + ", got '" + text + "'");
    }
}
