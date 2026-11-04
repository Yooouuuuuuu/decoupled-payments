package io.github.yooouuuuuuu.dp.report;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * A minimal JSON writer for the report's one-line summary: maps (in order), lists, strings, numbers, booleans.
 * Numbers are written with {@code String.valueOf}, so their digits never depend on the locale.
 */
final class Json {
    private Json() {
    }

    static String write(Object value) {
        StringBuilder json = new StringBuilder();
        append(json, value);
        return json.toString();
    }

    private static void append(StringBuilder json, Object value) {
        switch (value) {
            case Map<?, ?> map -> {
                json.append('{');
                String separator = "";
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    json.append(separator);
                    appendString(json, entry.getKey().toString());
                    json.append(':');
                    append(json, entry.getValue());
                    separator = ",";
                }
                json.append('}');
            }
            case List<?> list -> {
                json.append('[');
                String separator = "";
                for (Object element : list) {
                    json.append(separator);
                    append(json, element);
                    separator = ",";
                }
                json.append(']');
            }
            case String string -> appendString(json, string);
            case Double number when !Double.isFinite(number) ->
                    throw new IllegalArgumentException("JSON has no " + number);
            case Number number -> json.append(number);
            case Boolean bool -> json.append(bool);
            default -> throw new IllegalArgumentException("can't write " + value.getClass() + " as JSON");
        }
    }

    private static void appendString(StringBuilder json, String string) {
        json.append('"');
        for (char c : string.toCharArray()) {
            switch (c) {
                case '"' -> json.append("\\\"");
                case '\\' -> json.append("\\\\");
                case '\n' -> json.append("\\n");
                case '\r' -> json.append("\\r");
                case '\t' -> json.append("\\t");
                default -> {
                    if (c < 0x20) {
                        json.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        json.append(c);
                    }
                }
            }
        }
        json.append('"');
    }
}
