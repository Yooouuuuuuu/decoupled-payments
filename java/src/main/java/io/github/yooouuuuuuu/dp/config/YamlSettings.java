package io.github.yooouuuuuuu.dp.config;

import java.io.StringReader;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.Mark;
import org.yaml.snakeyaml.error.MarkedYAMLException;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.SequenceNode;
import org.yaml.snakeyaml.nodes.Tag;

/**
 * Reads YAML files of settings as a tree of nodes, and keeps every value as the text written in the file.
 * Nested sections become dotted keys: {@code kafka: {bootstrapServers: x}} is {@code kafka.bootstrapServers}.
 *
 * <p>The YAML library never decides what a value means. SnakeYAML follows YAML 1.1, where {@code yes},
 * {@code 010} and {@code 1_000} are true, 8 and 1000, while Go's YAML library (YAML 1.2), environment
 * variables and flags read them differently. So each text is parsed by the setting's {@link ValueType}, the
 * same way for every source and in both implementations.
 */
final class YamlSettings {
    private YamlSettings() {
    }

    /** A setting of default.yaml: its type (the kind of value YAML sees in it) and its value as written there. */
    record Default(ValueType type, String text) {
    }

    /** Every setting of default.yaml, in file order. A file that breaks these rules is a bug of the build. */
    static Map<String, Default> readDefaults(String yaml, String source) {
        Map<String, Default> defaults = new LinkedHashMap<>();
        try {
            Node document = singleDocument(yaml, source)
                    .orElseThrow(() -> new ConfigException(source + " has no settings"));
            addDefaults(document, "", source, defaults);
        } catch (ConfigException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        return defaults;
    }

    /**
     * The settings of a config file, which may only use the keys of default.yaml (and their sections). An
     * empty file, or one with only comments or only {@code ~}, sets nothing.
     *
     * @throws ConfigException naming the key and the file: for a key without a value ({@code ~},
     *                         {@code null} or nothing), a list, a section where a value is expected or the
     *                         other way round, an unknown or a duplicate key, an anchor or alias, more than
     *                         one document
     */
    static Map<String, String> readOverrides(String yaml, String source, Set<String> keys) throws ConfigException {
        Optional<Node> document = singleDocument(yaml, source);
        if (document.isEmpty() || isNull(document.get())) {
            return Map.of();
        }
        Map<String, String> values = new LinkedHashMap<>();
        addOverrides(plain("the top level", document.get(), source), "", source, keys, values);
        return values;
    }

    private static void addDefaults(Node section, String name, String source, Map<String, Default> defaults)
            throws ConfigException {
        for (Map.Entry<String, Node> entry : entries(section, name, source).entrySet()) {
            String key = entry.getKey();
            Node value = plain(key, entry.getValue(), source);
            if (value instanceof MappingNode) {
                addDefaults(value, key, source, defaults);
            } else {
                String text = text(key, value, source);
                if (defaults.put(key, new Default(typeOf(key, value), text)) != null) {
                    throw duplicate(key, source);
                }
            }
        }
    }

    private static void addOverrides(Node section, String name, String source, Set<String> keys,
            Map<String, String> values) throws ConfigException {
        for (Map.Entry<String, Node> entry : entries(section, name, source).entrySet()) {
            String key = entry.getKey();
            Node value = plain(key, entry.getValue(), source);
            if (keys.contains(key)) {
                // Also catches a key written both nested and dotted (kafka: {x: 1} and kafka.x: 2).
                if (values.put(key, text(key, value, source)) != null) {
                    throw duplicate(key, source);
                }
            } else if (isSection(key, keys)) {
                addOverrides(value, key, source, keys, values);
            } else {
                throw new ConfigException("unknown setting " + key + " in " + source);
            }
        }
    }

    /** The entries of a section (dotted key → value), which must be a mapping without duplicate keys. */
    private static Map<String, Node> entries(Node section, String name, String source) throws ConfigException {
        if (!(section instanceof MappingNode mapping)) {
            throw new ConfigException(name.isEmpty()
                    ? source + " must be a mapping of settings to values"
                    : name + " in " + source + " is a section of settings, but has a single value");
        }
        Map<String, Node> entries = new LinkedHashMap<>();
        for (NodeTuple entry : mapping.getValue()) {
            String key = keyName(entry.getKeyNode(), name, source);
            if (entries.put(key, entry.getValueNode()) != null) {
                throw duplicate(key, source);
            }
        }
        return entries;
    }

    private static String keyName(Node key, String section, String source) throws ConfigException {
        if (key instanceof ScalarNode scalar && !isNull(scalar) && key.getAnchor() == null) {
            return section.isEmpty() ? scalar.getValue() : section + "." + scalar.getValue();
        }
        throw new ConfigException((section.isEmpty() ? "a top-level key" : "a key in section " + section) + " of "
                + source + " is not a plain name");
    }

    /** A value written out in full: not empty, and no anchor ({@code &}) or alias ({@code *}) of another value. */
    private static Node plain(String key, Node value, String source) throws ConfigException {
        // An alias is the very node it refers to, so both carry the anchor.
        if (value.getAnchor() != null) {
            throw new ConfigException(key + " in " + source
                    + " uses an anchor or alias (& or *): write the value out instead");
        }
        if (isNull(value)) {
            throw new ConfigException(key + " in " + source + " has no value");
        }
        return value;
    }

    /** The text of a setting's value, which must be a single value (a scalar). */
    private static String text(String key, Node value, String source) throws ConfigException {
        return switch (value) {
            case ScalarNode scalar -> scalar.getValue();
            case SequenceNode list ->
                    throw new ConfigException(key + " in " + source + " is a list, but needs a single value");
            default -> throw new ConfigException(key + " in " + source + " is a section, but needs a single value");
        };
    }

    /** The type of a default, from the kind of value YAML sees in it: 500 is an integer, 1.0 a number, ... */
    private static ValueType typeOf(String key, Node value) {
        Tag tag = value.getTag();
        if (tag.equals(Tag.INT)) {
            return ValueType.INTEGER;
        }
        if (tag.equals(Tag.FLOAT)) {
            return ValueType.FLOAT;
        }
        if (tag.equals(Tag.BOOL)) {
            return ValueType.BOOLEAN;
        }
        if (tag.equals(Tag.STR)) {
            return ValueType.STRING;
        }
        throw new IllegalStateException("the default of " + key + " has an unsupported type: " + tag);
    }

    private static Optional<Node> singleDocument(String yaml, String source) throws ConfigException {
        try {
            Iterator<Node> documents = new Yaml(new SafeConstructor(new LoaderOptions()))
                    .composeAll(new StringReader(yaml)).iterator();
            if (!documents.hasNext()) {
                return Optional.empty(); // nothing, or only comments
            }
            Node document = documents.next();
            if (documents.hasNext()) {
                throw new ConfigException(source
                        + " has more than one YAML document (---): put all settings into one");
            }
            return Optional.of(document);
        } catch (YAMLException e) {
            throw new ConfigException(source + " is not valid YAML: " + problemOf(e));
        }
    }

    private static boolean isNull(Node node) {
        return node instanceof ScalarNode && node.getTag().equals(Tag.NULL);
    }

    private static boolean isSection(String key, Set<String> keys) {
        return keys.stream().anyMatch(setting -> setting.startsWith(key + "."));
    }

    private static ConfigException duplicate(String key, String source) {
        return new ConfigException("duplicate key " + key + " in " + source);
    }

    /** SnakeYAML's messages span several lines; one line is enough: the problem and where it is. */
    private static String problemOf(YAMLException e) {
        if (e instanceof MarkedYAMLException marked && marked.getProblem() != null
                && marked.getProblemMark() != null) {
            Mark mark = marked.getProblemMark();
            return marked.getProblem() + " (line " + (mark.getLine() + 1) + ", column " + (mark.getColumn() + 1) + ")";
        }
        return e.getMessage().lines().findFirst().orElse("");
    }
}
