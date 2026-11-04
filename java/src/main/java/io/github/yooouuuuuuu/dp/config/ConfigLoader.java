package io.github.yooouuuuuuu.dp.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Loads the settings of one process from four layers; later layers win:
 * <ol>
 *   <li>default.yaml (packaged in the jar): every setting and its default</li>
 *   <li>a YAML file named by {@code --config=<file>} or the {@code DP_CONFIG} environment variable</li>
 *   <li>environment variables: {@code DP_} and the key in upper snake case</li>
 *   <li>command-line flags: {@code --key=value}</li>
 * </ol>
 * The valid keys are exactly the keys of default.yaml, and every value, from any layer, is text parsed to
 * the type of its default.
 */
public final class ConfigLoader {
    /** Names the YAML file of layer 2 when there is no {@code --config} flag; empty means none. */
    public static final String CONFIG_FILE_VARIABLE = "DP_CONFIG";

    private static final String DEFAULTS = "default.yaml";

    private ConfigLoader() {
    }

    /**
     * @param role        the role being run: a flag that isn't a full key is looked up in this section
     * @param configFile  the value of {@code --config}, if given
     * @param environment the process environment
     */
    public static Config load(String role, Optional<String> configFile, List<Flag> flags,
            Map<String, String> environment) throws ConfigException {
        Map<String, ValueType> types = new LinkedHashMap<>();
        Map<String, Setting> settings = new LinkedHashMap<>();
        readDefaults().forEach((key, value) -> {
            types.put(key, value.type());
            settings.put(key, new Setting(parseDefault(key, value), DEFAULTS));
        });

        Optional<String> file = configFile.or(() -> Optional.ofNullable(environment.get(CONFIG_FILE_VARIABLE))
                .filter(name -> !name.isEmpty()));
        if (file.isPresent()) {
            applyFile(settings, types, Path.of(file.get()));
        }
        applyEnvironment(settings, types, environment);
        applyFlags(settings, types, role, flags);
        return Config.from(settings);
    }

    /** Every key of default.yaml, in file order, with its default written as in the file (e.g. 1.0). */
    public static Map<String, String> defaults() {
        Map<String, String> defaults = new LinkedHashMap<>();
        readDefaults().forEach((key, value) -> defaults.put(key, value.text()));
        return defaults;
    }

    /**
     * The environment variable that sets a key: {@code DP_}, then the key with every {@code .} replaced by
     * {@code _}, an {@code _} before every uppercase letter that follows a lowercase letter or a digit, and
     * everything uppercased. {@code aggregator.blockSize} becomes {@code DP_AGGREGATOR_BLOCK_SIZE}.
     */
    public static String environmentName(String key) {
        StringBuilder name = new StringBuilder("DP_");
        char previous = '.';
        for (char c : key.toCharArray()) {
            if (c == '.') {
                name.append('_');
            } else {
                boolean startsWord = isUpperCase(c) && (isLowerCase(previous) || isDigit(previous));
                if (startsWord) {
                    name.append('_');
                }
                name.append(Character.toUpperCase(c));
            }
            previous = c;
        }
        return name.toString();
    }

    /**
     * Finds the key a flag sets: a full key ({@code --aggregator.blockSize}) or, for brevity, a key in the
     * running role's section ({@code dp aggregator --blockSize}).
     */
    static String resolveFlag(String name, String role, Set<String> keys) throws ConfigException {
        if (keys.contains(name)) {
            return name;
        }
        String inRoleSection = role + "." + name;
        if (keys.contains(inRoleSection)) {
            return inRoleSection;
        }
        throw new ConfigException("unknown setting --" + name + ": neither " + name + " nor " + inRoleSection
                + " is a key of default.yaml");
    }

    private static Map<String, YamlSettings.Default> readDefaults() {
        try (InputStream in = ConfigLoader.class.getResourceAsStream(DEFAULTS)) {
            if (in == null) {
                throw new IllegalStateException(DEFAULTS + " is missing from the classpath");
            }
            return YamlSettings.readDefaults(new String(in.readAllBytes(), StandardCharsets.UTF_8), DEFAULTS);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Object parseDefault(String key, YamlSettings.Default value) {
        try {
            return value.type().parse(value.text());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("bad default for " + key + " in " + DEFAULTS + ": " + e.getMessage(), e);
        }
    }

    private static void applyFile(Map<String, Setting> settings, Map<String, ValueType> types, Path file)
            throws ConfigException {
        String source = "config file " + file;
        String yaml;
        try {
            yaml = Files.readString(file);
        } catch (IOException e) {
            throw new ConfigException("cannot read " + source + ": " + e);
        }
        for (Map.Entry<String, String> entry : YamlSettings.readOverrides(yaml, source, types.keySet()).entrySet()) {
            String key = entry.getKey();
            settings.put(key, parse(key, types.get(key), entry.getValue(), source));
        }
    }

    private static void applyEnvironment(Map<String, Setting> settings, Map<String, ValueType> types,
            Map<String, String> environment) throws ConfigException {
        for (Map.Entry<String, ValueType> entry : types.entrySet()) {
            String name = environmentName(entry.getKey());
            String text = environment.get(name);
            if (text != null) {
                String key = entry.getKey();
                settings.put(key, parse(key, entry.getValue(), text, "environment variable " + name));
            }
        }
    }

    private static void applyFlags(Map<String, Setting> settings, Map<String, ValueType> types, String role,
            List<Flag> flags) throws ConfigException {
        for (Flag flag : flags) {
            String key = resolveFlag(flag.name(), role, types.keySet());
            ValueType type = types.get(key);
            String source = "flag --" + flag.name();
            String text = flag.value();
            if (text == null) {
                if (type != ValueType.BOOLEAN) {
                    throw new ConfigException(source + " needs a value (" + type.description() + "): --"
                            + flag.name() + "=<value>");
                }
                text = "true";
            }
            settings.put(key, parse(key, type, text, source));
        }
    }

    private static Setting parse(String key, ValueType type, String text, String source) throws ConfigException {
        try {
            return new Setting(type.parse(text), source);
        } catch (IllegalArgumentException e) {
            throw new ConfigException("bad value for " + key + " (" + source + "): " + e.getMessage());
        }
    }

    private static boolean isUpperCase(char c) {
        return c >= 'A' && c <= 'Z';
    }

    private static boolean isLowerCase(char c) {
        return c >= 'a' && c <= 'z';
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }
}
