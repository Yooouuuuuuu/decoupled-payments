package io.github.yooouuuuuuu.dp.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Config files are read as text, never typed by the YAML library (SnakeYAML is YAML 1.1, where yes, 010 and
 * 1_000 mean something else than in Go's YAML 1.2 library): each value is parsed like a flag's.
 */
class YamlSettingsTest {
    private static final String SOURCE = "config file test.yaml";
    private static final Set<String> KEYS = ConfigLoader.defaults().keySet();

    @TempDir
    Path directory;

    private static Map<String, String> overrides(String yaml) throws ConfigException {
        return YamlSettings.readOverrides(yaml, SOURCE, KEYS);
    }

    private static void assertRejected(String yaml, String... expectedParts) {
        ConfigException e = assertThrows(ConfigException.class, () -> overrides(yaml), yaml);
        for (String part : expectedParts) {
            assertTrue(e.getMessage().contains(part), e.getMessage());
        }
        assertTrue(e.getMessage().contains(SOURCE), e.getMessage());
    }

    /** Loads a file through every layer, as `dp <role> --config=<file>` does. */
    private Config load(String yaml) throws IOException, ConfigException {
        Path file = directory.resolve("settings.yaml");
        Files.writeString(file, yaml);
        return ConfigLoader.load("aggregator", Optional.of(file.toString()), List.of(), Map.of());
    }

    private void assertBadValue(String yaml, String key) {
        ConfigException e = assertThrows(ConfigException.class, () -> load(yaml), yaml);
        assertTrue(e.getMessage().startsWith("bad value for " + key + " (config file "), e.getMessage());
    }

    @Test
    void valuesStayTheTextWrittenInTheFile() throws ConfigException {
        assertEquals(Map.of("init.reset", "yes", "aggregator.blockSize", "010", "kafka.bootstrapServers", "a:1",
                "model", "sequential"), overrides("""
                init:
                  reset: yes
                aggregator:
                  blockSize: 010
                kafka:
                  bootstrapServers: "a:1"
                model: 'sequential'
                """));
    }

    @Test
    void eachTextIsParsedByTheDefaultsTypeLikeAFlag() throws Exception {
        assertEquals(10, load("aggregator:\n  blockSize: 010\n").aggregator().blockSize(), "010 is ten, not 8");
        assertBadValue("init:\n  reset: yes\n", "init.reset");
        assertBadValue("aggregator:\n  blockSize: 1_000\n", "aggregator.blockSize");
        assertBadValue("aggregator:\n  blockSize: 0x10\n", "aggregator.blockSize");
        assertBadValue("aggregator:\n  blockSize: 1:30\n", "aggregator.blockSize");
        assertBadValue("generator:\n  zipfExponent: 1_0.5\n", "generator.zipfExponent");
    }

    @Test
    void aSettingWithoutAValueIsAnError() {
        assertRejected("kafka:\n  bootstrapServers: ~\n", "kafka.bootstrapServers", "has no value");
        assertRejected("kafka:\n  bootstrapServers: null\n", "kafka.bootstrapServers", "has no value");
        assertRejected("kafka:\n  bootstrapServers:\n", "kafka.bootstrapServers", "has no value");
        assertRejected("kafka:\n", "kafka", "has no value");
    }

    @Test
    void aListIsAnError() {
        assertRejected("kafka:\n  bootstrapServers: [a:1, b:2]\n", "kafka.bootstrapServers", "is a list");
        assertRejected("kafka:\n  bootstrapServers:\n    - a:1\n", "kafka.bootstrapServers", "is a list");
    }

    @Test
    void sectionsAndValuesMustNotBeMixedUp() {
        assertRejected("siteId:\n  value: 1\n", "siteId", "is a section, but needs a single value");
        assertRejected("kafka: localhost:9092\n", "kafka", "is a section of settings, but has a single value");
    }

    @Test
    void unknownAndDuplicateKeysAreErrors() {
        assertRejected("kafka:\n  bootstrap: a:1\n", "unknown setting kafka.bootstrap");
        assertRejected("siteId: 1\nsiteId: 2\n", "duplicate key siteId");
        assertRejected("kafka:\n  sessionTimeoutMs: 1\nkafka:\n  transactionTimeoutMs: 2\n", "duplicate key kafka");
        assertRejected("kafka:\n  sessionTimeoutMs: 1\nkafka.sessionTimeoutMs: 2\n",
                "duplicate key kafka.sessionTimeoutMs");
    }

    @Test
    void anchorsAndAliasesAreErrors() {
        assertRejected("siteId: &site 5\nbank:\n  count: *site\n", "siteId", "anchor or alias");
        assertRejected("kafka: &k\n  sessionTimeoutMs: 1\n", "kafka", "anchor or alias");
    }

    @Test
    void onlyOneDocumentIsAllowed() {
        assertRejected("siteId: 1\n---\nsiteId: 2\n", "more than one YAML document");
    }

    @Test
    void anEmptyFileOrOneWithOnlyCommentsOrATildeSetsNothing() throws ConfigException {
        assertEquals(Map.of(), overrides(""));
        assertEquals(Map.of(), overrides("# nothing to change\n"));
        assertEquals(Map.of(), overrides("~\n"));
        assertEquals(Map.of(), overrides("---\n"));
    }

    @Test
    void theTopLevelMustBeAMapping() {
        assertRejected("- siteId\n", "must be a mapping of settings to values");
        assertRejected("siteId\n", "must be a mapping of settings to values");
    }

    @Test
    void invalidYamlSaysWhereTheProblemIs() {
        assertRejected("kafka:\n  bootstrapServers: [a\n", "is not valid YAML", "line");
    }

    @Test
    void defaultsKeepTheirTextAndType() {
        Map<String, String> defaults = ConfigLoader.defaults();
        assertEquals(29, defaults.size());
        assertEquals("500", defaults.get("aggregator.blockSize"));
        assertEquals("1.0", defaults.get("generator.zipfExponent"), "a float default is shown as written");
        assertEquals("true", defaults.get("kafkaTransactions"));
        assertEquals("localhost:9092", defaults.get("kafka.bootstrapServers"));
    }
}
