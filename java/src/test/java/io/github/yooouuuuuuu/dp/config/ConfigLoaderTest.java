package io.github.yooouuuuuuu.dp.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.yooouuuuuuu.dp.avro.Model;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigLoaderTest {
    @TempDir
    Path directory;

    private static Config load(String role, Map<String, String> environment, String... flags) throws ConfigException {
        return ConfigLoader.load(role, Optional.empty(), parseFlags(flags), environment);
    }

    private static Config load(String role, String... flags) throws ConfigException {
        return load(role, Map.of(), flags);
    }

    private static List<Flag> parseFlags(String... flags) {
        return Arrays.stream(flags).map(flag -> {
            int equals = flag.indexOf('=');
            return equals < 0 ? new Flag(flag, null) : new Flag(flag.substring(0, equals), flag.substring(equals + 1));
        }).toList();
    }

    private Path file(String yaml) throws IOException {
        Path file = directory.resolve("settings.yaml");
        Files.writeString(file, yaml);
        return file;
    }

    @Test
    void defaultsAreTheSharedDefaultYaml() throws ConfigException {
        Config config = load("validator");
        assertEquals(Model.CONCURRENT, config.model());
        assertTrue(config.kafkaTransactions());
        assertEquals(1, config.siteId());
        assertEquals(LogLevel.INFO, config.logLevel());
        assertEquals(3, config.bank().count());
        assertEquals(100, config.bank().accountsPerBank());
        assertEquals(10_000, config.bank().initialBalance());
        assertEquals(Distribution.UNIFORM, config.generator().distribution());
        assertEquals(1, config.validator().blocksPerTransaction());
        assertEquals(500, config.validator().creditsPerTransaction());
        assertEquals(1.0, config.generator().zipfExponent());
        assertEquals(60_000, config.kafka().transactionTimeoutMs());
        assertFalse(config.report().json());
    }

    @Test
    void laterLayersWin() throws Exception {
        Path settings = file("""
                aggregator:
                  blockSize: 100
                  blockTimeoutMs: 200
                  maxPollRecords: 300
                """);
        Map<String, String> environment = Map.of(
                "DP_CONFIG", settings.toString(),
                "DP_AGGREGATOR_BLOCK_TIMEOUT_MS", "20",
                "DP_AGGREGATOR_MAX_POLL_RECORDS", "30");
        Config config = load("aggregator", environment, "maxPollRecords=3");

        assertEquals(100, config.aggregator().blockSize());      // file
        assertEquals(20, config.aggregator().blockTimeoutMs());  // environment over file
        assertEquals(3, config.aggregator().maxPollRecords());   // flag over environment
    }

    @Test
    void anEmptyConfigVariableIsUnset() throws ConfigException {
        assertEquals(1, load("validator", Map.of("DP_CONFIG", "")).siteId());
    }

    @Test
    void configFlagWinsOverTheConfigVariable() throws Exception {
        Path fromVariable = file("siteId: 7");
        Path fromFlag = directory.resolve("other.yaml");
        Files.writeString(fromFlag, "siteId: 8");
        Config config = ConfigLoader.load("validator", Optional.of(fromFlag.toString()), List.of(),
                Map.of("DP_CONFIG", fromVariable.toString()));
        assertEquals(8, config.siteId());
    }

    @Test
    void configFileMayOnlyUseKnownKeys() throws IOException {
        Path settings = file("aggregator:\n  blokSize: 5\n");
        ConfigException e = assertThrows(ConfigException.class,
                () -> ConfigLoader.load("aggregator", Optional.of(settings.toString()), List.of(), Map.of()));
        assertTrue(e.getMessage().contains("aggregator.blokSize"), e.getMessage());
        assertTrue(e.getMessage().contains(settings.toString()), e.getMessage());
    }

    @Test
    void configFileValuesAreParsedByTheDefaultsType() throws Exception {
        Path settings = file("""
                kafkaTransactions: false
                generator:
                  zipfExponent: 2
                  seed: -5
                kafka:
                  bootstrapServers: broker1:9092,broker2:9092
                """);
        Config config = ConfigLoader.load("generator", Optional.of(settings.toString()), List.of(), Map.of());
        assertFalse(config.kafkaTransactions());
        assertEquals(2.0, config.generator().zipfExponent());
        assertEquals(-5, config.generator().seed());
        assertEquals("broker1:9092,broker2:9092", config.kafka().bootstrapServers());

        Path bad = file("aggregator:\n  blockSize: 1.5\n");
        ConfigException e = assertThrows(ConfigException.class,
                () -> ConfigLoader.load("aggregator", Optional.of(bad.toString()), List.of(), Map.of()));
        assertTrue(e.getMessage().contains("aggregator.blockSize") && e.getMessage().contains("config file"),
                e.getMessage());
    }

    @Test
    void missingConfigFileIsAnError() {
        Optional<String> missing = Optional.of(directory.resolve("nope.yaml").toString());
        ConfigException e = assertThrows(ConfigException.class,
                () -> ConfigLoader.load("init", missing, List.of(), Map.of()));
        assertTrue(e.getMessage().contains("nope.yaml"), e.getMessage());
    }

    @Test
    void badEnvironmentValueNamesTheVariable() {
        ConfigException e = assertThrows(ConfigException.class,
                () -> load("aggregator", Map.of("DP_AGGREGATOR_BLOCK_SIZE", "big")));
        assertTrue(e.getMessage().contains("aggregator.blockSize"), e.getMessage());
        assertTrue(e.getMessage().contains("environment variable DP_AGGREGATOR_BLOCK_SIZE"), e.getMessage());
    }

    @Test
    void flagsAreFullKeysOrKeysOfTheRolesSection() throws ConfigException {
        assertEquals(42, load("aggregator", "blockSize=42").aggregator().blockSize());
        assertEquals(42, load("aggregator", "aggregator.blockSize=42").aggregator().blockSize());
        assertEquals(42, load("validator", "aggregator.blockSize=42").aggregator().blockSize());
        assertEquals(5, load("validator", "siteId=5").siteId());
        assertTrue(load("init", "reset").init().reset());
        assertTrue(load("report", "json").report().json());
        assertFalse(load("init", "kafkaTransactions=false").kafkaTransactions());
    }

    @Test
    void sectionShortcutOnlyWorksForTheRunningRole() {
        ConfigException e = assertThrows(ConfigException.class, () -> load("validator", "blockSize=42"));
        assertTrue(e.getMessage().contains("--blockSize"), e.getMessage());
    }

    @Test
    void unknownFlagIsAnError() {
        ConfigException e = assertThrows(ConfigException.class, () -> load("generator", "speed=9"));
        assertTrue(e.getMessage().contains("unknown setting --speed"), e.getMessage());
    }

    @Test
    void flagWithoutValueOnlyWorksForBooleans() {
        ConfigException e = assertThrows(ConfigException.class, () -> load("aggregator", "blockSize"));
        assertTrue(e.getMessage().contains("--blockSize needs a value"), e.getMessage());
    }

    @Test
    void badFlagValueNamesTheKeyAndTheFlag() {
        ConfigException e = assertThrows(ConfigException.class, () -> load("generator", "rps=fast"));
        assertTrue(e.getMessage().contains("generator.rps"), e.getMessage());
        assertTrue(e.getMessage().contains("flag --rps"), e.getMessage());
    }

    @Test
    void validationNamesTheKeyAndTheSource() {
        assertInvalid("model", "must be one of sequential, concurrent", "model=parallel");
        assertInvalid("generator.distribution", "must be one of uniform, zipf", "generator.distribution=normal");
        assertInvalid("logLevel", "must be one of debug, info, warn, error", "logLevel=trace");
        assertInvalid("bank.count", "at least 1", "bank.count=0");
        assertInvalid("generator.maxAmount", "at least generator.minAmount (50)", "generator.minAmount=50",
                "generator.maxAmount=49");
        assertInvalid("siteId", "at least 0", "siteId=-1");
        assertInvalid("bank.count", "at most 10000", "bank.count=10001");
        assertInvalid("kafka.bootstrapServers", "must name at least one broker", "kafka.bootstrapServers=");
    }

    private static void assertInvalid(String key, String problem, String... flags) {
        ConfigException e = assertThrows(ConfigException.class, () -> load("generator", flags));
        assertTrue(e.getMessage().startsWith(key + " "), e.getMessage());
        assertTrue(e.getMessage().contains(problem), e.getMessage());
        assertTrue(e.getMessage().contains("(flag --"), e.getMessage());
    }

    @Test
    void environmentNamesFollowTheRule() {
        assertEquals("DP_AGGREGATOR_BLOCK_SIZE", ConfigLoader.environmentName("aggregator.blockSize"));
        assertEquals("DP_KAFKA_TRANSACTIONS", ConfigLoader.environmentName("kafkaTransactions"));
        assertEquals("DP_KAFKA_TRANSACTION_TIMEOUT_MS", ConfigLoader.environmentName("kafka.transactionTimeoutMs"));
        assertEquals("DP_SITE_ID", ConfigLoader.environmentName("siteId"));
        assertEquals("DP_KAFKA_MAX_PARTITION_FETCH_BYTES",
                ConfigLoader.environmentName("kafka.maxPartitionFetchBytes"));
        assertEquals("DP_BANK_COUNT", ConfigLoader.environmentName("bank.count"));
        assertEquals("DP_A1_B", ConfigLoader.environmentName("a1B"));
    }

    @Test
    void noTwoKeysShareAnEnvironmentName() {
        List<String> names = ConfigLoader.defaults().keySet().stream().map(ConfigLoader::environmentName).toList();
        assertEquals(names.size(), names.stream().distinct().count(), names.toString());
        assertFalse(names.contains(ConfigLoader.CONFIG_FILE_VARIABLE), "a key would collide with DP_CONFIG");
    }
}
