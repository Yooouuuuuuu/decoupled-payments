package io.github.yooouuuuuuu.dp.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.yooouuuuuuu.dp.config.Flag;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class CommandLineTest {

    @Test
    void roleAndFlags() throws UsageException {
        CommandLine command = CommandLine.parse("aggregator", "--blockSize=500", "--kafka.bootstrapServers=k:9092",
                "--reset", "--x=a=b");
        assertEquals(Optional.of(Role.AGGREGATOR), command.role());
        assertFalse(command.help());
        assertEquals(List.of(new Flag("blockSize", "500"), new Flag("kafka.bootstrapServers", "k:9092"),
                new Flag("reset", null), new Flag("x", "a=b")), command.flags());
    }

    @Test
    void helpAndConfigAreNotSettings() throws UsageException {
        assertTrue(CommandLine.parse("--help").help());
        assertTrue(CommandLine.parse("report", "--help").help());
        assertTrue(CommandLine.parse("-h").help());
        assertTrue(CommandLine.parse("report", "-h").help());
        assertEquals(Optional.of(Role.REPORT), CommandLine.parse("report", "-h").role());
        CommandLine command = CommandLine.parse("init", "--config=my.yaml", "--reset");
        assertEquals(Optional.of("my.yaml"), command.configFile());
        assertEquals(List.of(new Flag("reset", null)), command.flags());
    }

    @Test
    void usageErrors() {
        assertUsageError("missing role");
        assertUsageError("missing role", "--blockSize=5");
        assertUsageError("unknown role 'validate'", "validate");
        assertUsageError("unexpected argument '500'", "aggregator", "--blockSize", "500");
        assertUsageError("unexpected argument '-x'", "init", "-x");
        assertUsageError("unexpected argument '--'", "init", "--");
        assertUsageError("unexpected argument '--=1'", "init", "--=1");
        assertUsageError("--config needs a file", "init", "--config");
        assertUsageError("--help takes no value", "--help=1");
        assertUsageError("--help takes no value", "validator", "--help=x");
        assertUsageError("--help takes no value", "validator", "--help=");
        assertUsageError("unexpected argument '-x'", "-x");
    }

    private static void assertUsageError(String message, String... args) {
        UsageException e = assertThrows(UsageException.class, () -> CommandLine.parse(args));
        assertTrue(e.getMessage().startsWith(message), e.getMessage());
    }

    @Test
    void helpTextNamesRolesAndTheDefaults() {
        String general = Usage.text(Optional.empty());
        for (Role role : Role.values()) {
            assertTrue(general.contains(role.id()), role.id());
        }
        assertTrue(general.contains("config/default.yaml"));
        String aggregator = Usage.text(Optional.of(Role.AGGREGATOR));
        assertTrue(aggregator.contains("--blockSize"));
        assertTrue(aggregator.contains("DP_AGGREGATOR_BLOCK_SIZE"));
    }

    @Test
    void roleHelpListsFlagThenDefaultAsInDefaultYamlThenVariable() {
        String generator = Usage.text(Optional.of(Role.GENERATOR));
        assertTrue(generator.contains("(flag, default, environment variable)"), generator);
        assertTrue(Pattern.compile("(?m)^  --zipfExponent +1\\.0 +DP_GENERATOR_ZIPF_EXPONENT$")
                .matcher(generator).find(), generator);
        assertTrue(Pattern.compile("(?m)^  --rps +1000 +DP_GENERATOR_RPS$").matcher(generator).find(), generator);
    }
}
