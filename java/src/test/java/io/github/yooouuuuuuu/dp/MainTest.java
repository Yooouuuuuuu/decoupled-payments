package io.github.yooouuuuuuu.dp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MainTest {
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();
    private PrintStream originalOut;
    private PrintStream originalErr;

    @BeforeEach
    void captureOutput() {
        originalOut = System.out;
        originalErr = System.err;
        System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
    }

    @AfterEach
    void restoreOutput() {
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    private int run(Map<String, String> environment, String... args) {
        return Main.run(args, environment);
    }

    @Test
    void helpExitsZero() {
        assertEquals(Main.OK, run(Map.of(), "--help"));
        assertTrue(out.toString(StandardCharsets.UTF_8).startsWith("Usage: dp <role>"));
        assertEquals(Main.OK, run(Map.of(), "validator", "--help"));
        assertEquals(Main.OK, run(Map.of(), "-h"));
        assertEquals(Main.OK, run(Map.of(), "report", "-h"));
        assertEquals("", err.toString(StandardCharsets.UTF_8));
    }

    @Test
    void helpWithAValueIsAUsageError() {
        assertEquals(Main.USAGE, run(Map.of(), "--help=1"));
        assertEquals(Main.USAGE, run(Map.of(), "validator", "--help=x"));
        assertTrue(err.toString(StandardCharsets.UTF_8).contains("dp: --help takes no value"));
        assertEquals("", out.toString(StandardCharsets.UTF_8));
    }

    @Test
    void everyBadSettingGetsItsOwnLine() {
        assertEquals(Main.USAGE, run(Map.of("DP_GENERATOR_TRADES", "4294967296"), "aggregator", "--blockSize=0",
                "--kafka.bootstrapServers=kafka-1:19092,"));
        assertEquals(List.of(
                "dp: kafka.bootstrapServers must name at least one broker, as host:port separated by commas, with no "
                        + "empty entry, but is 'kafka-1:19092,' (flag --kafka.bootstrapServers)",
                "dp: generator.trades must be at most 4294967295, but is 4294967296 (environment variable "
                        + "DP_GENERATOR_TRADES)",
                "dp: aggregator.blockSize must be at least 1, but is 0 (flag --blockSize)",
                "Run 'dp --help' for usage."), err.toString(StandardCharsets.UTF_8).lines().toList());
    }

    @Test
    void badUsageOrSettingsExitTwo() {
        assertEquals(Main.USAGE, run(Map.of()));
        assertEquals(Main.USAGE, run(Map.of(), "banker"));
        assertEquals(Main.USAGE, run(Map.of(), "aggregator", "--blockSize=0"));
        assertEquals(Main.USAGE, run(Map.of("DP_MODEL", "parallel"), "report"));
        String errors = err.toString(StandardCharsets.UTF_8);
        assertTrue(errors.contains("dp: missing role"), errors);
        assertTrue(errors.contains("dp: unknown role 'banker'"), errors);
        assertTrue(errors.contains("dp: aggregator.blockSize must be at least 1, but is 0 (flag --blockSize)"), errors);
        assertTrue(errors.contains("(environment variable DP_MODEL)"), errors);
        assertTrue(errors.contains("Run 'dp --help' for usage."), errors);
        assertEquals("", out.toString(StandardCharsets.UTF_8));
    }
}
