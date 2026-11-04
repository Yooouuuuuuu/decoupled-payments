package io.github.yooouuuuuuu.dp.cli;

import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;

/** The process types of the system. Each process runs exactly one role. */
public enum Role {
    INIT("create the topics and the accounts (--reset deletes all data first)", false),
    GENERATOR("the trade generator: send trades to the trades topic at a fixed rate", true),
    AGGREGATOR("layer 1: group trades into blocks, one per bank", true),
    VALIDATOR("layer 2: validate trades and apply credits for the banks it owns", true),
    REPORT("check the results for correctness and print throughput and latency", false);

    private final String summary;
    private final boolean stopsCleanly;

    Role(String summary, boolean stopsCleanly) {
        this.summary = summary;
        this.stopsCleanly = stopsCleanly;
    }

    /** The role's name on the command line; also the name of its section in default.yaml. */
    public String id() {
        return name().toLowerCase(Locale.ROOT);
    }

    public String summary() {
        return summary;
    }

    /**
     * True if SIGTERM or Ctrl-C stops the role cleanly (exit status 0). False for init and report: a signal
     * interrupts them before they finish, so there is no result (exit status 1).
     */
    public boolean stopsCleanly() {
        return stopsCleanly;
    }

    public static Optional<Role> parse(String id) {
        return Arrays.stream(values()).filter(role -> role.id().equals(id)).findFirst();
    }
}
