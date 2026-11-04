package io.github.yooouuuuuuu.dp.cli;

import io.github.yooouuuuuuu.dp.config.ConfigLoader;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/** The text printed by {@code dp --help} and {@code dp <role> --help}. */
public final class Usage {
    private Usage() {
    }

    public static String text(Optional<Role> role) {
        StringBuilder text = new StringBuilder("""
                Usage: dp <role> [--key=value ...]
                       dp --help | dp <role> --help      (-h is the same as --help)

                Roles:
                """);
        for (Role r : Role.values()) {
            text.append(String.format(Locale.ROOT, "  %-11s %s%n", r.id(), r.summary()));
        }
        text.append("""

                Settings:
                  Every setting and its default is in config/default.yaml. Later sources win:
                    --config=<file> or DP_CONFIG=<file>  a YAML file with some of the same keys
                    DP_<KEY>                             environment variables, e.g. DP_AGGREGATOR_BLOCK_SIZE=500
                    --<key>=<value>                      flags, e.g. --aggregator.blockSize=500
                  A flag that isn't a full key is looked up in the role's section (dp aggregator --blockSize=500).
                  A boolean flag on its own means true (dp init --reset).

                Exit status: 0 success, 1 runtime failure (or a failed report), 2 bad usage or settings.
                  SIGTERM or Ctrl-C stops the generator, an aggregator or a validator cleanly (0), also while it
                  is starting; it interrupts init and the report, which then have no result (1).
                """);
        role.ifPresent(r -> appendRoleSettings(text, r));
        return text.toString();
    }

    /** One line per setting of the role's section: the flag, its default as written in default.yaml, its variable. */
    private static void appendRoleSettings(StringBuilder text, Role role) {
        String prefix = role.id() + ".";
        Map<String, String> settings = new LinkedHashMap<>();
        ConfigLoader.defaults().forEach((key, defaultText) -> {
            if (key.startsWith(prefix)) {
                settings.put(key, defaultText);
            }
        });
        int flagWidth = settings.keySet().stream().mapToInt(key -> key.length() - prefix.length() + 2).max().orElse(1);
        int defaultWidth = settings.values().stream().mapToInt(String::length).max().orElse(1);
        String line = "  %-" + flagWidth + "s  %-" + defaultWidth + "s  %s%n";
        text.append(String.format(Locale.ROOT, "%nSettings of %s (flag, default, environment variable):%n", role.id()));
        settings.forEach((key, defaultText) -> text.append(String.format(Locale.ROOT, line,
                "--" + key.substring(prefix.length()), defaultText, ConfigLoader.environmentName(key))));
    }
}
