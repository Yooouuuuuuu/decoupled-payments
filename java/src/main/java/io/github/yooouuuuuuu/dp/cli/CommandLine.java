package io.github.yooouuuuuuu.dp.cli;

import io.github.yooouuuuuuu.dp.config.Flag;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A parsed command line: {@code dp <role> [--key=value | --key ...]}, {@code dp --help} or
 * {@code dp <role> --help} ({@code -h} is the same as {@code --help}). Two flags are not settings:
 * {@code --help} and {@code --config=<file>}.
 */
public record CommandLine(Optional<Role> role, boolean help, Optional<String> configFile, List<Flag> flags) {

    public static CommandLine parse(String... args) throws UsageException {
        int next = 0;
        Optional<Role> role = Optional.empty();
        if (args.length > 0 && !args[0].startsWith("-")) {
            role = Optional.of(Role.parse(args[0]).orElseThrow(
                    () -> new UsageException("unknown role '" + args[0] + "'")));
            next = 1;
        }

        boolean help = false;
        Optional<String> configFile = Optional.empty();
        List<Flag> flags = new ArrayList<>();
        for (String arg : List.of(args).subList(next, args.length)) {
            if (arg.equals("-h")) {
                help = true;
                continue;
            }
            Flag flag = parseFlag(arg);
            switch (flag.name()) {
                case "help" -> {
                    if (flag.value() != null) {
                        throw new UsageException("--help takes no value");
                    }
                    help = true;
                }
                case "config" -> {
                    if (flag.value() == null || flag.value().isEmpty()) {
                        throw new UsageException("--config needs a file: --config=<file>");
                    }
                    configFile = Optional.of(flag.value());
                }
                default -> flags.add(flag);
            }
        }
        if (role.isEmpty() && !help) {
            throw new UsageException("missing role");
        }
        return new CommandLine(role, help, configFile, List.copyOf(flags));
    }

    private static Flag parseFlag(String arg) throws UsageException {
        if (!arg.startsWith("--") || arg.length() == 2 || arg.charAt(2) == '=') {
            throw new UsageException("unexpected argument '" + arg + "': flags look like --key=value");
        }
        int equals = arg.indexOf('=');
        return equals < 0
                ? new Flag(arg.substring(2), null)
                : new Flag(arg.substring(2, equals), arg.substring(equals + 1));
    }
}
