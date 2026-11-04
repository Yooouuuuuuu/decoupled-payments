package io.github.yooouuuuuuu.dp;

import io.github.yooouuuuuuu.dp.aggregator.AggregatorRole;
import io.github.yooouuuuuuu.dp.cli.CommandLine;
import io.github.yooouuuuuuu.dp.cli.Failure;
import io.github.yooouuuuuuu.dp.cli.Logging;
import io.github.yooouuuuuuu.dp.cli.Role;
import io.github.yooouuuuuuu.dp.cli.Shutdown;
import io.github.yooouuuuuuu.dp.cli.Usage;
import io.github.yooouuuuuuu.dp.cli.UsageException;
import io.github.yooouuuuuuu.dp.config.Config;
import io.github.yooouuuuuuu.dp.config.ConfigException;
import io.github.yooouuuuuuu.dp.config.ConfigLoader;
import io.github.yooouuuuuuu.dp.generator.GeneratorRole;
import io.github.yooouuuuuuu.dp.init.InitRole;
import io.github.yooouuuuuuu.dp.kafka.BankDirectory;
import io.github.yooouuuuuuu.dp.report.ReportRole;
import io.github.yooouuuuuuu.dp.validator.ValidatorRole;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code dp <role> [--key=value ...]}: runs one role of the system. Exit status 0 = success or a clean
 * shutdown, 1 = runtime failure (including a failed or interrupted report), 2 = bad usage or settings.
 */
public final class Main {
    static final int OK = 0;
    static final int FAILED = 1;
    static final int USAGE = 2;

    private Main() {
    }

    public static void main(String[] args) {
        // Numbers in our output never depend on the locale. slf4j-simple, for one, writes the time of each line
        // with the default locale's digits (Arabic-Indic in ar-EG, say), and it is set up by the first logger,
        // which a library may create while the settings are read.
        Locale.setDefault(Locale.Category.FORMAT, Locale.ROOT);
        System.exit(run(args, System.getenv()));
    }

    static int run(String[] args, Map<String, String> environment) {
        CommandLine command;
        Config config;
        try {
            command = CommandLine.parse(args);
            if (command.help()) {
                System.out.print(Usage.text(command.role()));
                return OK;
            }
            config = ConfigLoader.load(command.role().orElseThrow().id(), command.configFile(), command.flags(),
                    environment);
        } catch (UsageException e) {
            return usageError(List.of(e.getMessage()));
        } catch (ConfigException e) {
            return usageError(e.problems());
        }
        return runRole(command.role().orElseThrow(), config);
    }

    /** One line per problem, so that every bad setting shows up at once. */
    private static int usageError(List<String> problems) {
        problems.forEach(problem -> System.err.println("dp: " + problem));
        System.err.println("Run 'dp --help' for usage.");
        return USAGE;
    }

    private static int runRole(Role role, Config config) {
        Logging.configure(config.logLevel()); // before our first logger is created
        Logger log = LoggerFactory.getLogger(Main.class);
        log.info("{} starting: siteId={} model={} kafkaTransactions={} bootstrapServers={}", role.id(),
                config.siteId(), BankDirectory.name(config.model()), config.kafkaTransactions(),
                config.kafka().bootstrapServers());
        Shutdown shutdown = Shutdown.install(role);
        int status = FAILED;
        try {
            switch (role) {
                case INIT -> new InitRole(config).run();
                case GENERATOR -> new GeneratorRole(config, shutdown).run();
                case AGGREGATOR -> new AggregatorRole(config, shutdown).run();
                case VALIDATOR -> new ValidatorRole(config, shutdown).run();
                case REPORT -> new ReportRole(config).run();
            }
            status = OK;
        } catch (Exception e) {
            Failure.in(e).ifPresentOrElse(failure -> log.error(failure.getMessage()),
                    () -> log.error("{} failed", role.id(), e));
        } finally {
            shutdown.finished(status);
        }
        return status;
    }
}
