package io.github.yooouuuuuuu.dp.cli;

import io.github.yooouuuuuuu.dp.config.LogLevel;
import java.util.Locale;

/** Logging goes through slf4j-simple, to stderr. Its fixed settings are in simplelogger.properties. */
public final class Logging {
    private static final String LEVEL_OF = "org.slf4j.simpleLogger.log.";

    private Logging() {
    }

    /**
     * Sets the level of our own logging to {@code logLevel}; the Kafka client library logs at WARN, or at ERROR
     * when our level is ERROR. The levels are set per package: slf4j-simple reads its default level only once,
     * when the first logger is created, which a library may have done already (Avro does, while the settings
     * are read), but it looks a package's level up whenever a logger is created, and ours and Kafka's are all
     * created after this.
     */
    public static void configure(LogLevel level) {
        System.setProperty(LEVEL_OF + "io.github.yooouuuuuuu.dp", level.name().toLowerCase(Locale.ROOT));
        System.setProperty(LEVEL_OF + "org.apache.kafka", level == LogLevel.ERROR ? "error" : "warn");
    }
}
