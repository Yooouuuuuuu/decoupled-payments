package io.github.yooouuuuuuu.dp.generator;

import io.github.yooouuuuuuu.dp.avro.Block;
import io.github.yooouuuuuuu.dp.cli.Failure;
import io.github.yooouuuuuuu.dp.cli.Shutdown;
import io.github.yooouuuuuuu.dp.config.Config;
import io.github.yooouuuuuuu.dp.kafka.BankDirectory;
import io.github.yooouuuuuuu.dp.kafka.Ids;
import io.github.yooouuuuuuu.dp.kafka.KafkaClients;
import io.github.yooouuuuuuu.dp.kafka.SendErrors;
import io.github.yooouuuuuuu.dp.kafka.Topics;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The trade generator: sends {@code generator.trades} trades (0 = until stopped) to the trades topic at
 * {@code generator.rps} trades per second. In the thesis's deployment the trades come from the banks' servers.
 *
 * <p>The same {@code generator.seed} gives the same trades, but only within this implementation: the Go
 * implementation draws its random numbers differently.
 */
public final class GeneratorRole {
    private static final Logger log = LoggerFactory.getLogger(GeneratorRole.class);
    /** How long the trades still in the producer may take to be sent at the end; any not sent by then fail. */
    private static final Duration FLUSH_LIMIT = Duration.ofSeconds(30);

    private final Config config;
    private final Shutdown shutdown;

    public GeneratorRole(Config config, Shutdown shutdown) {
        this.config = config;
        this.shutdown = shutdown;
    }

    public void run() {
        BankDirectory directory = BankDirectory.read(config, clientId("accountInfo"));
        List<Account> accounts = accountsOf(directory);
        if (accounts.size() < 2) {
            throw new Failure("the trade generator needs at least two accounts");
        }
        // The run ID comes from an unseeded source, so that runs with the same seed still get new serial numbers.
        int runId = new SecureRandom().nextInt() >>> 1;
        // Everything else is reproducible: the order of the accounts (their Zipf rank), the picks and the amounts.
        Random random = new Random(config.generator().seed());
        Collections.shuffle(accounts, random);
        Config.Generator settings = config.generator();
        TradeFactory trades = new TradeFactory(runId,
                AccountPicker.of(settings.distribution(), accounts, settings.zipfExponent()),
                random, directory.banks().size() > 1, settings.minAmount(), settings.maxAmount());

        long start = System.nanoTime();
        long sent = send(trades, start);
        double seconds = (System.nanoTime() - start) / 1e9;
        log.info("sent {} trades in {} s ({}/s), runId {}", sent, String.format(Locale.ROOT, "%.1f", seconds),
                Math.round(sent / seconds), runId);
    }

    /**
     * Sends trades on schedule until all are sent or a stop is requested, then waits for the producer to send
     * what it still holds, at most {@link #FLUSH_LIMIT}; returns how many trades were sent.
     *
     * @throws org.apache.kafka.common.KafkaException if a trade could not be sent (in time)
     */
    private long send(TradeFactory trades, long start) {
        SendErrors errors = new SendErrors();
        KafkaProducer<String, SpecificRecord> producer = KafkaClients.producer(config, clientId("producer"));
        StopInterrupt stopInterrupt = new StopInterrupt(Thread.currentThread());
        shutdown.stopCleanly(stopInterrupt::interrupt);
        long sent;
        try {
            sent = sendUntilDoneOrStopped(producer, trades, errors, start);
        } finally {
            stopInterrupt.disable();
            // Close sends what is left, for at most the limit; the trades it couldn't send fail their callbacks.
            producer.close(FLUSH_LIMIT);
        }
        errors.throwIfAny();
        return sent;
    }

    private long sendUntilDoneOrStopped(KafkaProducer<String, SpecificRecord> producer, TradeFactory trades,
            SendErrors errors, long start) {
        // 0 means until stopped, but a run ends after MAX_TRADES anyway: its sequence numbers have 32 bits.
        long limit = config.generator().trades() == 0 ? Config.MAX_TRADES : config.generator().trades();
        long rps = config.generator().rps();
        long statsInterval = TimeUnit.MILLISECONDS.toNanos(config.statsIntervalMs());
        long lastStats = start;
        long sent = 0;
        while (!shutdown.requested() && sent < limit) {
            long now = System.nanoTime();
            long due = Math.min(Pacing.dueAfter(now - start, rps), limit);
            try {
                for (; sent < due; sent++) {
                    Block block = new Block(List.of(trades.next(System.currentTimeMillis())));
                    // No key: the producer spreads the trades over the trades partitions, since any
                    // aggregator may take any trade.
                    producer.send(new ProducerRecord<>(Topics.TRADES, block), errors);
                }
            } catch (InterruptException e) {
                break; // a stop request interrupted a send that waited for room in the producer's buffer
            }
            errors.throwIfAny();
            if (now - lastStats >= statsInterval) {
                log.info("sent {} trades ({}/s)", sent, Math.round(sent / ((now - start) / 1e9)));
                lastStats = now;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                break; // a stop request
            }
        }
        return sent;
    }

    /** All accounts, bank by bank. */
    private static List<Account> accountsOf(BankDirectory directory) {
        List<Account> accounts = new ArrayList<>();
        for (BankDirectory.Bank bank : directory.banks()) {
            for (String account : bank.accounts()) {
                accounts.add(new Account(account, bank.id(), bank.partition()));
            }
        }
        return accounts;
    }

    private String clientId(String suffix) {
        return Ids.client("generator", config.siteId(), suffix);
    }

    /**
     * Lets a stop request interrupt the sending thread, so that a send waiting for room in the producer's
     * buffer (up to max.block.ms) ends at once. Only while sending: an interrupt would also cut short the
     * final flush, which must be able to wait.
     */
    private static final class StopInterrupt {
        private final Thread sender;
        private boolean enabled = true; // guarded by this

        StopInterrupt(Thread sender) {
            this.sender = sender;
        }

        synchronized void interrupt() {
            if (enabled) {
                sender.interrupt();
            }
        }

        /** No more interrupts, and none left pending, once this returns. */
        synchronized void disable() {
            enabled = false;
            Thread.interrupted(); // clears an interrupt that arrived after the send it was meant for
        }
    }
}
