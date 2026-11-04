package io.github.yooouuuuuuu.dp.validator;

import io.github.yooouuuuuuu.dp.avro.Block;
import io.github.yooouuuuuuu.dp.avro.Model;
import io.github.yooouuuuuuu.dp.cli.Failure;
import io.github.yooouuuuuuu.dp.cli.Shutdown;
import io.github.yooouuuuuuu.dp.cli.StatsTimer;
import io.github.yooouuuuuuu.dp.config.Config;
import io.github.yooouuuuuuu.dp.kafka.AvroSerde;
import io.github.yooouuuuuuu.dp.kafka.BankDirectory;
import io.github.yooouuuuuuu.dp.kafka.Groups;
import io.github.yooouuuuuuu.dp.kafka.Ids;
import io.github.yooouuuuuuu.dp.kafka.KafkaClients;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The validator role (layer 2), the core of the system. It owns the banks whose blocks partitions the
 * consumer group {@code validators} assigns to it, keeps their balances in memory, validates their trades
 * and applies their credits, all inside Kafka transactions.
 *
 * <p>Error policy: any failure in a transaction stops both threads and the process exits with status 1. It is
 * restarted and rebuilds its state from Kafka exactly as on a normal start, so the in-memory balances never
 * keep the effects of an aborted transaction and no undo logic is needed.
 */
public final class ValidatorRole {
    private static final Logger log = LoggerFactory.getLogger(ValidatorRole.class);
    private static final String ROLE = "validator";

    private final Config config;
    private final Shutdown shutdown;

    public ValidatorRole(Config config, Shutdown shutdown) {
        this.config = config;
        this.shutdown = shutdown;
    }

    public void run() throws InterruptedException {
        BankDirectory directory = BankDirectory.read(config, clientId("accountInfo"));
        directory.requireModelOf(config);
        Ledger ledger = new Ledger();
        ValidatorStats stats = new ValidatorStats();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        StatsTimer timer = StatsTimer.start(config.statsIntervalMs(),
                seconds -> stats.log(seconds, ledger.ownedBankIds()));
        try {
            List<Thread> threads = new ArrayList<>();
            if (config.model() == Model.CONCURRENT) {
                threads.add(start("credits", failure, () -> applyCredits(ledger, stats)));
            }
            threads.add(start("blocks", failure, () -> validateBlocks(ledger, directory, stats, failure)));
            for (Thread thread : threads) {
                thread.join();
            }
        } finally {
            timer.close();
        }

        ledger.lock();
        try {
            ledger.removeAll();
        } finally {
            ledger.unlock();
        }
        if (failure.get() != null) {
            throw new Failure("the validator stopped after a failure (see above); restart it to rebuild its "
                    + "state from Kafka");
        }
    }

    private void validateBlocks(Ledger ledger, BankDirectory directory, ValidatorStats stats,
            AtomicReference<Throwable> failure) {
        try (KafkaConsumer<String, Block> consumer = KafkaClients.groupConsumer(config, Groups.VALIDATORS,
                ROLE + "-" + config.siteId(), clientId("blocks"), AvroSerde.blocks(),
                config.validator().blocksPerTransaction())) {
            shutdown.stopCleanly(consumer::wakeup);
            new BlocksLoop(consumer, ledger, new BankTakeover(config, directory), config.model(), stats).run(shutdown);
            // The loop also ends when the credits thread failed and stopped it. Then, as after any failure, the
            // plain close of the try-with-resources keeps the membership, so that a restart with the same
            // siteId gets the banks back without a rebalance.
            if (failure.get() == null) {
                // Closing hands the banks back (onPartitionsRevoked) before leaving the group.
                KafkaClients.leaveGroupAndClose(consumer);
            }
        }
    }

    private void applyCredits(Ledger ledger, ValidatorStats stats) throws InterruptedException {
        try (KafkaConsumer<String, Block> consumer = KafkaClients.consumer(config, clientId("credits"),
                AvroSerde.blocks(), config.validator().creditsPerTransaction())) {
            shutdown.stopCleanly(consumer::wakeup);
            new CreditsLoop(consumer, ledger, stats).run(shutdown);
        }
    }

    private interface Loop {
        void run() throws Exception;
    }

    /** Runs a loop in its own thread; if it fails, the other thread is stopped too and the role fails. */
    private Thread start(String name, AtomicReference<Throwable> failure, Loop loop) {
        return Thread.ofPlatform().name(name).start(() -> {
            try {
                loop.run();
            } catch (Throwable e) {
                // A failure (e.g. "run init --reset") explains itself, also when it comes wrapped in the
                // exception of a rebalance callback; anything else needs its stack trace.
                Failure.in(e).ifPresentOrElse(cause -> log.error(cause.getMessage()),
                        () -> log.error("the {} thread failed", name, e));
                failure.compareAndSet(null, e);
                shutdown.request();
            }
        });
    }

    private String clientId(String suffix) {
        return Ids.client(ROLE, config.siteId(), suffix);
    }
}
