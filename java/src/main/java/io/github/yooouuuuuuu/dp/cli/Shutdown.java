package io.github.yooouuuuuuu.dp.cli;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * What SIGTERM or Ctrl-C does to a role. On a signal the JVM runs its shutdown hooks, and ours decides:
 * <ul>
 *   <li>init and the report are <em>interrupted</em>: they didn't finish, so there is no result (the report
 *       has no verdict). The process says so and exits with status 1 at once; there is nothing to clean up.</li>
 *   <li>The generator, the aggregator and the validator <em>stop cleanly</em> with status 0, like any
 *       successful run. While such a role is still starting, it holds nothing that needs cleaning up either,
 *       so the process exits at once. Once it runs (see {@link #stopCleanly}), the hook asks it to stop:
 *       {@link #requested()} turns true and its blocking calls are woken up. The role finishes what it is
 *       doing (a Kafka transaction, say), closes its clients and returns; then the process exits with the
 *       role's status.</li>
 * </ul>
 */
public final class Shutdown {
    private static final Logger log = LoggerFactory.getLogger(Shutdown.class);
    /** How long a running role may take to stop; longer than the generator's final flush. */
    private static final Duration GRACE = Duration.ofSeconds(45);

    private final Role role;
    private final CountDownLatch finished = new CountDownLatch(1);
    private final List<Runnable> wakeups = new ArrayList<>(); // guarded by this
    private boolean running; // guarded by this
    private volatile boolean requested;
    private volatile int exitStatus = 1;

    private Shutdown(Role role) {
        this.role = role;
    }

    public static Shutdown install(Role role) {
        Shutdown shutdown = new Shutdown(role);
        Runtime.getRuntime().addShutdownHook(new Thread(shutdown::onSignal, "shutdown"));
        return shutdown;
    }

    /**
     * Declares that the role runs and must be stopped cleanly from now on: on a signal, run the wakeups
     * (e.g. {@code consumer::wakeup}) and wait for the role to return. Until the first call the role is still
     * starting. May be called once per client that can block; if a stop was requested already, the wakeups
     * run at once.
     */
    public synchronized void stopCleanly(Runnable... wakeups) {
        running = true;
        this.wakeups.addAll(List.of(wakeups));
        if (requested) {
            for (Runnable wakeup : wakeups) {
                wakeup.run();
            }
        }
    }

    /** True once a stop was requested: the role's loops end at the next opportunity. */
    public boolean requested() {
        return requested;
    }

    /** Asks the role to stop, exactly like a signal does; a failing thread uses it to stop the others. */
    public synchronized void request() {
        requested = true;
        wakeups.forEach(Runnable::run);
    }

    /** Called when the role has returned, with the exit status of the process. */
    public void finished(int status) {
        exitStatus = status;
        finished.countDown();
    }

    private void onSignal() {
        if (finished.getCount() == 0) {
            return; // the role has returned: a normal exit, which runs the shutdown hooks too
        }
        if (!role.stopsCleanly()) {
            log.error("{} was interrupted before it finished", role.id());
            Runtime.getRuntime().halt(1);
        }
        synchronized (this) {
            if (!running) {
                log.info("{} stopped while starting", role.id());
                Runtime.getRuntime().halt(0);
            }
            request();
        }
        // Without halt the JVM would exit with 128 + the signal number.
        Runtime.getRuntime().halt(awaitRole());
    }

    private int awaitRole() {
        try {
            if (finished.await(GRACE.toMillis(), TimeUnit.MILLISECONDS)) {
                return exitStatus;
            }
            log.error("{} did not stop within {} s", role.id(), GRACE.toSeconds());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return 1;
    }
}
