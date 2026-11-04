package io.github.yooouuuuuuu.dp.validator;

import io.github.yooouuuuuuu.dp.cli.StatsTimer;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The validator's progress counters, updated by both threads after each committed transaction and read by
 * the stats timer, which needs neither thread nor the balance lock.
 */
final class ValidatorStats {
    private static final Logger log = LoggerFactory.getLogger(ValidatorStats.class);

    private final LongAdder blocks = new LongAdder();
    private final LongAdder accepted = new LongAdder();
    private final LongAdder rejected = new LongAdder();
    private final LongAdder credited = new LongAdder();

    void committed(BankBatch batch) {
        blocks.add(batch.blocks());
        accepted.add(batch.accepted());
        rejected.add(batch.rejected());
        credited.add(batch.credited());
    }

    /** One stats line, for {@link StatsTimer}: the counts since the previous line, and the banks owned now. */
    void log(double seconds, List<String> ownedBanks) {
        long acceptedNow = accepted.sumThenReset();
        long rejectedNow = rejected.sumThenReset();
        log.info("last {} s: validated {} blocks with {} trades ({} accepted, {} rejected), applied {} credits; "
                + "owns {}", StatsTimer.seconds(seconds), blocks.sumThenReset(), acceptedNow + rejectedNow,
                acceptedNow, rejectedNow, credited.sumThenReset(), ownedBanks);
    }
}
