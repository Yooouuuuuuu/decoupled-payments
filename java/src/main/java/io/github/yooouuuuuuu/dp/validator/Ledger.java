package io.github.yooouuuuuuu.dp.validator;

import io.github.yooouuuuuuu.dp.cli.Failure;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * What the blocks thread and the credits thread share (thesis §5.1): one balance map for all the banks this
 * validator owns, and the owned banks with their writers.
 *
 * <p>One lock guards it all, and it is held for the whole of every Kafka transaction, from begin to commit,
 * and during rebalance callbacks. Without it the two threads could change the same balance in overlapping
 * transactions: a dirty write if one of them aborts, or a lost update in localBalance (docs/consistency).
 * So the threads take turns, which the fair lock makes even. Everything except {@link #assignmentEpoch}
 * and {@link #ownedBankIds} must be called with the lock held.
 */
final class Ledger {
    private final ReentrantLock lock = new ReentrantLock(true);
    private final Map<String, Long> balances = new HashMap<>();
    private final Map<Integer, OwnedBank> banks = new TreeMap<>();
    /** Bumped whenever the owned banks change, so the credits thread knows to follow. */
    private volatile long assignmentEpoch;
    /** The IDs of the owned banks, replaced whenever they change. */
    private volatile List<String> ownedBankIds = List.of();
    /** Set when a transaction failed: the balances may still hold its effects. */
    private boolean dirty;

    void lock() {
        lock.lock();
    }

    void unlock() {
        lock.unlock();
    }

    /**
     * Called by a failed transaction before it releases the lock. Its changes to the balances can't be
     * undone, so from now on no transaction may start: the other thread would otherwise commit balances
     * that include them (a dirty write). The process exits, and the next owner rebuilds from Kafka.
     */
    void markDirty() {
        checkLocked();
        dirty = true;
    }

    /** Called at the start of every transaction. */
    void requireClean() {
        checkLocked();
        if (dirty) {
            throw new Failure("stopping: another transaction failed, so the balances may hold its effects");
        }
    }

    /** May be read without the lock, for a quick check whether the owned banks changed. */
    long assignmentEpoch() {
        return assignmentEpoch;
    }

    /** May be read without the lock (the stats timer does), in partition order: [bank0, bank2]. */
    List<String> ownedBankIds() {
        return ownedBankIds;
    }

    Map<String, Long> balances() {
        checkLocked();
        return balances;
    }

    OwnedBank bank(int partition) {
        checkLocked();
        OwnedBank bank = banks.get(partition);
        if (bank == null) {
            throw new IllegalStateException("partition " + partition + " belongs to no bank this validator owns");
        }
        return bank;
    }

    void add(OwnedBank bank, Map<String, Long> bankBalances) {
        checkLocked();
        banks.put(bank.partition(), bank);
        balances.putAll(bankBalances);
        ownedBanksChanged();
    }

    /** Hands a bank back: closes its writer and forgets its balances (the next owner reads them from Kafka). */
    void remove(int partition) {
        checkLocked();
        OwnedBank bank = banks.remove(partition);
        if (bank != null) {
            bank.writer().close();
            balances.keySet().removeAll(bank.accounts());
            ownedBanksChanged();
        }
    }

    void removeAll() {
        checkLocked();
        List.copyOf(banks.keySet()).forEach(this::remove);
    }

    /** Owned bank partition → the next credits offset to apply there. */
    Map<Integer, Long> creditsPositions() {
        checkLocked();
        Map<Integer, Long> positions = new TreeMap<>();
        banks.forEach((partition, bank) -> positions.put(partition, bank.creditsOffset()));
        return positions;
    }

    private void ownedBanksChanged() {
        assignmentEpoch++;
        ownedBankIds = banks.values().stream().map(OwnedBank::id).toList();
    }

    private void checkLocked() {
        if (!lock.isHeldByCurrentThread()) {
            throw new IllegalStateException("the ledger lock must be held");
        }
    }
}
