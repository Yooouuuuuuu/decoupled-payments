package io.github.yooouuuuuuu.dp.validator;

import java.util.Set;

/**
 * A bank this validator owns, because the consumer group assigned it the bank's blocks partition. Guarded by
 * the {@link Ledger} lock.
 */
final class OwnedBank {
    private final int partition;
    private final String id;
    private final BankWriter writer;
    private final Set<String> accounts;
    private long creditsOffset;

    OwnedBank(int partition, String id, BankWriter writer, Set<String> accounts, long creditsOffset) {
        this.partition = partition;
        this.id = id;
        this.writer = writer;
        this.accounts = Set.copyOf(accounts);
        this.creditsOffset = creditsOffset;
    }

    int partition() {
        return partition;
    }

    String id() {
        return id;
    }

    BankWriter writer() {
        return writer;
    }

    Set<String> accounts() {
        return accounts;
    }

    /** The next credits offset to apply: the in-memory balances include every credit before it. */
    long creditsOffset() {
        return creditsOffset;
    }

    /** Called after a credits transaction committed, to keep the position in step with the balances. */
    void creditsCommitted(long nextOffset) {
        creditsOffset = nextOffset;
    }
}
