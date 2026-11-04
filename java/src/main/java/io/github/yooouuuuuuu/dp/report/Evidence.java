package io.github.yooouuuuuuu.dp.report;

import io.github.yooouuuuuuu.dp.avro.Category;
import io.github.yooouuuuuuu.dp.avro.Trade;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * What the report has read from Kafka, as plain data (no Kafka here): the generated trades, what the
 * orders topic says happened to each of them, and the latest balances.
 */
final class Evidence {

    /**
     * One generated trade and the orders records about it. Each trade is in exactly one class: rejected,
     * completed, or in flight (accepted trades are completed or in flight).
     */
    static final class Outcome {
        final Trade trade;
        /** SEND records: the trade was validated and accepted. */
        int sends;
        /** REJECTED records: the trade was validated and rejected. */
        int rejections;
        /** RECEIVE records: the credit was applied, so the trade is completed. */
        int completions;
        /** timestamp2 - timestamp1 of the first validation record (the lowest offset). */
        long validationLatency;
        /** Kafka timestamp of the first completion record (the lowest offset). */
        long completedAt;
        private long firstValidationOffset = Long.MAX_VALUE;
        private long firstCompletionOffset = Long.MAX_VALUE;

        Outcome(Trade trade) {
            this.trade = trade;
        }

        int validations() {
            return sends + rejections;
        }

        /** Validated with at least one SEND record: its amount left the sender. */
        boolean accepted() {
            return sends > 0;
        }

        /** Rejected and never accepted. */
        boolean rejected() {
            return rejections > 0 && sends == 0;
        }

        /** Accepted, and its credit applied. */
        boolean completed() {
            return accepted() && completions > 0;
        }

        /** Neither rejected nor completed: not validated yet, or accepted and not completed yet. */
        boolean inFlight() {
            return !rejected() && !completed();
        }
    }

    private final Map<Long, Outcome> outcomes = new HashMap<>();
    private final Set<Long> waitingForValidation = new HashSet<>();
    private final Set<Long> waitingForCompletion = new HashSet<>();
    private final Map<String, Long> latestBalances = new HashMap<>();
    private long duplicateSerialNumbers;
    private long unknownValidations;
    private long unknownCompletions;
    private long negativeBalanceRecords;
    private String negativeBalanceExample = "";

    /** A trade from the trades topic. Credits there (RECEIVE, Sequential model) are not new trades. */
    void traded(Trade trade) {
        if (outcomes.putIfAbsent(trade.getSerialNumber(), new Outcome(trade)) != null) {
            duplicateSerialNumbers++;
            return;
        }
        waitingForValidation.add(trade.getSerialNumber());
    }

    /**
     * A trade inside the orders record at {@code offset}: SEND = accepted, REJECTED = rejected, RECEIVE =
     * completed. The completion may be read before the validation: they are on different orders partitions.
     */
    void ordered(Trade trade, long offset, long recordTimestamp) {
        Outcome outcome = outcomes.get(trade.getSerialNumber());
        switch (trade.getCategory()) {
            case SEND, REJECTED -> {
                if (outcome == null) {
                    unknownValidations++;
                } else {
                    validated(outcome, trade, offset);
                }
            }
            case RECEIVE -> {
                if (outcome == null) {
                    unknownCompletions++;
                } else {
                    completed(outcome, offset, recordTimestamp);
                }
            }
        }
    }

    private void validated(Outcome outcome, Trade validation, long offset) {
        if (offset < outcome.firstValidationOffset) {
            outcome.firstValidationOffset = offset;
            outcome.validationLatency = validation.getTimestamp2() - validation.getTimestamp1();
        }
        if (validation.getCategory() == Category.SEND) {
            outcome.sends++;
        } else {
            outcome.rejections++;
        }
        long serialNumber = outcome.trade.getSerialNumber();
        waitingForValidation.remove(serialNumber);
        if (outcome.accepted() && outcome.completions == 0) {
            waitingForCompletion.add(serialNumber);
        }
    }

    private void completed(Outcome outcome, long offset, long recordTimestamp) {
        if (offset < outcome.firstCompletionOffset) {
            outcome.firstCompletionOffset = offset;
            outcome.completedAt = recordTimestamp;
        }
        outcome.completions++;
        waitingForCompletion.remove(outcome.trade.getSerialNumber());
    }

    /** A record from localBalance; the last one per account is the account's balance. */
    void balance(String account, long balance) {
        latestBalances.put(account, balance);
        if (balance < 0) {
            if (negativeBalanceRecords == 0) {
                negativeBalanceExample = account + ": " + balance;
            }
            negativeBalanceRecords++;
        }
    }

    /** True once no trade is in flight: every trade is validated and every accepted trade is completed. */
    boolean settled() {
        return waitingForValidation.isEmpty() && waitingForCompletion.isEmpty();
    }

    long waitingForValidation() {
        return waitingForValidation.size();
    }

    long waitingForCompletion() {
        return waitingForCompletion.size();
    }

    Collection<Outcome> outcomes() {
        return outcomes.values();
    }

    Map<String, Long> latestBalances() {
        return latestBalances;
    }

    long duplicateSerialNumbers() {
        return duplicateSerialNumbers;
    }

    long unknownValidations() {
        return unknownValidations;
    }

    long unknownCompletions() {
        return unknownCompletions;
    }

    long negativeBalanceRecords() {
        return negativeBalanceRecords;
    }

    String negativeBalanceExample() {
        return negativeBalanceExample;
    }
}
