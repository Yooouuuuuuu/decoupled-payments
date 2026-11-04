package io.github.yooouuuuuuu.dp.report;

import io.github.yooouuuuuuu.dp.kafka.BankDirectory;
import io.github.yooouuuuuuu.dp.report.Evidence.Outcome;
import io.github.yooouuuuuuu.dp.report.Report.Check;
import io.github.yooouuuuuuu.dp.report.Report.Latency;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Turns the evidence into the report: the correctness checks and the metrics. Pure code, no Kafka; the Go
 * implementation follows exactly the same rules, so both print the same numbers for the same data.
 */
final class ReportChecks {
    private ReportChecks() {
    }

    static Report evaluate(Evidence evidence, BankDirectory directory, long timeoutMs) {
        Collection<Outcome> outcomes = evidence.outcomes();
        List<Check> checks = List.of(
                complete(outcomes, timeoutMs),
                validatedOnce(evidence),
                completedOnce(evidence),
                moneyConserved(evidence, directory),
                noNegativeBalances(evidence),
                balancesMatchOrders(evidence, directory));
        return new Report(
                BankDirectory.name(directory.model()),
                directory.kafkaTransactions(),
                outcomes.size(),
                count(outcomes, Outcome::accepted),
                count(outcomes, Outcome::rejected),
                count(outcomes, Outcome::completed),
                count(outcomes, Outcome::inFlight),
                throughputTps(outcomes),
                Latency.of(outcomes.stream().filter(Outcome::completed)
                        .mapToLong(o -> o.completedAt - o.trade.getTimestamp1()).toArray()),
                Latency.of(outcomes.stream().filter(o -> o.validations() > 0)
                        .mapToLong(o -> o.validationLatency).toArray()),
                checks);
    }

    /** Every trade was validated, and every accepted trade completed, before the report gave up waiting. */
    static Check complete(Collection<Outcome> outcomes, long timeoutMs) {
        long notValidated = count(outcomes, o -> o.validations() == 0);
        long accepted = count(outcomes, Outcome::accepted);
        long notCompleted = count(outcomes, o -> o.accepted() && o.completions == 0);
        if (notValidated == 0 && notCompleted == 0) {
            return new Check("complete", true, "all " + outcomes.size() + " trades validated, all " + accepted
                    + " accepted trades completed");
        }
        return new Check("complete", false, notValidated + " of " + outcomes.size() + " trades not validated, "
                + notCompleted + " of " + accepted + " accepted trades not completed after waiting up to "
                + timeoutMs + " ms");
    }

    /** Exactly one validation record (accepted or rejected) per trade, and none for unknown trades. */
    static Check validatedOnce(Evidence evidence) {
        Collection<Outcome> outcomes = evidence.outcomes();
        List<String> problems = new ArrayList<>();
        addProblem(problems, evidence.duplicateSerialNumbers(), "serial numbers generated more than once");
        addProblem(problems, count(outcomes, o -> o.validations() > 1), "trades validated more than once");
        addProblem(problems, count(outcomes, o -> o.validations() == 0), "trades not validated");
        addProblem(problems, evidence.unknownValidations(), "validation records for unknown trades");
        return check("validated-once", problems,
                "each of the " + outcomes.size() + " trades has exactly one validation record");
    }

    /** Exactly one completion record per accepted trade; none for rejected, unvalidated or unknown trades. */
    static Check completedOnce(Evidence evidence) {
        Collection<Outcome> outcomes = evidence.outcomes();
        List<String> problems = new ArrayList<>();
        addProblem(problems, count(outcomes, o -> o.accepted() && o.completions > 1),
                "accepted trades completed more than once");
        addProblem(problems, count(outcomes, o -> o.accepted() && o.completions == 0),
                "accepted trades not completed");
        addProblem(problems, count(outcomes, o -> o.rejected() && o.completions > 0), "rejected trades completed");
        addProblem(problems, count(outcomes, o -> o.validations() == 0 && o.completions > 0),
                "trades completed without a validation");
        addProblem(problems, evidence.unknownCompletions(), "completion records for unknown trades");
        return check("completed-once", problems, "each of the " + count(outcomes, Outcome::accepted)
                + " accepted trades has exactly one completion record");
    }

    /**
     * Money only moves: once nothing is in flight, the balances add up to the money init created. Only
     * checked on a settled run: localBalance is read after orders, so while trades are still being
     * processed it would show the effects of orders records the report hasn't read.
     */
    static Check moneyConserved(Evidence evidence, BankDirectory directory) {
        long unfinished = count(evidence.outcomes(), Outcome::inFlight);
        if (unfinished > 0) {
            return notChecked("money-conserved", unfinished);
        }
        long balances = 0;
        for (BankDirectory.Bank bank : directory.banks()) {
            for (String account : bank.accounts()) {
                balances = Math.addExact(balances, evidence.latestBalances().getOrDefault(account, 0L));
            }
        }
        long total = directory.totalMoney();
        return balances == total
                ? new Check("money-conserved", true, "the balances add up to " + total + ", the total money")
                : new Check("money-conserved", false, "the balances add up to " + balances + ", but the total money is "
                        + total);
    }

    static Check noNegativeBalances(Evidence evidence) {
        long negative = evidence.negativeBalanceRecords();
        return negative == 0
                ? new Check("no-negative-balances", true, "no balance record below 0")
                : new Check("no-negative-balances", false, negative + " balance records below 0, e.g. "
                        + evidence.negativeBalanceExample());
    }

    /**
     * Each balance equals: initial balance - accepted amounts it sent + completed amounts it received. Only
     * checked on a settled run, like {@link #moneyConserved}.
     */
    static Check balancesMatchOrders(Evidence evidence, BankDirectory directory) {
        long unfinished = count(evidence.outcomes(), Outcome::inFlight);
        if (unfinished > 0) {
            return notChecked("balances-match-orders", unfinished);
        }
        Map<String, Long> expected = new HashMap<>();
        directory.banks().forEach(bank -> bank.accounts().forEach(a -> expected.put(a, directory.initialBalance())));
        for (Outcome outcome : evidence.outcomes()) {
            long amount = outcome.trade.getAmount();
            if (outcome.accepted()) {
                expected.merge(outcome.trade.getOutAccount(), -amount, Long::sum);
            }
            if (outcome.completed()) {
                expected.merge(outcome.trade.getInAccount(), amount, Long::sum);
            }
        }
        long accounts = 0;
        long mismatches = 0;
        String example = "";
        for (BankDirectory.Bank bank : directory.banks()) {
            for (String account : bank.accounts()) {
                accounts++;
                Long balance = evidence.latestBalances().get(account);
                if (!Objects.equals(balance, expected.get(account))) {
                    if (mismatches == 0) {
                        example = account + " has " + (balance == null ? "no balance" : "balance " + balance)
                                + " but the orders say " + expected.get(account);
                    }
                    mismatches++;
                }
            }
        }
        return mismatches == 0
                ? new Check("balances-match-orders", true, "all " + accounts + " balances match the orders")
                : new Check("balances-match-orders", false, mismatches + " of " + accounts
                        + " balances differ from the orders, e.g. " + example);
    }

    /**
     * Completed trades per second, from the earliest creation of a generated trade to the latest first
     * completion record, rounded half up to one decimal; 0 if nothing completed or no time passed.
     */
    static double throughputTps(Collection<Outcome> outcomes) {
        long completed = count(outcomes, Outcome::completed);
        long start = outcomes.stream().mapToLong(o -> o.trade.getTimestamp1()).min().orElse(0);
        long end = outcomes.stream().filter(Outcome::completed).mapToLong(o -> o.completedAt).max().orElse(0);
        if (completed == 0 || end <= start) {
            return 0;
        }
        return Math.round(completed * 10_000.0 / (end - start)) / 10.0;
    }

    private static Check notChecked(String name, long unfinished) {
        return new Check(name, false, "not checked: " + unfinished + " trades not finished");
    }

    private static long count(Collection<Outcome> outcomes, Predicate<Outcome> condition) {
        return outcomes.stream().filter(condition).count();
    }

    private static void addProblem(List<String> problems, long count, String what) {
        if (count > 0) {
            problems.add(count + " " + what);
        }
    }

    private static Check check(String name, List<String> problems, String passedDetail) {
        return problems.isEmpty()
                ? new Check(name, true, passedDetail)
                : new Check(name, false, String.join(", ", problems));
    }
}
