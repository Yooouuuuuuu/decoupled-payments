package io.github.yooouuuuuuu.dp.report;

import static io.github.yooouuuuuuu.dp.TestTrades.send;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.yooouuuuuuu.dp.TestTrades;
import io.github.yooouuuuuuu.dp.avro.AccountInfo;
import io.github.yooouuuuuuu.dp.avro.Category;
import io.github.yooouuuuuuu.dp.avro.Model;
import io.github.yooouuuuuuu.dp.avro.Trade;
import io.github.yooouuuuuuu.dp.kafka.BankDirectory;
import io.github.yooouuuuuuu.dp.report.Report.Check;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class ReportChecksTest {
    /** Two banks with two accounts of 100 each: 400 in total. */
    private static final BankDirectory DIRECTORY = BankDirectory.of(List.of(
            new AccountInfo("bank0", 0, List.of("bank0-0000", "bank0-0001"), 100L, Model.CONCURRENT, true),
            new AccountInfo("bank1", 1, List.of("bank1-0000", "bank1-0001"), 100L, Model.CONCURRENT, true)));

    private final Evidence evidence = new Evidence();
    /** The offset of the next orders record, so that each record of a test has its own. */
    private long nextOffset;

    /** The trade as an orders record shows it. */
    private static Trade order(Trade trade, Category category, long timestamp2) {
        Trade order = TestTrades.trade(trade.getSerialNumber(), trade.getOutAccount(), trade.getInAccount(),
                trade.getAmount(), category);
        order.setTimestamp2(timestamp2);
        return order;
    }

    private void ordered(Trade trade, Category category, long timestamp2, long recordTimestamp) {
        evidence.ordered(order(trade, category, timestamp2), nextOffset++, recordTimestamp);
    }

    /** Trade 1: bank0-0000 pays 30 to bank1-0000, validated at 1100 and completed at 1500 (created at 1000). */
    private void acceptedAndCompleted() {
        Trade generated = send(1, "bank0-0000", "bank1-0000", 30);
        evidence.traded(generated);
        ordered(generated, Category.SEND, 1100, 1100);
        ordered(generated, Category.RECEIVE, 1100, 1500);
    }

    /** Trade 2: bank1-0001 tries to pay 500 and is rejected at 1200. */
    private void rejected() {
        Trade generated = send(2, "bank1-0001", "bank0-0001", 500);
        evidence.traded(generated);
        ordered(generated, Category.REJECTED, 1200, 1200);
    }

    private void balances(long bank00, long bank01, long bank10, long bank11) {
        evidence.balance("bank0-0000", bank00);
        evidence.balance("bank0-0001", bank01);
        evidence.balance("bank1-0000", bank10);
        evidence.balance("bank1-0001", bank11);
    }

    private Report report() {
        return ReportChecks.evaluate(evidence, DIRECTORY, 60_000);
    }

    private Map<String, Check> checks() {
        return report().checks().stream().collect(Collectors.toMap(Check::name, Function.identity()));
    }

    @Test
    void aCorrectRunPassesEveryCheck() {
        acceptedAndCompleted();
        rejected();
        balances(70, 100, 130, 100);

        Report report = report();
        assertTrue(report.passed(), report.checks().toString());
        assertEquals(List.of("complete", "validated-once", "completed-once", "money-conserved",
                "no-negative-balances", "balances-match-orders"), report.checks().stream().map(Check::name).toList());
        assertEquals(2, report.generated());
        assertEquals(1, report.accepted());
        assertEquals(1, report.rejected());
        assertEquals(1, report.completed());
        assertEquals(0, report.inFlight());
        assertEquals("concurrent", report.model());
        assertEquals(500, report.latencyMs().max(), "completion record at 1500 - created at 1000");
        assertEquals(200, report.validationLatencyMs().max(), "validated at 1200 - created at 1000");
        assertEquals(2.0, report.throughputTps(), "1 trade completed 500 ms after the first was created");
    }

    /**
     * The case shared with the Go implementation's tests, with the same expected numbers. T1..T8 are
     * generated (T6's serial number twice), U is unknown.
     */
    @Test
    void sharedCase() {
        Trade[] t = new Trade[9];
        for (int i = 1; i <= 8; i++) {
            t[i] = send(i, "bank0-0000", "bank1-0000", 10);
            evidence.traded(t[i]);
        }
        evidence.traded(send(6, "bank0-0001", "bank1-0001", 10));
        Trade unknown = send(99, "bank0-0000", "bank1-0000", 10);

        ordered(t[1], Category.SEND, 1100, 1100);
        ordered(t[1], Category.RECEIVE, 1100, 1200);
        ordered(t[2], Category.REJECTED, 1100, 1100);
        ordered(t[4], Category.SEND, 1100, 1100);
        ordered(t[5], Category.SEND, 1100, 1100);
        ordered(t[5], Category.SEND, 1150, 1150);
        ordered(t[5], Category.RECEIVE, 1100, 1300);
        ordered(t[5], Category.RECEIVE, 1100, 1400);
        ordered(t[6], Category.SEND, 1100, 1100);
        ordered(t[6], Category.RECEIVE, 1100, 1500);
        ordered(t[7], Category.REJECTED, 1100, 1100);
        ordered(t[7], Category.RECEIVE, 1100, 1600);
        ordered(t[8], Category.RECEIVE, 1100, 1700);
        ordered(unknown, Category.SEND, 1100, 1100);
        ordered(unknown, Category.RECEIVE, 1100, 1800);
        balances(100, 100, 100, 100);

        Report report = report();
        assertEquals(8, report.generated());
        assertEquals(4, report.accepted(), "T1 T4 T5 T6");
        assertEquals(2, report.rejected(), "T2 T7");
        assertEquals(3, report.completed(), "T1 T5 T6");
        assertEquals(3, report.inFlight(), "T3 T4 T8");
        Map<String, Check> checks = checks();
        assertFalse(checks.get("complete").passed());
        assertFalse(checks.get("validated-once").passed());
        assertFalse(checks.get("completed-once").passed());
        assertFalse(checks.get("money-conserved").passed());
        assertEquals("not checked: 3 trades not finished", checks.get("money-conserved").detail());
        assertFalse(checks.get("balances-match-orders").passed());
        assertEquals("not checked: 3 trades not finished", checks.get("balances-match-orders").detail());
        assertTrue(checks.get("no-negative-balances").passed());
        assertFalse(report.passed());

        assertEquals("2 of 8 trades not validated, 1 of 4 accepted trades not completed after waiting up to "
                + "60000 ms", checks.get("complete").detail());
        assertEquals("1 serial numbers generated more than once, 1 trades validated more than once, "
                + "2 trades not validated, 1 validation records for unknown trades",
                checks.get("validated-once").detail());
        assertEquals("1 accepted trades completed more than once, 1 accepted trades not completed, "
                + "1 rejected trades completed, 1 trades completed without a validation, "
                + "1 completion records for unknown trades", checks.get("completed-once").detail());
    }

    @Test
    void inFlightCountsTradesNotValidatedYetAndAcceptedTradesNotCompleted() {
        acceptedAndCompleted();
        evidence.traded(send(3, "bank0-0001", "bank1-0001", 5));          // not validated
        Trade accepted = send(4, "bank0-0001", "bank1-0001", 5);
        evidence.traded(accepted);
        ordered(accepted, Category.SEND, 1100, 1100);                       // not completed

        Report report = report();
        assertEquals(3, report.generated());
        assertEquals(2, report.accepted());
        assertEquals(0, report.rejected());
        assertEquals(1, report.completed());
        assertEquals(2, report.inFlight());
        assertFalse(evidence.settled());
    }

    @Test
    void anAcceptedAndRejectedTradeCountsAsAcceptedOnly() {
        Trade generated = send(1, "bank0-0000", "bank1-0000", 30);
        evidence.traded(generated);
        ordered(generated, Category.REJECTED, 1100, 1100);
        ordered(generated, Category.SEND, 1150, 1150);
        ordered(generated, Category.RECEIVE, 1150, 1500);
        balances(70, 100, 130, 100);

        Report report = report();
        assertEquals(1, report.accepted());
        assertEquals(0, report.rejected());
        assertEquals(1, report.completed());
        Map<String, Check> checks = checks();
        assertFalse(checks.get("validated-once").passed());
        assertTrue(checks.get("completed-once").passed(), "its one completion belongs to an accepted trade");
    }

    @Test
    void balanceChecksWaitForASettledRun() {
        Trade generated = send(1, "bank0-0000", "bank1-0000", 30);
        evidence.traded(generated);
        ordered(generated, Category.SEND, 1100, 1100);
        balances(70, 100, 100, 100);

        Map<String, Check> checks = checks();
        assertFalse(checks.get("complete").passed());
        assertTrue(checks.get("complete").detail().contains("1 of 1 accepted trades not completed"),
                checks.get("complete").detail());
        assertFalse(checks.get("completed-once").passed());
        assertEquals("not checked: 1 trades not finished", checks.get("money-conserved").detail());
        assertEquals("not checked: 1 trades not finished", checks.get("balances-match-orders").detail());
    }

    @Test
    void latenciesComeFromTheFirstRecordsByOffset() {
        Trade generated = send(1, "bank0-0000", "bank1-0000", 30);
        evidence.traded(generated);
        evidence.ordered(order(generated, Category.RECEIVE, 1100), 9, 1900);
        evidence.ordered(order(generated, Category.RECEIVE, 1100), 4, 1600);
        evidence.ordered(order(generated, Category.SEND, 1300), 7, 1300);
        evidence.ordered(order(generated, Category.SEND, 1100), 2, 1100);

        Report report = report();
        assertEquals(100, report.validationLatencyMs().max(), "offset 2: validated at 1100, created at 1000");
        assertEquals(600, report.latencyMs().max(), "offset 4: completed at 1600, created at 1000");
        assertEquals(1.7, report.throughputTps(), "1 trade in 0.6 s: 1.666... rounded half up to 1 decimal");
    }

    @Test
    void throughputIsZeroWithoutCompletions() {
        rejected();
        assertEquals(0.0, report().throughputTps());
        assertEquals(new Report.Latency(0, 0, 0, 0), report().latencyMs());
    }

    @Test
    void aTradeValidatedTwiceFailsValidatedOnce() {
        acceptedAndCompleted();
        ordered(send(1, "bank0-0000", "bank1-0000", 30), Category.SEND, 1300, 1300);
        balances(40, 100, 130, 100);

        Map<String, Check> checks = checks();
        assertFalse(checks.get("validated-once").passed());
        assertTrue(checks.get("validated-once").detail().contains("1 trades validated more than once"));
        assertFalse(checks.get("balances-match-orders").passed(), "the balance shows the double debit");
        assertFalse(checks.get("money-conserved").passed());
    }

    @Test
    void aTradeCompletedTwiceFailsCompletedOnce() {
        acceptedAndCompleted();
        ordered(send(1, "bank0-0000", "bank1-0000", 30), Category.RECEIVE, 1100, 1600);
        balances(70, 100, 160, 100);

        Map<String, Check> checks = checks();
        assertFalse(checks.get("completed-once").passed());
        assertTrue(checks.get("completed-once").detail().contains("1 accepted trades completed more than once"));
        assertFalse(checks.get("money-conserved").passed());
        assertEquals("the balances add up to 430, but the total money is 400",
                checks.get("money-conserved").detail());
    }

    @Test
    void ordersForUnknownOrRejectedTradesFail() {
        rejected();
        ordered(send(9, "bank0-0000", "bank1-0000", 1), Category.SEND, 1, 1);
        ordered(send(2, "bank1-0001", "bank0-0001", 500), Category.RECEIVE, 1, 1);
        balances(100, 100, 100, 100);

        Map<String, Check> checks = checks();
        assertTrue(checks.get("validated-once").detail().contains("1 validation records for unknown trades"));
        assertTrue(checks.get("completed-once").detail().contains("1 rejected trades completed"),
                checks.get("completed-once").detail());
    }

    @Test
    void duplicateSerialNumbersFailValidatedOnce() {
        evidence.traded(send(1, "bank0-0000", "bank1-0000", 30));
        evidence.traded(send(1, "bank0-0001", "bank1-0001", 30));
        assertTrue(checks().get("validated-once").detail().contains("1 serial numbers generated more than once"));
    }

    @Test
    void negativeBalancesFail() {
        balances(100, 100, 100, 100);
        evidence.balance("bank0-0001", -5);
        evidence.balance("bank0-0001", 100);
        Check check = checks().get("no-negative-balances");
        assertFalse(check.passed());
        assertEquals("1 balance records below 0, e.g. bank0-0001: -5", check.detail());
    }

    @Test
    void aMissingBalanceFailsBalancesMatchOrders() {
        evidence.balance("bank0-0000", 100);
        Check check = checks().get("balances-match-orders");
        assertFalse(check.passed());
        assertTrue(check.detail().startsWith("3 of 4 balances differ from the orders"), check.detail());
    }

    @Test
    void completionsMayBeReadBeforeTheirValidation() {
        Trade generated = send(1, "bank0-0000", "bank1-0000", 30);
        evidence.traded(generated);
        ordered(generated, Category.RECEIVE, 1100, 1500);
        assertFalse(evidence.settled(), "still waiting for the validation record");
        ordered(generated, Category.SEND, 1100, 1100);
        assertTrue(evidence.settled());
    }
}
