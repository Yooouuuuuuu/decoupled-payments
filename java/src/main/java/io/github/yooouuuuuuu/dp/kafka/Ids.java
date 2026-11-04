package io.github.yooouuuuuuu.dp.kafka;

import java.util.Locale;

/** How banks, accounts and Kafka clients are named. */
public final class Ids {
    private Ids() {
    }

    /** Bank {@code i} owns partition {@code i} of the per-bank topics: {@code bank0}, {@code bank1}, ... */
    public static String bank(int partition) {
        return "bank" + partition;
    }

    /**
     * Account {@code j} of a bank, zero-padded to four digits: {@code bank2-0042}. Always ASCII digits:
     * the default locale may have others (Arabic-Indic, for example).
     */
    public static String account(String bankId, int index) {
        return bankId + "-" + String.format(Locale.ROOT, "%04d", index);
    }

    /** {@code <role>-<siteId>}, plus a suffix when a process has several clients: {@code validator-2-credits}. */
    public static String client(String role, int siteId, String suffix) {
        return role + "-" + siteId + "-" + suffix;
    }
}
