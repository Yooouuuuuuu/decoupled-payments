package io.github.yooouuuuuuu.dp.generator;

import java.util.List;
import java.util.random.RandomGenerator;

/**
 * Zipf's law: the account at rank k (1-based, its position in the list) is picked with weight 1/k^s, so a
 * few accounts trade far more often than the rest. The thesis uses it to model hot accounts (§6.1).
 */
final class ZipfPicker implements AccountPicker {
    private final List<Account> accounts;
    /** cumulative[i] = the sum of the weights of ranks 1 to i+1. */
    private final double[] cumulative;

    ZipfPicker(List<Account> accounts, double exponent) {
        this.accounts = List.copyOf(accounts);
        this.cumulative = new double[accounts.size()];
        double sum = 0;
        for (int rank = 1; rank <= accounts.size(); rank++) {
            sum += 1 / Math.pow(rank, exponent);
            cumulative[rank - 1] = sum;
        }
    }

    @Override
    public Account pick(RandomGenerator random) {
        double total = cumulative[cumulative.length - 1];
        return accounts.get(indexOf(random.nextDouble() * total));
    }

    /** Binary search for the first rank whose cumulative weight exceeds {@code point}, a value in [0, total). */
    int indexOf(double point) {
        int low = 0;
        int high = cumulative.length - 1;
        while (low < high) {
            int middle = (low + high) >>> 1;
            if (cumulative[middle] > point) {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        return low;
    }
}
