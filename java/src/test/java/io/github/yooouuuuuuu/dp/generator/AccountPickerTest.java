package io.github.yooouuuuuuu.dp.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.yooouuuuuuu.dp.config.Distribution;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

class AccountPickerTest {
    private static final int SAMPLES = 200_000;

    static List<Account> accounts(int banks, int perBank) {
        List<Account> accounts = new ArrayList<>();
        for (int bank = 0; bank < banks; bank++) {
            for (int index = 0; index < perBank; index++) {
                accounts.add(new Account("bank" + bank + "-" + index, "bank" + bank, bank));
            }
        }
        return accounts;
    }

    private static Map<Account, Integer> histogram(AccountPicker picker) {
        Random random = new Random(1);
        Map<Account, Integer> counts = new HashMap<>();
        for (int i = 0; i < SAMPLES; i++) {
            counts.merge(picker.pick(random), 1, Integer::sum);
        }
        return counts;
    }

    @Test
    void uniformPicksEveryAccountAboutEquallyOften() {
        List<Account> accounts = accounts(2, 10);
        Map<Account, Integer> counts = histogram(AccountPicker.of(Distribution.UNIFORM, accounts, 1.0));
        assertEquals(accounts.size(), counts.size());
        double expected = (double) SAMPLES / accounts.size();
        counts.values().forEach(count -> assertEquals(expected, count, expected * 0.05));
    }

    @Test
    void zipfWeightsRankKByOneOverKToTheS() {
        List<Account> accounts = accounts(1, 10);
        double exponent = 1.0;
        double total = 0;
        for (int rank = 1; rank <= accounts.size(); rank++) {
            total += 1 / Math.pow(rank, exponent);
        }
        Map<Account, Integer> counts = histogram(AccountPicker.of(Distribution.ZIPF, accounts, exponent));
        for (int rank = 1; rank <= accounts.size(); rank++) {
            double expected = SAMPLES / Math.pow(rank, exponent) / total;
            assertEquals(expected, counts.getOrDefault(accounts.get(rank - 1), 0), expected * 0.05, "rank " + rank);
        }
    }

    @Test
    void zipfWithExponentZeroIsUniform() {
        List<Account> accounts = accounts(1, 4);
        Map<Account, Integer> counts = histogram(AccountPicker.of(Distribution.ZIPF, accounts, 0.0));
        counts.values().forEach(count -> assertEquals(SAMPLES / 4.0, count, SAMPLES / 4.0 * 0.05));
    }

    @Test
    void zipfBinarySearchFindsTheFirstRankAboveThePoint() {
        // Cumulative weights for exponent 1: 1, 1.5, 1.833...
        ZipfPicker picker = new ZipfPicker(accounts(1, 3), 1.0);
        assertEquals(0, picker.indexOf(0.0));
        assertEquals(0, picker.indexOf(0.999));
        assertEquals(1, picker.indexOf(1.0));
        assertEquals(1, picker.indexOf(1.4999));
        assertEquals(2, picker.indexOf(1.5));
        assertEquals(2, picker.indexOf(1.8333));
    }

    @Test
    void zipfWithAHugeExponentAlwaysPicksRankOne() {
        List<Account> accounts = accounts(1, 5);
        Map<Account, Integer> counts = histogram(AccountPicker.of(Distribution.ZIPF, accounts, 2000.0));
        assertEquals(Map.of(accounts.getFirst(), SAMPLES), counts);
    }
}
