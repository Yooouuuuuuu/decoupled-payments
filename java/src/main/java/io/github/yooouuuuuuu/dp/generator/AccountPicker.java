package io.github.yooouuuuuuu.dp.generator;

import io.github.yooouuuuuuu.dp.config.Distribution;
import java.util.List;
import java.util.random.RandomGenerator;

/** Picks the accounts of new trades from a fixed list of all accounts. */
sealed interface AccountPicker permits UniformPicker, ZipfPicker {

    Account pick(RandomGenerator random);

    static AccountPicker of(Distribution distribution, List<Account> accounts, double zipfExponent) {
        return switch (distribution) {
            case UNIFORM -> new UniformPicker(accounts);
            case ZIPF -> new ZipfPicker(accounts, zipfExponent);
        };
    }
}
