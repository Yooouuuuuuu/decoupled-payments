package io.github.yooouuuuuuu.dp.generator;

import java.util.List;
import java.util.random.RandomGenerator;

/** Every account is equally likely. */
record UniformPicker(List<Account> accounts) implements AccountPicker {

    @Override
    public Account pick(RandomGenerator random) {
        return accounts.get(random.nextInt(accounts.size()));
    }
}
