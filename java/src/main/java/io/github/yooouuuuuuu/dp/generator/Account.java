package io.github.yooouuuuuuu.dp.generator;

/** An account and the bank (and so the partition) that owns it. */
record Account(String id, String bank, int partition) {
}
