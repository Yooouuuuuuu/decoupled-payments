package io.github.yooouuuuuuu.dp.config;

/**
 * The value of one setting and where it came from ("default.yaml", "flag --blockSize", ...), so that
 * an error message can point at the source of a bad value.
 */
record Setting(Object value, String source) {
}
