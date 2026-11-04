package io.github.yooouuuuuuu.dp;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.List;
import java.util.Locale;

/** Runs test code under default locales whose digits aren't ASCII, to show our output doesn't depend on them. */
public final class Locales {
    /** Arabic-Indic and Persian digits. */
    public static final List<Locale> NON_ASCII_DIGITS = List.of(Locale.forLanguageTag("ar-EG"),
            Locale.forLanguageTag("fa-IR"));

    private Locales() {
    }

    public static void withEachNonAsciiDefault(Runnable test) {
        Locale original = Locale.getDefault();
        try {
            for (Locale locale : NON_ASCII_DIGITS) {
                Locale.setDefault(locale);
                // Otherwise the test would prove nothing: this locale really formats 42 differently.
                assertNotEquals("42", String.format("%d", 42), locale.toLanguageTag());
                test.run();
            }
        } finally {
            Locale.setDefault(original);
        }
    }
}
