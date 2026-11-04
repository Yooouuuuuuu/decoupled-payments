package io.github.yooouuuuuuu.dp.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.yooouuuuuuu.dp.Locales;
import org.junit.jupiter.api.Test;

class IdsTest {

    @Test
    void bankIdIsItsPartition() {
        assertEquals("bank0", Ids.bank(0));
        assertEquals("bank12", Ids.bank(12));
    }

    @Test
    void accountIdIsZeroPaddedToFourDigits() {
        assertEquals("bank0-0000", Ids.account("bank0", 0));
        assertEquals("bank2-0042", Ids.account("bank2", 42));
        assertEquals("bank1-9999", Ids.account("bank1", 9999));
        assertEquals("bank1-12345", Ids.account("bank1", 12345));
    }

    @Test
    void clientIdIsRoleSiteAndPurpose() {
        assertEquals("validator-2-credits", Ids.client("validator", 2, "credits"));
    }

    @Test
    void idsHaveAsciiDigitsWhateverTheDefaultLocale() {
        Locales.withEachNonAsciiDefault(() -> {
            assertEquals("bank2-0042", Ids.account("bank2", 42));
            assertEquals("bank12", Ids.bank(12));
            assertEquals("validator-12-bank3", Ids.client("validator", 12, Ids.bank(3)));
        });
    }
}
