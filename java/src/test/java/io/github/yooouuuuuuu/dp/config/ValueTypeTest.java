package io.github.yooouuuuuuu.dp.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** The strict text grammar every source shares (default.yaml, config files, environment variables, flags). */
class ValueTypeTest {

    @Test
    void integersAreOptionalMinusAndDecimalDigits() {
        assertEquals(42L, ValueType.INTEGER.parse("42"));
        assertEquals(-7L, ValueType.INTEGER.parse("-7"));
        assertEquals(10L, ValueType.INTEGER.parse("010"), "leading zeros are still decimal");
        assertEquals(Long.MAX_VALUE, ValueType.INTEGER.parse("9223372036854775807"));
        assertEquals(Long.MIN_VALUE, ValueType.INTEGER.parse("-9223372036854775808"));
        assertThrows(IllegalArgumentException.class, () -> ValueType.INTEGER.parse("9223372036854775808"));
        for (String bad : new String[] {"", "+7", "1.0", "1e3", "0x10", "ten", " 1", "1 ", "1_000", "1:30", "-"}) {
            assertThrows(IllegalArgumentException.class, () -> ValueType.INTEGER.parse(bad), bad);
        }
    }

    @Test
    void floatsArePlainDecimals() {
        assertEquals(2.0, ValueType.FLOAT.parse("2"));
        assertEquals(1.0, ValueType.FLOAT.parse("1.0"));
        assertEquals(1500.0, ValueType.FLOAT.parse("1.5e3"));
        assertEquals(0.015, ValueType.FLOAT.parse("1.5E-2"));
        assertEquals(15.0, ValueType.FLOAT.parse("1.5e+1"));
        assertEquals(-1.25, ValueType.FLOAT.parse("-1.25"));
        for (String bad : new String[] {"", ".5", "5.", "+1", "NaN", "Infinity", "-Infinity", "0x1p3", "0x1p-2",
                "1_0.5", "1.0f", "1e", "1e999", "one"}) {
            assertThrows(IllegalArgumentException.class, () -> ValueType.FLOAT.parse(bad), bad);
        }
    }

    @Test
    void booleansAreTrueOrFalse() {
        assertEquals(true, ValueType.BOOLEAN.parse("true"));
        assertEquals(false, ValueType.BOOLEAN.parse("false"));
        for (String bad : new String[] {"", "TRUE", "True", "yes", "no", "on", "off", "1", "y"}) {
            assertThrows(IllegalArgumentException.class, () -> ValueType.BOOLEAN.parse(bad), bad);
        }
    }

    @Test
    void stringsAreTakenAsTheyAre() {
        assertEquals("kafka1:9092, kafka2:9092", ValueType.STRING.parse("kafka1:9092, kafka2:9092"));
    }

    @Test
    void errorSaysWhatWasExpected() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> ValueType.BOOLEAN.parse("yes"));
        assertEquals("expected true or false, got 'yes'", e.getMessage());
    }
}
