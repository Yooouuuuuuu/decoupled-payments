package io.github.yooouuuuuuu.dp.kafka;

import java.util.List;

/** The consumer groups of the system. */
public final class Groups {
    public static final String AGGREGATORS = "aggregators";
    public static final String VALIDATORS = "validators";

    public static final List<String> ALL = List.of(AGGREGATORS, VALIDATORS);

    private Groups() {
    }
}
