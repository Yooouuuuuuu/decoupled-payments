package io.github.yooouuuuuuu.dp.kafka;

import java.util.List;

/** The topics of the system (see docs/architecture). */
public final class Topics {
    /** New trades from the trade generator, and in the Sequential model also the credits sent back. */
    public static final String TRADES = "trades";
    /** Blocks of trades, one partition per bank. */
    public static final String BLOCKS = "blocks";
    /** Credits waiting to be applied, one partition per receiving bank (Concurrent model). */
    public static final String CREDITS = "credits";
    /** Results: accepted or rejected after validation, completed after the credit. */
    public static final String ORDERS = "orders";
    /** Latest balance per account (compacted), to rebuild a bank's state. */
    public static final String LOCAL_BALANCE = "localBalance";
    /** How far each bank's validator has read blocks and credits (compacted). */
    public static final String VALIDATOR_OFFSETS = "validatorOffsets";
    /** The banks and accounts, written once by init. */
    public static final String ACCOUNT_INFO = "accountInfo";

    public static final List<String> ALL =
            List.of(TRADES, BLOCKS, CREDITS, ORDERS, LOCAL_BALANCE, VALIDATOR_OFFSETS, ACCOUNT_INFO);

    private Topics() {
    }
}
