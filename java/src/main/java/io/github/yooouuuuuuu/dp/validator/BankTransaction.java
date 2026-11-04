package io.github.yooouuuuuuu.dp.validator;

import io.github.yooouuuuuuu.dp.avro.Model;

/**
 * One Kafka transaction of one bank, from begin to commit: the work on the polled records, the balances it
 * changed and the bank's new read position become visible together, or not at all. Both threads run their
 * transactions through here, with the ledger lock held throughout (thesis §5.1).
 */
final class BankTransaction {

    /** What a transaction does with the records it was given. */
    interface Work {
        /**
         * Validates or applies the records with {@code batch} and sends their orders (and credits) through
         * the bank's writer.
         *
         * @return the bank's read position after these records: the next offset to read
         */
        long run(OwnedBank bank, BankBatch batch);
    }

    private BankTransaction() {
    }

    /**
     * Runs one transaction of the bank of {@code partition}; must be called with the ledger lock held.
     *
     * <p>Any Throwable on the way, an Error too, means the transaction failed. The balance map may then hold
     * effects that will never commit, and there is no undo: the ledger is marked dirty before anything else,
     * so that no later transaction of either thread can commit those effects; then the transaction is
     * aborted and the error passed on. The process exits, and the next owner rebuilds the bank from Kafka.
     *
     * @param topic the topic the records came from (blocks or credits), whose read position is committed
     * @return the batch, for the stats
     */
    static BankBatch run(Ledger ledger, int partition, Model model, String topic, Work work) {
        ledger.requireClean();
        OwnedBank bank = ledger.bank(partition);
        BankWriter writer = bank.writer();
        BankBatch batch = new BankBatch(bank.partition(), bank.id(), bank.accounts(), model, ledger.balances());
        try {
            writer.begin();
            long nextOffset = work.run(bank, batch);
            batch.changedBalances().forEach((account, balance) ->
                    writer.send(ValidatorRecords.balance(bank, account, balance)));
            writer.commit(ValidatorRecords.position(bank, topic, nextOffset));
        } catch (Throwable e) {
            ledger.markDirty();
            writer.abort(e);
            throw e;
        }
        return batch;
    }
}
