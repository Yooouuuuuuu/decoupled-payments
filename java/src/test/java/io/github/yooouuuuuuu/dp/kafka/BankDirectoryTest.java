package io.github.yooouuuuuuu.dp.kafka;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.yooouuuuuuu.dp.avro.AccountInfo;
import io.github.yooouuuuuuu.dp.avro.Model;
import io.github.yooouuuuuuu.dp.cli.Failure;
import io.github.yooouuuuuuu.dp.config.Config;
import io.github.yooouuuuuuu.dp.config.ConfigException;
import io.github.yooouuuuuuu.dp.config.ConfigLoader;
import io.github.yooouuuuuuu.dp.config.Flag;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class BankDirectoryTest {

    private static AccountInfo bank(int partition, Model model) {
        return new AccountInfo(Ids.bank(partition), partition,
                List.of(Ids.account(Ids.bank(partition), 0), Ids.account(Ids.bank(partition), 1)), 100L, model, true);
    }

    @Test
    void banksAreOrderedByPartition() {
        BankDirectory directory = BankDirectory.of(List.of(bank(2, Model.SEQUENTIAL), bank(0, Model.SEQUENTIAL),
                bank(1, Model.SEQUENTIAL)));
        assertEquals(List.of("bank0", "bank1", "bank2"),
                directory.banks().stream().map(BankDirectory.Bank::id).toList());
        assertEquals("bank1", directory.bank(1).id());
        assertEquals(6, directory.accountCount());
        assertEquals(600, directory.totalMoney());
        assertEquals(Model.SEQUENTIAL, directory.model());
    }

    @Test
    void banksMustBeNumberedFromZeroWithoutGaps() {
        List<AccountInfo> withGap = List.of(bank(0, Model.CONCURRENT), bank(2, Model.CONCURRENT));
        assertThrows(Failure.class, () -> BankDirectory.of(withGap));
    }

    @Test
    void banksMustAgreeOnTheModel() {
        List<AccountInfo> mixed = List.of(bank(0, Model.CONCURRENT), bank(1, Model.SEQUENTIAL));
        assertThrows(Failure.class, () -> BankDirectory.of(mixed));
    }

    @Test
    void processesMustRunTheModelInitSetUp() throws ConfigException {
        BankDirectory concurrent = BankDirectory.of(List.of(bank(0, Model.CONCURRENT)));
        assertDoesNotThrow(() -> concurrent.requireModelOf(config()));
        Config sequential = config(new Flag("model", "sequential"));
        Failure e = assertThrows(Failure.class, () -> concurrent.requireModelOf(sequential));
        assertTrue(e.getMessage().contains("model=concurrent") && e.getMessage().contains("model=sequential"),
                e.getMessage());
        Config withoutTransactions = config(new Flag("kafkaTransactions", "false"));
        assertThrows(Failure.class, () -> concurrent.requireModelOf(withoutTransactions));
    }

    private static Config config(Flag... flags) throws ConfigException {
        return ConfigLoader.load("validator", Optional.empty(), List.of(flags), Map.of());
    }
}
