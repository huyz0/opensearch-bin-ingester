// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Locale;
import org.junit.jupiter.api.Test;

/**
 * Every key {@link LogKeys} builds is byte for byte the one the old
 * {@code String.format} built (M13.50): the chain's keys are wire values,
 * and a reader of an older chain must find every one of them.
 *
 * <p>⚠️ WHY THE FORMATTER WENT: a profile of the whole sequencer suite put
 * about 30% of its CPU in {@code java.util.Formatter}, three calls per key
 * the invariant checkers list -- the cost that pushed the commit-protocol
 * sweeps past their timeout under load.
 */
class LogKeysFormatTest {

    private static final long[] VALUES = {0, 1, 9, 10, 15, 16, 255, 0x1234_5678_9abcL,
            Long.MAX_VALUE, -1, Long.MIN_VALUE, -0x10L};

    @Test
    void hex16IsTheSixteenDigitFormatForEveryValue() {
        for (long v : VALUES) {
            assertThat(LogKeys.hex16(v)).as("%d", v)
                    .isEqualTo(String.format(Locale.ROOT, "%016x", v));
        }
    }

    @Test
    void everyKEYIsTheOneTheFormatterBuilt() {
        for (long epoch : VALUES) {
            LogKeys keys = new LogKeys("bins/cluster-a", epoch);
            String prefix = String.format(Locale.ROOT, "%s/ctl/log/0/%016x/", "bins/cluster-a",
                    epoch);
            assertThat(keys.logPrefix()).isEqualTo(prefix);
            for (long seq : VALUES) {
                assertThat(keys.keyFor(seq)).isEqualTo(
                        String.format(Locale.ROOT, "%s%016x%s", prefix, seq, ".delta"));
                assertThat(keys.isEntryKey(keys.keyFor(seq))).isTrue();
                assertThat(keys.checkpointKeyFor(seq)).isEqualTo(
                        String.format(Locale.ROOT, "%sckpt/%016x.ckpt", prefix, seq));
            }
        }
    }
}
