// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * The key grammar, and the CANONICALISATION that makes it an allow-list.
 *
 * <p>⚠️ THIS FILE EXISTS BECAUSE A MUTANT SURVIVED. `ChainReplayNonDeltaKeyTest`
 * drives both readers end to end and pins that a foreign key is not read — but
 * every interloper it seeds is refused one step EARLIER — two at the suffix
 * guard, one at {@code Long.parseUnsignedLong} — so replacing the final
 * {@code key.equals(keyFor(sequence))} with {@code return true} left the whole
 * suite green. Both reviewers measured that independently. The round trip is
 * the half the javadoc calls the point of the design, and nothing constrained
 * it.
 *
 * <p>⚠️ THE CASES BELOW ALL PARSE. That is the entire point: a key whose digits
 * are not hex never reaches the comparison, so it cannot pin it. Each of these
 * yields a perfectly good {@code long} and is refused only because regenerating
 * the canonical key does not reproduce it.
 *
 * <p>⚠️ PADDING IS LOAD-BEARING, not cosmetic. ADR-0022 removed
 * {@code lastModifiedMillis}, leaving key order as the only ordering a reader
 * has, and unpadded {@code 10.delta} sorts BEFORE {@code 9.delta}. A filter that
 * admitted unpadded keys would hand {@code chainEnd} the wrong chain end.
 */
class LogKeysTest {

    private static final LogKeys KEYS = new LogKeys("bins", 1);

    @Test
    void everyKeyTheWriterProducesIsAcceptedIncludingTheExtremes() {
        // ⚠️ THE DIRECTION THAT MATTERS MOST. A filter that refuses a key the
        // log actually wrote drops a real entry from recovery -- I2/I3, and
        // strictly worse than the bug this filter fixes.
        assertThat(KEYS.isEntryKey(KEYS.keyFor(0))).isTrue();
        assertThat(KEYS.isEntryKey(KEYS.keyFor(9))).isTrue();
        assertThat(KEYS.isEntryKey(KEYS.keyFor(16))).isTrue();
        assertThat(KEYS.isEntryKey(KEYS.keyFor(Long.MAX_VALUE))).isTrue();
    }

    @Test
    void aKeyWhoseDigitsParseButDoNotRegenerateIsRefused() {
        String p = KEYS.logPrefix();

        // Unpadded: parses to 9, canonicalises to 0000000000000009.
        assertThat(KEYS.isEntryKey(p + "9.delta"))
                .as("unpadded -- lexicographic order would stop being numeric order")
                .isFalse();
        // Uppercase: parses to 10, canonicalises to lowercase a.
        assertThat(KEYS.isEntryKey(p + "000000000000000A.delta"))
                .as("uppercase hex is not what the writer emits")
                .isFalse();
        // Seventeen digits: parses to 9, canonicalises to sixteen.
        assertThat(KEYS.isEntryKey(p + "00000000000000009.delta"))
                .as("over-long, though it parses and its value is in range")
                .isFalse();
        // `Long.parseUnsignedLong` accepts a leading plus sign.
        assertThat(KEYS.isEntryKey(p + "+000000000000009.delta"))
                .as("a signed form the writer can never produce")
                .isFalse();
    }

    @Test
    void aKeyTooSHORTToCarryTheGrammarIsRefusedRatherThanThrowing() {
        // ⚠️ THE TWO LENGTH GUARDS ARE LOAD-BEARING, and nothing pinned them.
        // `isEntryKey` slices BETWEEN them, so dropping either lets a key too
        // short to hold both reach `begin > end` and throw
        // StringIndexOutOfBoundsException instead of answering false.
        // ⚠️ REACHABLE TODAY. An earlier draft of this comment said it waited
        // for M4.8b2; that was wrong, and wrong in the SAFE direction --
        // `store.list(logPrefix, ...)` returns a foreign object whatever its
        // name, `<logPrefix>x` included, and both readers already ask about
        // every key it returns.
        assertThat(KEYS.isEntryKey(".delta"))
                .as("too short to start with the prefix -- pins the startsWith guard")
                .isFalse();
        assertThat(KEYS.isEntryKey(KEYS.logPrefix() + "x"))
                .as("under the prefix, too short for the suffix -- pins the endsWith guard")
                .isFalse();
        // ⚠️ PINS THE GUARD'S OPERAND, not merely its presence: narrowing
        // `startsWith(logPrefix)` to `startsWith(prefix)` -- the record
        // component rather than the full key prefix -- passes BOTH guards here
        // and then slices `substring(32, 4)`. The two cases above miss it.
        assertThat(KEYS.isEntryKey("bins.delta"))
                .as("starts with the bare prefix component, not the log prefix")
                .isFalse();
        // ⚠️ AND EVERY PROPER PREFIX AT ONCE. The log prefix with its trailing
        // `/` replaced by the suffix starts with each one of them and with the
        // whole prefix with NONE, so any narrowing of that operand -- the bare
        // component, the dropped `/`, the dropped epoch segment -- admits it,
        // and it is one character too short to slice.
        String proper = KEYS.logPrefix();
        assertThat(KEYS.isEntryKey(proper.substring(0, proper.length() - 1) + ".delta"))
                .as("starts with every PROPER prefix of the log prefix, and not the whole one")
                .isFalse();
        // ⚠️ AND THE SUFFIX GUARD'S OPERAND, the same asymmetry: narrowing
        // `endsWith(SUFFIX)` to `endsWith("delta")` -- or any suffix of it --
        // leaves every real key passing, and this one slicing backwards.
        assertThat(KEYS.isEntryKey(KEYS.logPrefix() + "delta"))
                .as("ends with a proper suffix of \".delta\", pinning that guard's operand")
                .isFalse();
    }

    @Test
    void aKeyThatIsNotThisChainsAtAllIsRefused() {
        String p = KEYS.logPrefix();

        assertThat(KEYS.isEntryKey(p + "ckpt/0000000000000009.ckpt")).isFalse();
        // ⚠️ ONE SEGMENT DEEPER, which `BinStore.list` still returns because it
        // is flat and undelimited -- and which ends in `.delta`, so an
        // extension check would admit it.
        assertThat(KEYS.isEntryKey(p + "ckpt/0000000000000009.delta")).isFalse();
        assertThat(KEYS.isEntryKey(p + "0000000000000001.delta.tmp")).isFalse();
        assertThat(KEYS.isEntryKey(p + ".delta"))
                .as("no digits at all")
                .isFalse();
        assertThat(KEYS.isEntryKey(new LogKeys("bins", 2).keyFor(0)))
                .as("a sibling epoch's entry is not this chain's")
                .isFalse();
    }
}
