// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * Reading a segment's write time out of its key (M7.6).
 *
 * <p>⚠️ GC JUDGES AGE FROM THIS AND NOTHING ELSE. A {@code stat} per segment
 * would cost a request per object collected — the cost that makes
 * commit-log-driven GC free is exactly the absence of one.
 */
class SegmentKeyTimestampTest {

    private static final long WHEN = 1_789_600_000_000L;

    private static String key(String prefix) {
        return new SegmentKey(prefix, WHEN, "poda", 7, 12).key();
    }

    @Test
    void theTimestampComesBackFromTheKey() {
        assertThat(SegmentKey.timestampOf(key("bucket"))).isEqualTo(WHEN);
    }

    @Test
    void aPREFIXFullOfDigitsAndSlashesDoesNotConfuseIt() {
        assertThat(SegmentKey.timestampOf(key("0000000000000000000/9999")))
                .as("the prefix is caller-supplied and the date path in front of the tail "
                        + "is digits too -- a reader that scanned from the START would "
                        + "find a prefix that looks like a timestamp")
                .isEqualTo(WHEN);
    }

    @Test
    void theEPOCHItselfRoundTrips() {
        assertThat(SegmentKey.timestampOf(new SegmentKey("bucket", 0, "poda", 1, 4).key()))
                .as("zero is fixed-width-padded to nineteen digits, and a reader that "
                        + "trimmed leading zeros would see a shorter field and refuse a "
                        + "legitimate key")
                .isZero();
    }

    @Test
    void aKeyThatIsNOTASegmentKeyIsREFUSEDRatherThanGuessedAt() {
        assertThatThrownBy(() -> SegmentKey.timestampOf("bucket/data/not-a-segment-key"))
                .as("⚠️ NEVER A GUESS: a guess of 'now' makes an unjudgeable object "
                        + "immortal, and a guess of zero deletes it on the next pass")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aTailWhoseFirstFieldIsTHEWRONGWidthIsREFUSED() {
        assertThatThrownBy(() -> SegmentKey.timestampOf("bucket/data/123-poda-0001-h4-N.bseg"))
                .as("nineteen digits exactly, because a shorter field is a different "
                        + "grammar and parsing it anyway reads some other key's number as "
                        + "a time")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aNineteenCharacterFieldThatIsNOTDigitsIsREFUSED() {
        assertThatThrownBy(() -> SegmentKey.timestampOf(
                "bucket/data/abcdefghijklmnopqrs-poda-0001-h4-N.bseg"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
