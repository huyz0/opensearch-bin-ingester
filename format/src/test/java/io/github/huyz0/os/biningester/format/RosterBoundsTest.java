// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * The roster's remaining refusals (M13.26b review round 1, T5): a negative
 * {@code notBefore}, a decision's negative number or resume offset, and an
 * unpaired surrogate, which ADR-0082 §3 says the encoder refuses.
 */
class RosterBoundsTest {

    private static final Roster.Incarnation L =
            new Roster.Incarnation("ingester-1", "uid-1", "az-a", "");

    @Test
    void aNEGATIVENotBeforeIsRefused() {
        assertThatThrownBy(() -> new Roster(7, -1, L,
                List.of(new Roster.Member(L, Roster.State.ROSTERED)),
                List.of(new Roster.TermRecord(0, new TreeMap<>())), List.of(), -1, 0, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aDECISIONsNegativeNumberOrOffsetIsRefused() {
        RunKey stream = new RunKey(RosterTest.I1, 0);
        assertThatThrownBy(() -> new Roster.Decision(-1, stream, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Roster.Decision(0, stream, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anUNPAIREDSurrogateIsRefused() {
        assertThatThrownBy(() -> new Roster.Incarnation("p\uD800", "uid", "az", ""))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
